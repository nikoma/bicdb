//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_raw_analyze(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut targets = Vec::with_capacity(statements.len());
        for statement in &statements {
            let Some(target) = raw_analyze_target(statement)? else {
                return Ok(None);
            };
            targets.push(target);
        }
        for target in targets {
            match target {
                Some(table) => {
                    self.require_table_ownership(&table, "analyze it")?;
                    self.analyze_collection_with_type_statistics(&table)?
                }
                None => self.analyze_all_permitted_with_type_statistics()?,
            }
        }
        Ok(Some(SqlResult::command("ANALYZE")))
    }

    pub(crate) fn execute_raw_create_sequence(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut sequences = Vec::new();
        for statement in &statements {
            let Some((sequence, if_not_exists)) = raw_create_sequence(statement)? else {
                return Ok(None);
            };
            sequences.push((sequence, if_not_exists));
        }
        for (sequence, if_not_exists) in sequences {
            self.create_session_sequence_if_missing(sequence, if_not_exists)?;
        }
        Ok(Some(SqlResult::command("CREATE SEQUENCE")))
    }

    pub(crate) fn execute_raw_alter_sequence(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut changes = Vec::new();
        for statement in &statements {
            let Some(change) = raw_alter_sequence(statement)? else {
                return Ok(None);
            };
            changes.push(change);
        }
        for change in changes {
            match change {
                RawAlterSequence::OwnerTo {
                    sequence: name,
                    owner,
                } => {
                    ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                    let mut sequence = load_sequence_required(self.db_ref(), &name)?;
                    let object = format!("sequence {name}");
                    self.require_object_ownership(&sequence.owner, &object, "change its owner")?;
                    self.require_settable_new_owner(&owner, &object)?;
                    sequence.owner = owner;
                    self.save_session_sequence(&sequence)?;
                }
                RawAlterSequence::OwnedBy {
                    sequence: name,
                    owner,
                } => {
                    let mut sequence = load_sequence_required(self.db_ref(), &name)?;
                    let identity_owned = match (
                        sequence.owned_by_table.as_deref(),
                        sequence.owned_by_column.as_deref(),
                    ) {
                        (Some(table), Some(column)) => load_schema(self.db_ref(), table)?
                            .as_ref()
                            .and_then(|schema| schema.column(column))
                            .is_some_and(|column| column.identity.is_some()),
                        _ => false,
                    };
                    if identity_owned {
                        return Err(SqlError::Unsupported(
                            "cannot change ownership of identity sequence".to_string(),
                        ));
                    }
                    match owner {
                        Some((table, column)) => {
                            let schema = load_schema(self.db_ref(), &table)?
                                .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
                            ensure_schema_column(&table, &schema, &column)?;
                            sequence.owned_by_table = Some(table);
                            sequence.owned_by_column = Some(column);
                        }
                        None => {
                            sequence.owned_by_table = None;
                            sequence.owned_by_column = None;
                        }
                    }
                    self.save_session_sequence(&sequence)?;
                }
                RawAlterSequence::Restart {
                    sequence: name,
                    value,
                } => {
                    let mut sequence = load_sequence_required(self.db_ref(), &name)?;
                    let value = value.unwrap_or(sequence.start_value);
                    validate_sequence_value(&sequence, value, "RESTART")?;
                    sequence.last_value = value;
                    sequence.is_called = false;
                    self.save_session_sequence(&sequence)?;
                }
            }
        }
        Ok(Some(SqlResult::command("ALTER SEQUENCE")))
    }

    /// `ALTER TABLE ... ALTER COLUMN ... ADD GENERATED ... AS IDENTITY
    /// (SEQUENCE NAME ...)` — pg_dump's standalone identity attach. Binds
    /// the column to the (pg_dump-created) named sequence so later
    /// `setval`s line up; creates a default backing sequence only if the
    /// named one is absent.
    pub(crate) fn execute_raw_alter_add_identity(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut changes = Vec::new();
        for statement in &statements {
            let Some(change) = raw_alter_add_identity(statement)? else {
                return Ok(None);
            };
            changes.push(change);
        }
        for change in changes {
            let table = resolve_session_relation_name(self.db_ref(), &change.table)?;
            self.require_table_ownership(&table, "ALTER TABLE")?;
            let mut schema = load_schema(self.db_ref(), &table)?
                .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
            let idx = schema
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(&change.column))
                .ok_or_else(|| SqlError::UndefinedColumn {
                    table: table.clone(),
                    column: change.column.clone(),
                })?;
            if !matches!(
                schema.columns[idx].pg_type.as_str(),
                "int2" | "int4" | "int8"
            ) {
                return Err(SqlError::invalid_parameter_value(
                    "identity column type must be smallint, integer, or bigint",
                ));
            }
            if schema.columns[idx].identity.is_some() {
                return Err(SqlError::object_not_in_prerequisite_state(format!(
                    "column \"{}\" of relation \"{}\" is already an identity column",
                    change.column, table
                )));
            }
            let sequence_name = match &change.sequence_name {
                Some(name) => resolve_session_relation_name_if_exists(self.db_ref(), name),
                None => sequence_name_for_column(&table, &schema.columns[idx].name),
            };
            if load_sequence(self.db_ref(), &sequence_name)?.is_none() {
                let mut sequence = sequence_from_options(
                    sequence_name.clone(),
                    &schema.columns[idx].pg_type,
                    &[],
                )?;
                sequence.owned_by_table = Some(table.clone());
                sequence.owned_by_column = Some(schema.columns[idx].name.clone());
                self.create_session_sequence_if_missing(sequence, false)?;
            }
            schema.columns[idx].identity = Some(change.kind.clone());
            schema.columns[idx].default_sequence = Some(sequence_name);
            schema.columns[idx].nullable = false;
            self.save_session_schema(&schema)?;
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    pub(crate) fn execute_raw_alter_identity_restart(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut changes = Vec::new();
        for statement in &statements {
            let Some(change) = raw_alter_identity_restart(statement)? else {
                return Ok(None);
            };
            changes.push(change);
        }
        for change in changes {
            self.require_table_ownership(&change.table, "ALTER TABLE")?;
            let schema = load_schema(self.db_ref(), &change.table)?
                .ok_or_else(|| SqlError::InvalidCollection(change.table.clone()))?;
            let column =
                schema
                    .column(&change.column)
                    .ok_or_else(|| SqlError::UndefinedColumn {
                        table: change.table.clone(),
                        column: change.column.clone(),
                    })?;
            if column.identity.is_none() {
                return Err(SqlError::object_not_in_prerequisite_state(format!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    change.column, change.table
                )));
            }
            let sequence_name = column.default_sequence.as_ref().ok_or_else(|| {
                SqlError::object_not_in_prerequisite_state(format!(
                    "identity column \"{}\" has no sequence",
                    change.column
                ))
            })?;
            let mut sequence = load_sequence_required(self.db_ref(), sequence_name)?;
            let value = change.value.unwrap_or(sequence.start_value);
            validate_sequence_value(&sequence, value, "RESTART")?;
            sequence.last_value = value;
            sequence.is_called = false;
            self.save_session_sequence(&sequence)?;
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    pub(crate) fn validate_column_compression(column: &ColumnSchema) -> Result<()> {
        if column.type_storage() == 'p' {
            return Err(SqlError::Unsupported(format!(
                "column data type {} does not support compression",
                column.pg_type
            )));
        }
        Ok(())
    }

    pub(crate) fn execute_raw_create_table_compression(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(rewrite) = raw_create_table_compression(sql)? else {
            return Ok(None);
        };
        let statements = parse_statements(&rewrite.rewritten_sql)?;
        let [Statement::CreateTable(create_table)] = statements.as_slice() else {
            return Err(SqlError::InvalidSql(
                "CREATE TABLE compression rewrite did not produce one CREATE TABLE statement"
                    .to_string(),
            ));
        };
        self.execute_create_table_with_compression(create_table, &rewrite.columns)
            .map(Some)
    }

    pub(crate) fn execute_raw_alter_column_compression(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(change) = raw_alter_column_compression(sql)? else {
            return Ok(None);
        };
        let Some(mut schema) = load_schema(self.db_ref(), &change.table)? else {
            if change.if_exists {
                return Ok(Some(SqlResult::command("ALTER TABLE")));
            }
            return Err(SqlError::InvalidCollection(change.table));
        };
        self.require_table_ownership(&change.table, "ALTER TABLE")?;
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.name == change.column)
            .ok_or_else(|| SqlError::UndefinedColumn {
                table: change.table.clone(),
                column: change.column.clone(),
            })?;
        Self::validate_column_compression(column)?;
        column.compression = change.compression;
        self.save_session_schema(&schema)?;
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    pub(crate) fn execute_raw_alter_table_set_schema(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut changes = Vec::with_capacity(statements.len());
        for statement in &statements {
            let Some(change) = raw_alter_table_set_schema(statement)? else {
                return Ok(None);
            };
            changes.push(change);
        }
        for change in changes {
            self.apply_raw_alter_table_set_schema(change)?;
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    pub(crate) fn apply_raw_alter_table_set_schema(
        &mut self,
        change: RawAlterTableSetSchema,
    ) -> Result<()> {
        let Some(mut schema) = load_schema(self.db_ref(), &change.table)? else {
            if change.if_exists {
                return Ok(());
            }
            return Err(SqlError::InvalidCollection(change.table));
        };
        // Moving a relation is a change to it, and PostgreSQL requires
        // ownership. Ungated, any role could relocate another tenant's table.
        self.require_table_ownership(&change.table, "set its schema")?;
        if !is_known_schema(&change.schema_name)
            && load_namespace(self.db_ref(), &change.schema_name)?.is_none()
        {
            return Err(SqlError::InvalidSql(format!(
                "schema \"{}\" does not exist",
                change.schema_name
            )));
        }
        if schema.schema_name.eq_ignore_ascii_case(&change.schema_name) {
            return Ok(());
        }
        schema.schema_name = change.schema_name;
        self.save_session_schema(&schema)?;
        if schema.partitioning.is_some() {
            sync_partition_children_from_parent(self.db_mut()?, &schema)?;
        }
        Ok(())
    }

    pub(crate) fn execute_raw_create_partition_table(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut partitions = Vec::with_capacity(statements.len());
        for statement in &statements {
            let Some(partition) = raw_create_partition_table(statement)? else {
                return Ok(None);
            };
            partitions.push(partition);
        }
        for partition in partitions {
            self.apply_raw_create_partition_table(partition)?;
        }
        Ok(Some(SqlResult::command("CREATE TABLE")))
    }

    pub(crate) fn execute_raw_create_table_like(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut definitions = Vec::with_capacity(statements.len());
        for statement in &statements {
            let Some(definition) = raw_create_table_like(statement)? else {
                return Ok(None);
            };
            definitions.push(definition);
        }
        for definition in definitions {
            self.apply_raw_create_table_like(definition)?;
        }
        Ok(Some(SqlResult::command("CREATE TABLE")))
    }

    pub(crate) fn apply_raw_create_table_like(
        &mut self,
        definition: RawCreateTableLike,
    ) -> Result<()> {
        if !matches!(
            definition.schema_name.as_str(),
            "public" | "pg_catalog" | "information_schema"
        ) {
            self.create_session_namespace_if_missing(
                NamespaceSchema {
                    name: definition.schema_name.clone(),
                    owner: current_user_from_gucs(&self.session_gucs),
                },
                true,
            )?;
        }

        let relation_exists = load_schema(self.db_ref(), &definition.table)?.is_some()
            || load_sequence(self.db_ref(), &definition.table)?.is_some()
            || load_view(self.db_ref(), &definition.table)?.is_some()
            || user_collection_names(self.db_ref())
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&definition.table));
        if relation_exists {
            if definition.if_not_exists {
                return Ok(());
            }
            return Err(SqlError::InvalidSql(format!(
                "relation \"{}\" already exists",
                definition.table
            )));
        }

        let source = load_schema(self.db_ref(), &definition.source_table)?
            .ok_or_else(|| SqlError::InvalidCollection(definition.source_table.clone()))?;
        let mut columns = source
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .cloned()
            .collect::<Vec<_>>();
        if !definition.options.include_constraints {
            for column in &mut columns {
                column.primary_key = false;
            }
        }
        if !definition.options.include_defaults {
            for column in &mut columns {
                column.default_sequence = None;
                column.default_value = None;
                column.default_expr = None;
                column.identity = None;
            }
        }

        let primary_key_name = definition
            .options
            .include_constraints
            .then(|| source.primary_key_name.clone())
            .flatten();
        let constraints = if definition.options.include_constraints {
            source.constraints.clone()
        } else {
            Vec::new()
        };
        // LIKE ... INCLUDING CONSTRAINTS clones the source's foreign keys onto
        // the new table, which attaches it to whatever those keys point at.
        // That is FK creation by another route, and it bypassed the REFERENCES
        // gate the declared paths enforce — the same "gate on path A, same
        // effect via path B" shape the gate exists to close.
        self.require_reference_privilege(&definition.table, &constraints)?;
        let indexes = if definition.options.include_indexes {
            source
                .indexes
                .iter()
                .map(|index| {
                    let mut index = index.clone();
                    index.name = copied_like_index_name(
                        &index.name,
                        &definition.source_table,
                        &definition.table,
                    );
                    index.metadata_only = true;
                    index
                })
                .collect()
        } else {
            Vec::new()
        };

        if !columns.iter().any(|column| column.primary_key) {
            columns.push(ColumnSchema {
                name: SYNTHETIC_PRIMARY_KEY.to_string(),
                pg_type: "text".to_string(),
                user_type: None,
                collation: None,
                type_modifier: None,
                array_ndims: 0,
                compression: None,
                primary_key: true,
                hidden: true,
                nullable: false,
                vector_dim: None,
                default_sequence: None,
                default_value: None,
                default_expr: None,
                generated_expr: None,
                identity: None,
            });
        }

        self.create_session_collection(&definition.table)?;
        let row_type_oid =
            table_row_type_oid(namespace_oid(&definition.schema_name), &definition.table);
        let row_array_type_oid =
            table_row_array_type_oid(namespace_oid(&definition.schema_name), &definition.table);
        self.save_session_schema(&TableSchema {
            name: definition.table,
            schema_name: definition.schema_name,
            row_type_oid: Some(row_type_oid),
            row_array_type_oid: Some(row_array_type_oid),
            columns,
            primary_key_name,
            indexes,
            constraints,
            rls_enabled: source.rls_enabled,
            rls_forced: source.rls_forced,
            policies: source.policies,
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            partitioning: definition.partitioning,
            partition_of: None,
        })?;
        Ok(())
    }

    pub(crate) fn apply_raw_create_partition_table(
        &mut self,
        partition: RawCreatePartitionTable,
    ) -> Result<()> {
        if !matches!(
            partition.partition_schema.as_str(),
            "public" | "pg_catalog" | "information_schema"
        ) {
            self.create_session_namespace_if_missing(
                NamespaceSchema {
                    name: partition.partition_schema.clone(),
                    owner: current_user_from_gucs(&self.session_gucs),
                },
                true,
            )?;
        }

        let relation_exists = load_schema(self.db_ref(), &partition.partition_table)?.is_some()
            || load_sequence(self.db_ref(), &partition.partition_table)?.is_some()
            || load_view(self.db_ref(), &partition.partition_table)?.is_some()
            || user_collection_names(self.db_ref())
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&partition.partition_table));
        if relation_exists {
            if partition.if_not_exists {
                return Ok(());
            }
            return Err(SqlError::InvalidSql(format!(
                "relation \"{}\" already exists",
                catalog_display_name(&partition.partition_table)
            )));
        }

        // Creating a partition OF someone else's partitioned table extends
        // that table, so it needs the parent's authority.
        self.require_table_ownership(&partition.parent_table, "create a partition of it")?;
        let parent = load_schema(self.db_ref(), &partition.parent_table)?
            .ok_or_else(|| SqlError::InvalidCollection(partition.parent_table.clone()))?;
        if parent.partitioning.is_none() {
            return Err(SqlError::InvalidSql(format!(
                "relation \"{}\" is not a partitioned table",
                partition.parent_table
            )));
        }

        self.create_session_collection(&partition.partition_table)?;
        let row_type_oid = table_row_type_oid(
            namespace_oid(&partition.partition_schema),
            &partition.partition_table,
        );
        let row_array_type_oid = table_row_array_type_oid(
            namespace_oid(&partition.partition_schema),
            &partition.partition_table,
        );
        self.save_session_schema(&TableSchema {
            name: partition.partition_table,
            schema_name: partition.partition_schema,
            row_type_oid: Some(row_type_oid),
            row_array_type_oid: Some(row_array_type_oid),
            columns: parent.columns,
            primary_key_name: parent.primary_key_name,
            indexes: Vec::new(),
            constraints: parent.constraints,
            rls_enabled: parent.rls_enabled,
            rls_forced: parent.rls_forced,
            policies: parent.policies,
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            partitioning: None,
            partition_of: Some(PartitionOfSchema {
                parent_table: partition.parent_table,
                parent_schema: if parent.schema_name.is_empty() {
                    partition.parent_schema
                } else {
                    parent.schema_name
                },
                bound: partition.bound,
            }),
        })?;
        Ok(())
    }

    pub(crate) fn execute_raw_partition_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut tag = None;
        let mut table_actions = Vec::new();
        for statement in &statements {
            if let Some(action) = raw_alter_table_partition_action(statement)? {
                tag = Some("ALTER TABLE");
                table_actions.push(action);
                continue;
            }
            let Some(command_tag) = raw_partition_ddl_command_tag(statement)? else {
                return Ok(None);
            };
            tag = Some(command_tag);
        }
        for action in table_actions {
            self.apply_raw_alter_table_partition(action)?;
        }
        Ok(Some(SqlResult::command(tag.unwrap_or("ALTER TABLE"))))
    }

    pub(crate) fn apply_raw_alter_table_partition(
        &mut self,
        action: RawAlterTablePartition,
    ) -> Result<()> {
        match action {
            RawAlterTablePartition::Attach {
                parent_table,
                parent_schema,
                partition_table,
                partition_schema,
                bound,
            } => {
                // Both relations change: the parent gains a partition and the
                // attached table becomes part of it. Ownership of both is
                // required, as in PostgreSQL.
                self.require_table_ownership(&parent_table, "attach a partition to it")?;
                self.require_table_ownership(&partition_table, "attach it as a partition")?;
                let parent = load_schema(self.db_ref(), &parent_table)?
                    .ok_or_else(|| SqlError::InvalidCollection(parent_table.clone()))?;
                if parent.partitioning.is_none() {
                    return Err(SqlError::InvalidSql(format!(
                        "relation \"{}\" is not a partitioned table",
                        parent_table
                    )));
                }
                let mut partition = load_schema(self.db_ref(), &partition_table)?
                    .ok_or_else(|| SqlError::InvalidCollection(partition_table.clone()))?;
                partition.schema_name = partition_schema;
                partition.partition_of = Some(PartitionOfSchema {
                    parent_table,
                    parent_schema: if parent.schema_name.is_empty() {
                        parent_schema
                    } else {
                        parent.schema_name
                    },
                    bound,
                });
                self.save_session_schema(&partition)?;
            }
            RawAlterTablePartition::Detach { partition_table } => {
                self.require_table_ownership(&partition_table, "detach it")?;
                let mut partition = load_schema(self.db_ref(), &partition_table)?
                    .ok_or_else(|| SqlError::InvalidCollection(partition_table.clone()))?;
                partition.partition_of = None;
                self.save_session_schema(&partition)?;
            }
        }
        Ok(())
    }

    pub(crate) fn execute_raw_exclusion_constraint_ddl(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut parsed = Vec::new();
        for statement in &statements {
            let Some((table, constraint)) = raw_alter_table_exclusion_constraint(statement)? else {
                return Ok(None);
            };
            parsed.push((table, constraint));
        }
        for (table, constraint) in parsed {
            self.require_table_ownership(&table, "ALTER TABLE")?;
            let mut schema = load_schema(self.db_ref(), &table)?
                .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
            validate_constraint_columns(self.db_ref(), &table, &schema, &constraint)?;
            let name = constraint_name(&constraint);
            if schema
                .constraints
                .iter()
                .any(|candidate| constraint_name(candidate) == name)
            {
                return Err(SqlError::duplicate_constraint(&table, name));
            }
            validate_existing_constraint(self.db_ref(), &table, &schema, &constraint)?;
            schema.constraints.push(constraint);
            self.save_session_schema(&schema)?;
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    pub(crate) fn apply_search_path_setting(&mut self, values: &[Expr]) -> Result<()> {
        if values.is_empty() {
            return Err(SqlError::InvalidSql(
                "SET search_path expects at least one schema".to_string(),
            ));
        }
        let path = values
            .iter()
            .map(|value| eval_setting_value(value).map(|value| value.to_cell()))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        Arc::make_mut(&mut self.session_gucs).insert("search_path".to_string(), path);
        Ok(())
    }

    pub(crate) fn apply_setting(&mut self, name: &ObjectName, value: &Expr) -> Result<()> {
        let setting = object_name(name)?.to_ascii_lowercase();
        let value = eval_setting_value(value)?;
        self.apply_setting_value(setting, value)
    }

    pub(crate) fn apply_setting_value(&mut self, setting: String, value: SqlValue) -> Result<()> {
        if setting == POSTGRES_VERSION_BANNER_GUC {
            return Err(protected_security_setting_error(&setting));
        }
        if is_protected_security_setting(&setting)
            && !self.postgres_compatibility_security_gucs_enabled()
        {
            return Err(protected_security_setting_error(&setting));
        }
        match setting.as_str() {
            "bicdb.vector_search" => {
                let SqlValue::String(mode) = value else {
                    return Err(SqlError::InvalidSql(
                        "bicdb.vector_search expects 'exact' or 'ann'".to_string(),
                    ));
                };
                self.settings.vector_search = match mode.to_ascii_lowercase().as_str() {
                    "exact" => VectorSearchMode::Exact,
                    "ann" => VectorSearchMode::Ann,
                    other => {
                        return Err(SqlError::InvalidSql(format!(
                            "unsupported bicdb.vector_search value '{other}'; expected exact or ann"
                        )));
                    }
                };
                Ok(())
            }
            "bicdb.ef_search" => {
                let value = match value {
                    SqlValue::Int(value) => value,
                    SqlValue::String(value) => value.parse::<i64>().map_err(|_| {
                        SqlError::InvalidSql(
                            "bicdb.ef_search expects a positive integer".to_string(),
                        )
                    })?,
                    _ => {
                        return Err(SqlError::InvalidSql(
                            "bicdb.ef_search expects a positive integer".to_string(),
                        ));
                    }
                };
                if value <= 0 {
                    return Err(SqlError::InvalidSql(
                        "bicdb.ef_search expects a positive integer".to_string(),
                    ));
                }
                self.settings.ef_search = value as usize;
                Ok(())
            }
            "application_name" => {
                let name = value.to_cell();
                if name.as_bytes().contains(&0) {
                    return Err(SqlError::InvalidSql(
                        "SET application_name does not allow NUL bytes".to_string(),
                    ));
                }
                Arc::make_mut(&mut self.session_gucs).insert(setting, name);
                Ok(())
            }
            // Opt-in commit-time delta repair for increment-shaped updates
            // (see core RepairPlan); also enabled globally by the
            // BICDB_UPDATE_REPAIR env var.
            "bicdb.update_repair" => {
                Arc::make_mut(&mut self.session_gucs).insert(setting, value.to_cell());
                Ok(())
            }
            "client_encoding" => {
                let encoding = value.to_cell();
                if encoding.eq_ignore_ascii_case("utf8")
                    || encoding.eq_ignore_ascii_case("utf-8")
                    || encoding.eq_ignore_ascii_case("unicode")
                {
                    Arc::make_mut(&mut self.session_gucs)
                        .insert("client_encoding".to_string(), "UTF8".to_string());
                    Ok(())
                } else {
                    Err(SqlError::Unsupported(format!(
                        "SET client_encoding supports only UTF8, got {encoding}"
                    )))
                }
            }
            "client_min_messages" => {
                let level = value.to_cell().to_ascii_lowercase();
                match level.as_str() {
                    "debug" | "debug1" | "debug2" | "debug3" | "debug4" | "debug5" | "log"
                    | "info" | "notice" | "warning" | "error" | "fatal" | "panic" => {
                        Arc::make_mut(&mut self.session_gucs)
                            .insert("client_min_messages".to_string(), level);
                        Ok(())
                    }
                    other => Err(SqlError::Unsupported(format!(
                        "SET client_min_messages got unsupported value {other}"
                    ))),
                }
            }
            "datestyle" => {
                let style = normalize_datestyle_setting(&value.to_cell())?;
                Arc::make_mut(&mut self.session_gucs).insert("datestyle".to_string(), style);
                Ok(())
            }
            "default_table_access_method" => {
                let method = value.to_cell();
                if method.trim().is_empty() {
                    return Err(SqlError::InvalidSql(
                        "SET default_table_access_method expects a value".to_string(),
                    ));
                }
                Arc::make_mut(&mut self.session_gucs).insert(setting, method);
                Ok(())
            }
            "default_tablespace" => {
                Arc::make_mut(&mut self.session_gucs).insert(setting, value.to_cell());
                Ok(())
            }
            "restrict_nonsystem_relation_kind" => {
                Arc::make_mut(&mut self.session_gucs).insert(setting, value.to_cell());
                Ok(())
            }
            "search_path" => {
                let path = value.to_cell();
                if path.trim().is_empty() {
                    return Err(SqlError::InvalidSql(
                        "SET search_path expects at least one schema".to_string(),
                    ));
                }
                Arc::make_mut(&mut self.session_gucs).insert(setting, path);
                Ok(())
            }
            "extra_float_digits" => {
                let digits = value.to_cell().parse::<i64>().map_err(|_| {
                    SqlError::InvalidSql("SET extra_float_digits expects an integer".to_string())
                })?;
                if !(-15..=3).contains(&digits) {
                    return Err(SqlError::InvalidSql(
                        "SET extra_float_digits expects a value between -15 and 3".to_string(),
                    ));
                }
                Arc::make_mut(&mut self.session_gucs)
                    .insert("extra_float_digits".to_string(), digits.to_string());
                Ok(())
            }
            "bytea_output" => {
                let output = value.to_cell().to_ascii_lowercase();
                if !matches!(output.as_str(), "hex" | "escape") {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "invalid value for parameter bytea_output: {output}"
                    )));
                }
                Arc::make_mut(&mut self.session_gucs).insert("bytea_output".to_string(), output);
                Ok(())
            }
            "standard_conforming_strings" => {
                let enabled = value.to_cell().to_ascii_lowercase();
                if enabled == "on" || enabled == "true" {
                    Arc::make_mut(&mut self.session_gucs)
                        .insert("standard_conforming_strings".to_string(), "on".to_string());
                    Ok(())
                } else {
                    Err(SqlError::Unsupported(format!(
                        "SET standard_conforming_strings supports only on, got {enabled}"
                    )))
                }
            }
            "intervalstyle" => {
                let style = value.to_cell().to_ascii_lowercase();
                match style.as_str() {
                    "postgres" | "postgres_verbose" | "sql_standard" | "iso_8601" => {
                        Arc::make_mut(&mut self.session_gucs)
                            .insert("intervalstyle".to_string(), style);
                        Ok(())
                    }
                    other => Err(SqlError::Unsupported(format!(
                        "SET intervalstyle got unsupported value {other}"
                    ))),
                }
            }
            "check_function_bodies" => {
                let enabled = normalize_bool_setting(&setting, &value.to_cell())?;
                Arc::make_mut(&mut self.session_gucs).insert(setting, enabled);
                Ok(())
            }
            "xmloption" => {
                let option = value.to_cell().to_ascii_lowercase();
                if matches!(option.as_str(), "content" | "document") {
                    Arc::make_mut(&mut self.session_gucs).insert(setting, option);
                    Ok(())
                } else {
                    Err(SqlError::invalid_parameter_value(format!(
                        "invalid value for parameter xmloption: {option}"
                    )))
                }
            }
            "statement_timeout"
            | "lock_timeout"
            | "idle_in_transaction_session_timeout"
            | "idle_session_timeout"
            | "transaction_timeout" => {
                let timeout = value.to_cell();
                if timeout.trim().is_empty() {
                    return Err(SqlError::InvalidSql(format!(
                        "SET {setting} expects a timeout value"
                    )));
                }
                Arc::make_mut(&mut self.session_gucs).insert(setting, timeout);
                Ok(())
            }
            "jit" | "row_security" | "synchronize_seqscans" => {
                let enabled = normalize_bool_setting(&setting, &value.to_cell())?;
                Arc::make_mut(&mut self.session_gucs).insert(setting, enabled);
                Ok(())
            }
            "timezone" => self.apply_timezone_value(value),
            "role" => self.apply_set_role(&value.to_cell()),
            "session_authorization" => self.apply_set_session_authorization(Some(&value.to_cell())),
            // Reserved identity plumbing; forging either would allow RLS or
            // SET SESSION AUTHORIZATION privilege escalation.
            INITIAL_SESSION_AUTHORIZATION_GUC | RLS_CHECK_AS_GUC => Err(SqlError::BicDb(
                BicDbError::Authorization(format!("parameter \"{setting}\" cannot be changed")),
            )),
            // Custom (dotted) GUCs are settable like in PostgreSQL; they are
            // the primary identity vehicle for GUC-driven RLS policies.
            other if other.contains('.') => {
                Arc::make_mut(&mut self.session_gucs).insert(setting, value.to_cell());
                Ok(())
            }
            other => Err(SqlError::Unsupported(format!(
                "SET {other} is not supported"
            ))),
        }
    }

    pub(crate) fn apply_timezone_setting(&mut self, value: &Expr) -> Result<()> {
        let value = eval_setting_value(value)?;
        self.apply_timezone_value(value)
    }

    pub(crate) fn apply_timezone_value(&mut self, value: SqlValue) -> Result<()> {
        let timezone = value.to_cell();
        if !validate_timezone_name(&timezone) {
            return Err(SqlError::invalid_parameter_value(format!(
                "invalid value for parameter \"TimeZone\": \"{timezone}\""
            )));
        }
        Arc::make_mut(&mut self.session_gucs).insert("timezone".to_string(), timezone);
        Ok(())
    }

    pub(crate) fn execute_statement(&mut self, statement: &Statement) -> Result<SqlResult> {
        if matches!(
            statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        ) && (self.tx.is_some() || self.mutation_has_row_triggers(statement)?)
        {
            return self
                .with_statement_transaction(|session| session.execute_statement_inner(statement));
        }
        self.execute_statement_inner(statement)
    }

    fn execute_statement_inner(&mut self, statement: &Statement) -> Result<SqlResult> {
        match statement {
            Statement::Query(query) => self.execute_query(query),
            Statement::ShowVariable { variable } => self.sql_engine().execute_show(variable),
            Statement::CreateTable(create_table) => self.execute_create_table(create_table),
            Statement::CreateView(create_view) => self.execute_create_view(create_view),
            Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
            } => self.execute_create_sequence(
                *temporary,
                *if_not_exists,
                name,
                data_type.as_ref(),
                sequence_options,
                owned_by.as_ref(),
            ),
            Statement::CreateDomain(create_domain) => self.execute_create_domain(create_domain),
            Statement::CreateType {
                name,
                representation,
            } => self.execute_create_type(name, representation.as_ref()),
            Statement::AlterType(alter_type) => self.execute_alter_type(alter_type),
            Statement::Lock(lock) => self.execute_lock(lock),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => self.execute_create_schema(schema_name, *if_not_exists),
            Statement::CreateExtension(create_extension) => {
                self.execute_create_extension(create_extension)
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => self.execute_drop(*object_type, *if_exists, names, *cascade),
            Statement::AlterTable(alter_table) => self.execute_alter_table(alter_table),
            Statement::CreateIndex(create_index) => self.execute_create_index(create_index),
            Statement::CreateFunction(create_function) => {
                self.execute_create_function(create_function)
            }
            Statement::AlterFunction(alter_function) => self.execute_alter_function(alter_function),
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                language,
                body,
            } => self.execute_create_procedure(
                *or_alter,
                name,
                params.as_ref(),
                language.as_ref(),
                body,
            ),
            Statement::CreateTrigger(create_trigger) => self.execute_create_trigger(create_trigger),
            Statement::Comment { .. } => Ok(SqlResult::command("COMMENT")),
            Statement::CreateRole(create_role) => self.execute_create_role(create_role),
            Statement::AlterRole { name, operation } => self.execute_alter_role(name, operation),
            Statement::DropFunction(drop_function) => self.execute_drop_function(drop_function),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                ..
            } => self.execute_drop_procedure(*if_exists, proc_desc),
            Statement::DropTrigger(drop_trigger) => self.execute_drop_trigger(drop_trigger),
            Statement::Call(function) => self.execute_call(function),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Truncate(truncate) => self.execute_truncate(truncate),
            Statement::Analyze(analyze) => self.execute_analyze(analyze.table_name.as_ref()),
            Statement::Discard { object_type } => {
                if matches!(object_type, DiscardObject::ALL) {
                    self.discard_all()?;
                }
                Ok(SqlResult::command(match object_type {
                    DiscardObject::ALL => "DISCARD ALL",
                    DiscardObject::PLANS => "DISCARD PLANS",
                    DiscardObject::SEQUENCES => "DISCARD SEQUENCES",
                    DiscardObject::TEMP => "DISCARD TEMP",
                }))
            }
            Statement::Grant(grant) => self.execute_grant(grant),
            Statement::Revoke(revoke) => self.execute_revoke(revoke),
            Statement::CreatePolicy(create_policy) => self.execute_create_policy(create_policy),
            Statement::AlterPolicy(alter_policy) => self.execute_alter_policy(alter_policy),
            Statement::DropPolicy(drop_policy) => self.execute_drop_policy(drop_policy),
            Statement::Explain {
                analyze, statement, ..
            } => self.sql_engine().execute_explain(statement, *analyze),
            other => Err(SqlError::Unsupported(format!(
                "unsupported SQL statement {other}"
            ))),
        }
    }

    pub(crate) fn execute_analyze(&mut self, table_name: Option<&ObjectName>) -> Result<SqlResult> {
        if let Some(table_name) = table_name {
            let collection = relation_name(table_name)?;
            // ANALYZE samples real column values into pg_stats, so running it
            // is a read of the table's contents. Ungated, it turned the
            // statistics catalog into an on-demand exfiltration primitive:
            // analyze someone else's table, then read their values back out.
            self.require_table_ownership(&collection, "analyze it")?;
            self.analyze_collection_with_type_statistics(&collection)?;
        } else {
            self.analyze_all_permitted_with_type_statistics()?;
        }
        Ok(SqlResult::command("ANALYZE"))
    }

    /// A bare `ANALYZE` follows PostgreSQL: a superuser analyzes every table;
    /// anyone else analyzes the tables they own and the rest are skipped
    /// (PostgreSQL warns "skipping ... only table or database owner can
    /// analyze it"). Refusing the whole statement instead broke every client
    /// that ends a load with a plain ANALYZE as the schema's owning role —
    /// HammerDB's TPC-C schema build among them — while gaining nothing: the
    /// per-table gate below is what keeps foreign statistics out of pg_stats.
    pub(crate) fn analyze_all_permitted_with_type_statistics(&mut self) -> Result<()> {
        if self.current_user_is_superuser()? {
            return self.analyze_all_with_type_statistics();
        }
        let owned = list_schemas(self.db_ref())?
            .into_iter()
            .map(|schema| schema.name)
            .filter(|table| self.require_table_ownership(table, "analyze it").is_ok())
            .collect::<Vec<_>>();
        for collection in owned {
            self.analyze_collection_with_type_statistics(&collection)?;
        }
        Ok(())
    }

    pub(crate) fn analyze_collection_with_type_statistics(
        &mut self,
        collection: &str,
    ) -> Result<()> {
        self.db_mut()?.analyze_collection(collection)?;
        self.enrich_statistics_from_sample(collection)
    }

    pub(crate) fn analyze_all_with_type_statistics(&mut self) -> Result<()> {
        self.db_mut()?.analyze()?;
        let collections = list_schemas(self.db_ref())?
            .into_iter()
            .map(|schema| schema.name)
            .collect::<Vec<_>>();
        for collection in collections {
            self.enrich_statistics_from_sample(&collection)?;
        }
        Ok(())
    }

    /// One bounded sample of the table (see `analyze_sample_limit`) feeds the
    /// typed, range and network enrichment passes. Each pass used to scan and
    /// materialize the whole table on its own, which is what took the TPC-C
    /// seed loader past 50 GB inside HammerDB's closing ANALYZE.
    fn enrich_statistics_from_sample(&mut self, collection: &str) -> Result<()> {
        let Some(schema) = load_schema(self.db_ref(), collection)? else {
            return Ok(());
        };
        if self.db_ref().table_statistics(collection).is_none() {
            return Ok(());
        }
        let (records, total_rows) = self
            .db_ref()
            .sample_collection(collection, analyze_sample_limit())?;
        self.enrich_typed_statistics(collection, &schema, &records, total_rows)?;
        self.enrich_range_statistics(collection, &schema, &records)?;
        self.enrich_network_statistics(collection, &schema, &records)
    }

    pub(crate) fn enrich_typed_statistics(
        &mut self,
        collection: &str,
        schema: &TableSchema,
        records: &[Record],
        total_rows: usize,
    ) -> Result<()> {
        let Some(mut table_stats) = self.db_ref().table_statistics(collection).cloned() else {
            return Ok(());
        };
        let sample_rows = records.len();

        for column in schema.columns.iter().filter(|column| !column.hidden) {
            let field = IndexField::MetadataPath(vec![column.name.clone()]);
            let Some(column_stats) = table_stats.columns.values_mut().find(|stats| {
                index_field_matches(&stats.field, &field)
                    || (column.primary_key && stats.field == IndexField::Id)
            }) else {
                continue;
            };
            let mut null_count = 0usize;
            let mut width_bytes = 0usize;
            let mut frequencies = BTreeMap::<String, (String, usize)>::new();
            let mut ordered = Vec::<(String, String)>::new();
            for record in records {
                let value = record_column_value(record, schema, &column.name);
                if matches!(value, SqlValue::Null) {
                    null_count += 1;
                    continue;
                }
                let label = column_typed_index_label(column, &value)?;
                let display = value.to_cell();
                width_bytes = width_bytes.saturating_add(display.len());
                ordered.push((label.clone(), display.clone()));
                let frequency = frequencies.entry(label).or_insert((display, 0));
                frequency.1 += 1;
            }
            ordered.sort_by(|left, right| left.0.cmp(&right.0));
            let mut common = frequencies
                .iter()
                .map(|(label, (value, count))| (label.clone(), value.clone(), *count))
                .collect::<Vec<_>>();
            common.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
            common.truncate(100);
            // The sample's absolute counts are read by the planner as row
            // estimates for the whole table; scale them (no-op when the
            // sample is the table).
            for entry in &mut common {
                entry.2 = scale_sample_count(entry.2, sample_rows, total_rows);
            }
            let non_null = ordered.len();
            column_stats.null_count = scale_sample_count(null_count, sample_rows, total_rows);
            column_stats.distinct_count = estimate_distinct_from_sample(
                frequencies.len(),
                frequencies
                    .values()
                    .filter(|(_, count)| *count == 1)
                    .count(),
                sample_rows,
                total_rows,
            );
            if column.user_type.is_some() || uses_typed_storage(&column.pg_type) {
                column_stats.min = ordered
                    .first()
                    .map(|(label, _)| IndexValue::String(label.clone()));
                column_stats.max = ordered
                    .last()
                    .map(|(label, _)| IndexValue::String(label.clone()));
                column_stats.most_common = common
                    .iter()
                    .map(|(label, _, count)| ValueFrequency {
                        value: IndexValue::String(label.clone()),
                        count: *count,
                    })
                    .collect();
            }
            let histogram = histogram_quantiles(ordered, 100);
            column_stats.typed = Some(TypedColumnStatistics {
                pg_type: column.pg_type.clone(),
                avg_width: if non_null == 0 {
                    0
                } else {
                    width_bytes / non_null
                },
                most_common: common
                    .into_iter()
                    .map(|(_, value, count)| TypedValueFrequency { value, count })
                    .collect(),
                histogram_keys: histogram.iter().map(|(label, _)| label.clone()).collect(),
                histogram_values: histogram.into_iter().map(|(_, value)| value).collect(),
            });
        }
        self.db_mut()?.replace_table_statistics(table_stats)?;
        Ok(())
    }

    pub(crate) fn enrich_range_statistics(
        &mut self,
        collection: &str,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        let range_columns = schema
            .columns
            .iter()
            .filter(|column| {
                is_builtin_range_type(&column.pg_type)
                    || is_builtin_multirange_type(&column.pg_type)
            })
            .collect::<Vec<_>>();
        if range_columns.is_empty() {
            return Ok(());
        }
        let Some(mut table_stats) = self.db_ref().table_statistics(collection).cloned() else {
            return Ok(());
        };

        for column in range_columns {
            let mut samples = Vec::new();
            let mut bounds = Vec::new();
            let mut lengths = Vec::new();
            let mut empty_count = 0usize;
            for record in records {
                let value = record_column_value(record, schema, &column.name);
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let Some(parsed) = parse_pg_canonical_special(&column.pg_type, &value.to_cell())
                    .map_err(|error| SqlError::InvalidSql(error.to_string()))?
                else {
                    continue;
                };
                match parsed {
                    PgCanonicalValue::Range(range) => {
                        if range.empty {
                            empty_count += 1;
                        }
                        if let Some(length) = range_statistics_length(&range)? {
                            lengths.push(length);
                        }
                        bounds.push(range.to_postgres_text());
                        samples.push(range.to_postgres_text());
                    }
                    PgCanonicalValue::Multirange(ranges) => {
                        if ranges.is_empty() {
                            empty_count += 1;
                        }
                        let mut total = SqlValue::String("0".to_string());
                        let mut finite = true;
                        for range in &ranges {
                            let Some(length) = range_statistics_length(range)? else {
                                finite = false;
                                break;
                            };
                            total = eval_pg_numeric_arithmetic(
                                total,
                                &BinaryOperator::Plus,
                                SqlValue::String(length),
                            )?;
                        }
                        if finite && !ranges.is_empty() {
                            lengths.push(total.to_cell());
                        }
                        if let (Some(first), Some(last)) = (ranges.first(), ranges.last()) {
                            bounds.push(
                                PgRange {
                                    subtype: first.subtype.clone(),
                                    empty: false,
                                    lower: first.lower.clone(),
                                    upper: last.upper.clone(),
                                }
                                .to_postgres_text(),
                            );
                        }
                        samples.push(format_pg_multirange(&ranges));
                    }
                    _ => continue,
                }
            }
            samples.sort_by(|left, right| {
                column_typed_compare(
                    column,
                    &SqlValue::String(left.clone()),
                    &SqlValue::String(right.clone()),
                )
                .unwrap_or(Ordering::Equal)
            });
            let bounds_type =
                pg_range_statistics_bounds_type(&column.pg_type).unwrap_or(column.pg_type.as_str());
            bounds.sort_by(|left, right| {
                pg_typed_compare(
                    bounds_type,
                    &SqlValue::String(left.clone()),
                    &SqlValue::String(right.clone()),
                )
                .unwrap_or(Ordering::Equal)
            });
            lengths.sort_by(|left, right| {
                pg_typed_compare(
                    "numeric",
                    &SqlValue::String(left.clone()),
                    &SqlValue::String(right.clone()),
                )
                .unwrap_or(Ordering::Equal)
            });
            let sample_count = samples.len();
            let range = RangeStatistics {
                pg_type: column.pg_type.clone(),
                sample_count,
                empty_count,
                samples: histogram_quantiles(samples, 100),
                bounds_histogram: histogram_quantiles(bounds, 100),
                length_histogram: histogram_quantiles(lengths, 100),
            };
            if let Some(column_stats) = table_stats.columns.values_mut().find(|stats| {
                index_field_matches(
                    &stats.field,
                    &IndexField::MetadataPath(vec![column.name.clone()]),
                )
            }) {
                column_stats.range = Some(range);
            }
        }
        self.db_mut()?.replace_table_statistics(table_stats)?;
        Ok(())
    }

    pub(crate) fn enrich_network_statistics(
        &mut self,
        collection: &str,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        let network_columns = schema
            .columns
            .iter()
            .filter(|column| matches!(column.pg_type.as_str(), "inet" | "cidr"))
            .collect::<Vec<_>>();
        if network_columns.is_empty() {
            return Ok(());
        }
        let Some(mut table_stats) = self.db_ref().table_statistics(collection).cloned() else {
            return Ok(());
        };

        for column in network_columns {
            let mut samples = Vec::new();
            for record in records {
                let value = record_column_value(record, schema, &column.name);
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let Some(PgCanonicalValue::Network(network)) =
                    parse_pg_canonical_special(&column.pg_type, &value.to_cell())
                        .map_err(|error| SqlError::InvalidSql(error.to_string()))?
                else {
                    continue;
                };
                samples.push(network.to_postgres_text());
            }
            samples.sort_by(|left, right| {
                column_typed_compare(
                    column,
                    &SqlValue::String(left.clone()),
                    &SqlValue::String(right.clone()),
                )
                .unwrap_or(Ordering::Equal)
            });
            let sample_count = samples.len();
            let network = NetworkStatistics {
                pg_type: column.pg_type.clone(),
                sample_count,
                samples: histogram_quantiles(samples, 100),
            };
            if let Some(column_stats) = table_stats.columns.values_mut().find(|stats| {
                index_field_matches(
                    &stats.field,
                    &IndexField::MetadataPath(vec![column.name.clone()]),
                )
            }) {
                column_stats.network = Some(network);
            }
        }
        self.db_mut()?.replace_table_statistics(table_stats)?;
        Ok(())
    }

    pub(crate) fn execute_create_schema(
        &mut self,
        schema_name: &SchemaName,
        if_not_exists: bool,
    ) -> Result<SqlResult> {
        let name = schema_name_value(schema_name)?;
        let owner = match schema_name {
            SchemaName::Simple(_) => current_user_from_gucs(&self.session_gucs),
            SchemaName::NamedAuthorization(_, owner) | SchemaName::UnnamedAuthorization(owner) => {
                let owner = normalize_role_name(&ident_value(owner));
                ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                self.require_settable_new_owner(&owner, &format!("schema {name}"))?;
                owner
            }
        };
        self.create_session_namespace_if_missing(NamespaceSchema { name, owner }, if_not_exists)?;
        Ok(SqlResult::command("CREATE SCHEMA"))
    }

    pub(crate) fn execute_raw_alter_schema_owner(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some((name, owner)) = parse_raw_alter_schema_owner(sql)? else {
            return Ok(None);
        };
        ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
        let previous = load_namespace(self.db_ref(), &name)?;
        let current_owner = previous
            .as_ref()
            .map(|namespace| namespace.owner.clone())
            .unwrap_or_else(current_role_name);
        let object = format!("schema {name}");
        self.require_object_ownership(&current_owner, &object, "change its owner")?;
        self.require_settable_new_owner(&owner, &object)?;
        save_namespace(
            self.db_mut()?,
            NamespaceSchema {
                name: name.clone(),
                owner,
            },
        )?;
        if self.tx.is_some() {
            match previous {
                Some(namespace) => self.ddl_undo.push(DdlUndo::RestoreNamespace { namespace }),
                None => self
                    .ddl_undo
                    .push(DdlUndo::DeleteNamespace { namespace: name }),
            }
        }
        Ok(Some(SqlResult::command("ALTER SCHEMA")))
    }

    pub(crate) fn execute_raw_pg_dump_range_type(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(normalized) = normalize_pg_dump_range_zero_options(sql)? else {
            return Ok(None);
        };
        let statements = parse_statements(&normalized)?;
        let [statement] = statements.as_slice() else {
            return Err(SqlError::InvalidSql(
                "CREATE TYPE AS RANGE must contain one statement".to_string(),
            ));
        };
        Ok(Some(self.execute_statement(statement)?))
    }

    pub(crate) fn execute_raw_alter_view_owner(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((name, owner)) = parse_raw_alter_view_owner(sql)? else {
            return Ok(None);
        };
        ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
        let mut view = load_view(self.db_ref(), &name)?
            .ok_or_else(|| SqlError::InvalidCollection(name.clone()))?;
        let current_owner = view.owner.clone().unwrap_or_else(current_role_name);
        self.require_object_ownership(&current_owner, &format!("view {name}"), "change its owner")?;
        // Views execute with DEFINER semantics (`materialize_view_with_selection`
        // runs the body as the view owner), so handing one to a role the caller
        // does not hold is a read-privilege escalation: the caller keeps SELECT
        // on the view and the view keeps the new owner's access to the source
        // tables. Only a role you could `SET ROLE` to may receive it.
        self.require_settable_new_owner(&owner, &format!("view {name}"))?;
        view.owner = Some(owner);
        self.save_session_view(&view)?;
        Ok(Some(SqlResult::command("ALTER VIEW")))
    }

    pub(crate) fn execute_create_extension(
        &mut self,
        create_extension: &CreateExtension,
    ) -> Result<SqlResult> {
        self.require_superuser_for_admin_operation("create extensions")?;
        let extension = ExtensionSchema {
            name: ident_value(&create_extension.name),
            schema: create_extension
                .schema
                .as_ref()
                .map(ident_value)
                .unwrap_or_else(|| "public".to_string()),
            version: create_extension.version.as_ref().map(ident_value),
        };
        reject_postgis_extension(&extension.name)?;
        save_extension_if_missing(self.db_mut()?, extension, create_extension.if_not_exists)?;
        Ok(SqlResult::command("CREATE EXTENSION"))
    }

    pub(crate) fn execute_raw_create_extension(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((extension, if_not_exists)) = parse_raw_create_extension(sql)? else {
            return Ok(None);
        };
        self.require_superuser_for_admin_operation("create extensions")?;
        reject_postgis_extension(&extension.name)?;
        save_extension_if_missing(self.db_mut()?, extension, if_not_exists)?;
        Ok(Some(SqlResult::command("CREATE EXTENSION")))
    }

    pub(crate) fn execute_raw_extension_catalog(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(ddl) = parse_extension_catalog_ddl(sql)? else {
            return Ok(None);
        };
        // The whole extension/app-hosting surface — install, activate,
        // upgrade, drop, plus RESOURCE / EVENT SUBSCRIPTION / WEBSITE — loads
        // and runs executable WASM that registers HTTP routes, database-event
        // handlers and queue consumers. It was reachable by any authenticated
        // role. PostgreSQL requires superuser for CREATE EXTENSION.
        self.require_superuser_for_admin_operation("manage extensions")?;
        let command = match ddl {
            ExtensionCatalogDdl::Install {
                name,
                module_sha256,
                manifest,
                if_not_exists,
            } => {
                reject_postgis_extension(&name)?;
                let legacy = list_extensions(self.db_ref())?
                    .into_iter()
                    .find(|extension| extension.name.eq_ignore_ascii_case(&name));
                if legacy.is_some() && load_extension(self.db_ref(), &name)?.is_none() {
                    return Err(SqlError::InvalidSql(format!(
                        "extension \"{name}\" already exists without an executable package"
                    )));
                }
                let installation = ExtensionInstallation {
                    manifest,
                    module_sha256,
                    state: ExtensionState::Staged,
                    installed_at_ms: unix_now_ms(),
                    activation: None,
                    last_error: None,
                };
                let created =
                    install_extension(self.db_mut()?, installation.clone(), if_not_exists)?;
                if created {
                    let legacy_created = legacy.is_none();
                    if legacy_created {
                        if let Err(error) = save_extension_if_missing(
                            self.db_mut()?,
                            ExtensionSchema {
                                name: name.clone(),
                                schema: "public".to_string(),
                                version: Some(installation.manifest.identity.version.clone()),
                            },
                            false,
                        ) {
                            let _ = delete_extension(self.db_mut()?, &name, true);
                            return Err(error);
                        }
                    }
                    if self.tx.is_some() {
                        if legacy_created {
                            self.ddl_undo
                                .push(DdlUndo::DeleteLegacyExtension { name: name.clone() });
                        }
                        self.ddl_undo
                            .push(DdlUndo::DeleteExtensionInstallation { name });
                    }
                }
                "CREATE EXTENSION"
            }
            ExtensionCatalogDdl::ActivateSingleNode { name } => {
                let previous = activate_extension_single_node(self.db_mut()?, &name)?;
                if self.tx.is_some() {
                    for installation in previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionInstallation { installation });
                    }
                }
                "ALTER EXTENSION"
            }
            ExtensionCatalogDdl::UpgradeSingleNode {
                name,
                module_sha256,
                manifest,
            } => {
                let previous_legacy = list_extensions(self.db_ref())?
                    .into_iter()
                    .find(|extension| extension.name.eq_ignore_ascii_case(&name));
                let version = manifest.identity.version.clone();
                let changed =
                    upgrade_extension_single_node(self.db_mut()?, &name, module_sha256, manifest)?;
                self.db_mut()?.insert(
                    EXTENSION_COLLECTION,
                    Record::new(&name).with_metadata(serde_json::to_value(ExtensionSchema {
                        name: name.clone(),
                        schema: "public".to_string(),
                        version: Some(version),
                    })?),
                )?;
                if self.tx.is_some() {
                    for installation in changed {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionInstallation { installation });
                    }
                    match previous_legacy {
                        Some(extension) => self
                            .ddl_undo
                            .push(DdlUndo::RestoreLegacyExtension { extension }),
                        None => self.ddl_undo.push(DdlUndo::DeleteLegacyExtension { name }),
                    }
                }
                "ALTER EXTENSION"
            }
            ExtensionCatalogDdl::Disable { name, cascade } => {
                let previous = disable_extension(self.db_mut()?, &name, cascade)?;
                if self.tx.is_some() {
                    for installation in previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionInstallation { installation });
                    }
                }
                "ALTER EXTENSION"
            }
            ExtensionCatalogDdl::DropExtension {
                name,
                if_exists,
                cascade,
            } => {
                let installation = load_extension(self.db_ref(), &name)?;
                let installations_before = if installation.is_some() && cascade {
                    list_installed_extensions(self.db_ref())?
                } else {
                    Vec::new()
                };
                let legacy = list_extensions(self.db_ref())?
                    .into_iter()
                    .find(|extension| extension.name.eq_ignore_ascii_case(&name));
                if installation.is_none() && legacy.is_none() {
                    if !if_exists {
                        return Err(SqlError::InvalidSql(format!(
                            "extension \"{name}\" does not exist"
                        )));
                    }
                    return Ok(Some(SqlResult::command("DROP EXTENSION")));
                }
                let resources = if installation.is_some() {
                    list_rest_resources(self.db_ref())?
                        .into_iter()
                        .filter(|resource| resource.extension.eq_ignore_ascii_case(&name))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let bindings = if installation.is_some() {
                    list_event_bindings(self.db_ref())?
                        .into_iter()
                        .filter(|binding| binding.extension.eq_ignore_ascii_case(&name))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let websites = if installation.is_some() {
                    list_websites(self.db_ref())?
                        .into_iter()
                        .filter(|website| website.extension.eq_ignore_ascii_case(&name))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let mut website_releases = Vec::new();
                for website in &websites {
                    website_releases.extend(list_website_releases(self.db_ref(), &website.name)?);
                }
                if installation.is_some() {
                    delete_extension(self.db_mut()?, &name, cascade)?;
                }
                if legacy.is_some() {
                    match self.db_mut()?.delete(EXTENSION_COLLECTION, &name) {
                        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                if self.tx.is_some() {
                    for resource in resources {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionResource { resource });
                    }
                    for binding in bindings {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionEventBinding { binding });
                    }
                    for release in website_releases {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionWebsiteRelease { release });
                    }
                    for website in websites {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionWebsite { website });
                    }
                    if let Some(installation) = installation {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionInstallation { installation });
                    }
                    for dependent in installations_before {
                        if !dependent.manifest.identity.name.eq_ignore_ascii_case(&name) {
                            self.ddl_undo.push(DdlUndo::RestoreExtensionInstallation {
                                installation: dependent,
                            });
                        }
                    }
                    if let Some(extension) = legacy {
                        self.ddl_undo
                            .push(DdlUndo::RestoreLegacyExtension { extension });
                    }
                }
                "DROP EXTENSION"
            }
            ExtensionCatalogDdl::CreateResource {
                definition,
                if_not_exists,
            } => {
                let name = definition.name.clone();
                if save_rest_resource(self.db_mut()?, definition, if_not_exists)?
                    && self.tx.is_some()
                {
                    self.ddl_undo
                        .push(DdlUndo::DeleteExtensionResource { name });
                }
                "CREATE RESOURCE"
            }
            ExtensionCatalogDdl::DropResource { name, if_exists } => {
                let previous = load_rest_resource(self.db_ref(), &name)?;
                if !delete_rest_resource(self.db_mut()?, &name)? && !if_exists {
                    return Err(SqlError::InvalidSql(format!(
                        "resource `{name}` does not exist"
                    )));
                }
                if self.tx.is_some() {
                    if let Some(resource) = previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionResource { resource });
                    }
                }
                "DROP RESOURCE"
            }
            ExtensionCatalogDdl::CreateEventBinding {
                definition,
                if_not_exists,
            } => {
                let name = definition.name.clone();
                if save_event_binding(self.db_mut()?, definition, if_not_exists)?
                    && self.tx.is_some()
                {
                    self.ddl_undo
                        .push(DdlUndo::DeleteExtensionEventBinding { name });
                }
                "CREATE EVENT SUBSCRIPTION"
            }
            ExtensionCatalogDdl::DropEventBinding { name, if_exists } => {
                let previous = load_event_binding(self.db_ref(), &name)?;
                if !delete_event_binding(self.db_mut()?, &name)? && !if_exists {
                    return Err(SqlError::InvalidSql(format!(
                        "event subscription `{name}` does not exist"
                    )));
                }
                if self.tx.is_some() {
                    if let Some(binding) = previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionEventBinding { binding });
                    }
                }
                "DROP EVENT SUBSCRIPTION"
            }
            ExtensionCatalogDdl::CreateWebsite {
                definition,
                if_not_exists,
            } => {
                let name = definition.name.clone();
                if save_website(self.db_mut()?, definition, if_not_exists)? && self.tx.is_some() {
                    self.ddl_undo.push(DdlUndo::DeleteExtensionWebsite { name });
                }
                "CREATE WEBSITE"
            }
            ExtensionCatalogDdl::PublishWebsite { release, activate } => {
                let website = release.website.clone();
                let version = release.version.clone();
                save_website_release(self.db_mut()?, release)?;
                let previous = if activate {
                    match activate_website_version(self.db_mut()?, &website, &version) {
                        Ok(previous) => Some(previous),
                        Err(error) => {
                            let _ = delete_website_release(self.db_mut()?, &website, &version);
                            return Err(error);
                        }
                    }
                } else {
                    None
                };
                if self.tx.is_some() {
                    self.ddl_undo
                        .push(DdlUndo::DeleteExtensionWebsiteRelease { website, version });
                    if let Some(website) = previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionWebsite { website });
                    }
                }
                "PUBLISH WEBSITE"
            }
            ExtensionCatalogDdl::ActivateWebsite { name, version } => {
                let previous = activate_website_version(self.db_mut()?, &name, &version)?;
                if self.tx.is_some() {
                    self.ddl_undo
                        .push(DdlUndo::RestoreExtensionWebsite { website: previous });
                }
                "ALTER WEBSITE"
            }
            ExtensionCatalogDdl::RollbackWebsite { name } => {
                let previous = rollback_website(self.db_mut()?, &name)?;
                if self.tx.is_some() {
                    self.ddl_undo
                        .push(DdlUndo::RestoreExtensionWebsite { website: previous });
                }
                "ALTER WEBSITE"
            }
            ExtensionCatalogDdl::DropWebsite {
                name,
                if_exists,
                cascade,
            } => {
                let previous = load_website(self.db_ref(), &name)?;
                let releases = if previous.is_some() {
                    list_website_releases(self.db_ref(), &name)?
                } else {
                    Vec::new()
                };
                if !delete_website(self.db_mut()?, &name, cascade)? && !if_exists {
                    return Err(SqlError::InvalidSql(format!(
                        "website `{name}` does not exist"
                    )));
                }
                if self.tx.is_some() {
                    for release in releases {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionWebsiteRelease { release });
                    }
                    if let Some(website) = previous {
                        self.ddl_undo
                            .push(DdlUndo::RestoreExtensionWebsite { website });
                    }
                }
                "DROP WEBSITE"
            }
        };
        Ok(Some(SqlResult::command(command)))
    }
}
