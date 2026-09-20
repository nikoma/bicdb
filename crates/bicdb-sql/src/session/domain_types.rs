//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl<'db> SqlSession<'db> {
    pub(crate) fn drop_user_type_definition(
        &mut self,
        user_type: &UserTypeSchema,
        cascade: bool,
        dropped: &mut BTreeSet<(String, String)>,
    ) -> Result<()> {
        let identity = (user_type.schema_name.clone(), user_type.name.clone());
        if dropped.contains(&identity) {
            return Ok(());
        }

        if let UserTypeKind::Multirange {
            range_schema_name,
            range_name,
            ..
        } = &user_type.kind
        {
            let range_identity = (range_schema_name.clone(), range_name.clone());
            if !dropped.contains(&range_identity) {
                if let Some(range) = load_user_type(self.db_ref(), range_schema_name, range_name)? {
                    if !cascade {
                        return Err(SqlError::dependent_objects_still_exist(format!(
                            "cannot drop type {}.{} because type {}.{} requires it",
                            user_type.schema_name, user_type.name, range.schema_name, range.name
                        )));
                    }
                    self.drop_user_type_definition(&range, true, dropped)?;
                    return Ok(());
                }
            }
        }

        let paired_multirange_oid = match &user_type.kind {
            UserTypeKind::Range { multirange_oid, .. } => Some(*multirange_oid),
            _ => None,
        };

        let dependents = list_user_types(self.db_ref())?
            .into_iter()
            .filter(|candidate| {
                Some(candidate.oid) != paired_multirange_oid
                    && user_type_kind_depends_on_oid(&candidate.kind, user_type.oid)
            })
            .collect::<Vec<_>>();
        if let Some(dependent) = dependents.first().filter(|_| !cascade) {
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop type {}.{} because type {}.{} depends on it",
                user_type.schema_name, user_type.name, dependent.schema_name, dependent.name
            )));
        }
        for dependent in dependents {
            self.drop_user_type_definition(&dependent, true, dropped)?;
        }

        let routines = list_routines(self.db_ref())?
            .into_iter()
            .filter(|routine| routine_depends_on_user_type(routine, user_type))
            .collect::<Vec<_>>();
        if let Some(routine) = routines.first().filter(|_| !cascade) {
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop type {}.{} because function {} depends on it",
                user_type.schema_name, user_type.name, routine.name
            )));
        }
        for routine in routines {
            self.delete_session_routine(&routine)?;
        }

        let mut columns = Vec::new();
        for schema in list_schemas(self.db_ref())? {
            for column in schema.columns {
                if column
                    .user_type
                    .as_ref()
                    .is_some_and(|column_type| column_type.oid == user_type.oid)
                {
                    columns.push((schema.name.clone(), column.name));
                }
            }
        }
        if let Some((table, column)) = columns.first().filter(|_| !cascade) {
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop type {}.{} because column {column} of table {table} depends on it",
                user_type.schema_name, user_type.name
            )));
        }
        for (table, column) in columns {
            let mut schema = load_schema(self.db_ref(), &table)?
                .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
            self.capture_table_state_undo(&table, &schema)?;
            alter_table_drop_column(self.db_mut()?, &table, &mut schema, &column, false)?;
            self.change_column_privileges(&table, &column, None)?;
        }

        if let UserTypeKind::Range {
            multirange_schema_name,
            multirange_name,
            ..
        } = &user_type.kind
        {
            dropped.insert(identity.clone());
            if let Some(multirange) =
                load_user_type(self.db_ref(), multirange_schema_name, multirange_name)?
            {
                if let Err(error) = self.drop_user_type_definition(&multirange, cascade, dropped) {
                    dropped.remove(&identity);
                    return Err(error);
                }
            }
        }
        let privilege_name = user_type_privilege_name(&user_type.schema_name, &user_type.name);
        for grant in list_privileges(self.db_ref())?
            .into_iter()
            .filter(|grant| {
                grant.object_type == PrivilegeObjectType::Type
                    && grant.object_name == privilege_name
            })
            .collect::<Vec<_>>()
        {
            self.delete_session_privilege(&grant)?;
        }
        self.delete_session_user_type(&user_type.schema_name, &user_type.name)?;
        dropped.insert(identity);
        Ok(())
    }

    pub(crate) fn sync_user_type_columns(
        &mut self,
        user_type: &UserTypeSchema,
        renamed_label: Option<(&str, &str)>,
    ) -> Result<()> {
        let mut validation_type = user_type.clone();
        if let UserTypeKind::Domain { constraints, .. } = &mut validation_type.kind {
            constraints.retain(|constraint| constraint.validated);
        }
        for mut schema in list_schemas(self.db_ref())? {
            let dependent_columns = schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| {
                    column
                        .user_type
                        .as_ref()
                        .filter(|column_type| column_type.oid == user_type.oid)
                        .map(|column_type| (index, column_type.array))
                })
                .collect::<Vec<_>>();
            if dependent_columns.is_empty() {
                continue;
            }
            for record in self.db_ref().scan_collection(&schema.name)? {
                for (index, array) in &dependent_columns {
                    let column = &schema.columns[*index];
                    if column.primary_key {
                        continue;
                    }
                    let mut value = record_column_value(&record, &schema, &column.name);
                    if let Some((from, to)) = renamed_label {
                        rename_enum_label_in_value(&mut value, from, to);
                    }
                    let column_type = validation_type.column_type(*array);
                    cast_value_to_user_type(value, &column_type)?;
                }
            }
            self.capture_table_state_undo(&schema.name, &schema)?;
            for (index, array) in &dependent_columns {
                let column_type = user_type.column_type(*array);
                schema.columns[*index].pg_type = column_type.formatted_name();
                schema.columns[*index].user_type = Some(column_type);
            }
            if let Some((from, to)) = renamed_label {
                for mut record in self.db_ref().scan_collection(&schema.name)? {
                    for (index, _) in &dependent_columns {
                        let column = &schema.columns[*index];
                        if column.primary_key {
                            continue;
                        }
                        let mut value = record_column_value(&record, &schema, &column.name);
                        rename_enum_label_in_value(&mut value, from, to);
                        set_record_column(&mut record, Some(&schema), &column.name, value)?;
                    }
                    self.db_mut()?.insert(&schema.name, record)?;
                }
            }
            save_schema(self.db_mut()?, &schema)?;
        }
        let replacements = BTreeMap::from([(user_type.oid, user_type.clone())]);
        for mut dependent in list_user_types(self.db_ref())? {
            if dependent.oid == user_type.oid {
                continue;
            }
            if replace_user_type_kind_references(&mut dependent.kind, &replacements) {
                self.save_session_user_type(&dependent)?;
                self.sync_user_type_columns(&dependent, None)?;
            }
        }
        Ok(())
    }

    pub(crate) fn execute_create_view(&mut self, create_view: &CreateView) -> Result<SqlResult> {
        if create_view.temporary {
            return Err(SqlError::Unsupported(
                "temporary views are not supported".to_string(),
            ));
        }
        if create_view.or_alter {
            return Err(SqlError::Unsupported(
                "CREATE OR ALTER VIEW is not supported".to_string(),
            ));
        }

        let view = relation_name(&create_view.name)?;
        if load_schema(self.db_ref(), &view)?.is_some()
            || load_sequence(self.db_ref(), &view)?.is_some()
            || user_collection_names(self.db_ref())
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&view))
        {
            return Err(SqlError::InvalidSql(format!(
                "relation \"{view}\" already exists"
            )));
        }
        if load_view(self.db_ref(), &view)?.is_some() && !create_view.or_replace {
            if create_view.if_not_exists {
                return Ok(SqlResult::command("CREATE VIEW"));
            }
            return Err(SqlError::InvalidSql(format!(
                "relation \"{view}\" already exists"
            )));
        }

        let mut columns = match view_columns_from_query(&create_view.query) {
            Ok(columns) => columns,
            Err(metadata_error) => {
                let query_result = self.sql_engine().execute_query(&create_view.query);
                match query_result {
                    Ok(result) => result
                        .columns
                        .iter()
                        .map(|name| text_column_schema(name.clone()))
                        .collect::<Vec<_>>(),
                    Err(SqlError::Unsupported(_) | SqlError::InvalidCollection(_)) => {
                        return Err(metadata_error);
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        inherit_view_source_column_types(self.db_ref(), &create_view.query, &mut columns)?;
        if !create_view.columns.is_empty() {
            if create_view.columns.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "CREATE VIEW specifies {} column names, but query returns {} columns",
                    create_view.columns.len(),
                    columns.len()
                )));
            }
            for (column, alias) in columns.iter_mut().zip(create_view.columns.iter()) {
                column.name = alias.name.value.clone();
                if let Some(data_type) = &alias.data_type {
                    let (pg_type, vector_dim) = pg_type_from_data_type(data_type)?;
                    column.pg_type = pg_type;
                    column.type_modifier = pg_type_modifier_from_data_type(data_type)?;
                    column.vector_dim = vector_dim;
                }
            }
        }
        apply_catalog_view_column_types(&view, &create_view.query.to_string(), &mut columns);

        let mut security_invoker = false;
        let mut security_barrier = false;
        if let CreateTableOptions::With(options) = &create_view.options {
            for option in options {
                let SqlOption::KeyValue { key, value } = option else {
                    return Err(SqlError::Unsupported(format!(
                        "CREATE VIEW option {option} is not supported"
                    )));
                };
                let enabled = match eval_constant_expr(value)? {
                    SqlValue::Bool(enabled) => enabled,
                    SqlValue::String(text) => {
                        text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("on")
                    }
                    other => {
                        return Err(SqlError::InvalidSql(format!(
                            "CREATE VIEW option {} expects a boolean, got {}",
                            key.value,
                            other.to_cell()
                        )));
                    }
                };
                match key.value.to_ascii_lowercase().as_str() {
                    "security_invoker" => security_invoker = enabled,
                    "security_barrier" => security_barrier = enabled,
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "CREATE VIEW option {other} is not supported"
                        )));
                    }
                }
            }
        }
        if create_view.materialized {
            // A materialized view READS its sources and stores the result, so
            // PostgreSQL requires SELECT on them at creation time. A plain view
            // stores only the definition and is gated when it is read, which is
            // why the check is scoped to the materialized case.
            self.sql_engine()
                .authorize_read_relations(&create_view.query)?;
        }
        self.save_session_view(&ViewSchema {
            name: view,
            query_sql: create_view.query.to_string(),
            columns,
            materialized: create_view.materialized,
            indexes: Vec::new(),
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            security_invoker,
            security_barrier,
        })?;
        Ok(SqlResult::command(if create_view.materialized {
            "CREATE MATERIALIZED VIEW"
        } else {
            "CREATE VIEW"
        }))
    }

    pub(crate) fn execute_lock(&mut self, lock: &Lock) -> Result<SqlResult> {
        let relation_names = catalog_table_names(self.db_ref())
            .into_iter()
            .map(|table| table.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        for target in &lock.tables {
            let table = relation_name(&target.name)?;
            if !relation_names.contains(&table.to_ascii_lowercase()) {
                return Err(SqlError::InvalidCollection(table));
            }
            // PostgreSQL requires a privilege on the relation to lock it; this
            // validated existence only, so any role could lock any tenant's
            // table. SELECT is the weakest right any LOCK mode accepts.
            self.require_table_privilege(&table, "SELECT")?;
        }
        Ok(SqlResult::command("LOCK TABLE"))
    }

    pub(crate) fn execute_create_sequence(
        &mut self,
        temporary: bool,
        if_not_exists: bool,
        name: &ObjectName,
        data_type: Option<&DataType>,
        sequence_options: &[SequenceOptions],
        owned_by: Option<&ObjectName>,
    ) -> Result<SqlResult> {
        if temporary {
            return Err(SqlError::Unsupported(
                "temporary sequences are not supported".to_string(),
            ));
        }
        let sequence_data_type = if let Some(data_type) = data_type {
            let (pg_type, _) = pg_type_from_data_type(data_type)?;
            if !matches!(pg_type.as_str(), "int2" | "int4" | "int8") {
                return Err(SqlError::Unsupported(
                    "CREATE SEQUENCE AS supports only integer types".to_string(),
                ));
            }
            pg_type
        } else {
            "int8".to_string()
        };
        let sequence_name = relation_name(name)?;
        let mut sequence =
            sequence_from_options(sequence_name, &sequence_data_type, sequence_options)?;
        if let Some(owned_by) = owned_by {
            let owned_by = relation_name(owned_by)?;
            let parts = owned_by.split('.').collect::<Vec<_>>();
            if parts.len() >= 2 {
                sequence.owned_by_table = Some(parts[parts.len() - 2].to_string());
                sequence.owned_by_column = Some(parts[parts.len() - 1].to_string());
            }
        }
        self.create_session_sequence_if_missing(sequence, if_not_exists)?;
        Ok(SqlResult::command("CREATE SEQUENCE"))
    }

    pub(crate) fn execute_drop(
        &mut self,
        object_type: ObjectType,
        if_exists: bool,
        names: &[ObjectName],
        cascade: bool,
    ) -> Result<SqlResult> {
        match object_type {
            ObjectType::Table => {
                for name in names {
                    let table = relation_name(name)?;
                    // DROP TABLE destroys a relation outright and had no
                    // ownership check: any authenticated role could delete
                    // any tenant's table.
                    self.require_table_ownership(&table, "drop it")?;
                    let schema = load_schema(self.db_ref(), &table)?;
                    if let Some(schema) = &schema {
                        self.drop_table_row_type_dependents(schema, cascade)?;
                    }
                    let schema_existed = schema.is_some();
                    let existed = self.drop_session_collection(&table)?;
                    self.delete_session_schema(&table)?;
                    if existed || schema_existed {
                        self.drop_owned_sequences_for_table(&table)?;
                    }
                    if !existed && !schema_existed && !if_exists {
                        return Err(SqlError::InvalidCollection(table));
                    }
                }
                Ok(SqlResult::command("DROP TABLE"))
            }
            ObjectType::Index => {
                for name in names {
                    let index = relation_name(name)?;
                    // An index belongs to its table, so the table's
                    // authority governs it — dropping an index is a
                    // performance and constraint change to someone else's
                    // relation.
                    if let Some(table) = self.table_owning_index(&index)? {
                        self.require_table_ownership(&table, "drop an index on it")?;
                    }
                    if list_schemas(self.db_ref())?.iter().any(|schema| {
                        schema.constraints.iter().any(|constraint| {
                            matches!(constraint, ConstraintSchema::Exclusion { name, .. } if name.eq_ignore_ascii_case(&index))
                        })
                    }) {
                        return Err(SqlError::dependent_objects_still_exist(format!(
                            "cannot drop index {index} because constraint {index} requires it"
                        )));
                    }
                    let existed = self.drop_session_index(&index)?;
                    if !existed && !if_exists {
                        return Err(SqlError::InvalidSql(format!("index `{index}` not found")));
                    }
                }
                Ok(SqlResult::command("DROP INDEX"))
            }
            ObjectType::Sequence => {
                for name in names {
                    let sequence = relation_name(name)?;
                    if let Some(owned) = load_sequence(self.db_ref(), &sequence)? {
                        self.require_object_ownership(
                            &owned.owner,
                            &format!("sequence {sequence}"),
                            "drop it",
                        )?;
                        let mut dependent_column =
                            list_schemas(self.db_ref())?.into_iter().find_map(|schema| {
                                schema.columns.iter().find_map(|column| {
                                    column
                                        .default_sequence
                                        .as_deref()
                                        .is_some_and(|candidate| {
                                            candidate.eq_ignore_ascii_case(&sequence)
                                        })
                                        .then(|| {
                                            (
                                                schema.name.clone(),
                                                column.name.clone(),
                                                column.identity.is_some(),
                                            )
                                        })
                                })
                            });
                        if dependent_column.is_none() {
                            dependent_column = match (
                                owned.owned_by_table.as_ref(),
                                owned.owned_by_column.as_ref(),
                            ) {
                                (Some(table), Some(column)) => load_schema(self.db_ref(), table)?
                                    .and_then(|schema| {
                                        schema.column(column).map(|column_schema| {
                                            (
                                                table.clone(),
                                                column.clone(),
                                                column_schema.identity.is_some(),
                                            )
                                        })
                                    }),
                                _ => None,
                            };
                        }
                        if let Some((table, column, identity)) = &dependent_column {
                            if *identity {
                                return Err(SqlError::dependent_objects_still_exist(format!(
                                    "cannot drop sequence {sequence} because column {column} of table {table} requires it"
                                )));
                            }
                            if !cascade {
                                return Err(SqlError::dependent_objects_still_exist(format!(
                                    "cannot drop sequence {sequence} because other objects depend on it"
                                )));
                            }
                            if let Some(mut schema) = load_schema(self.db_ref(), table)? {
                                if let Some(column) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
                                {
                                    if column.default_sequence.as_deref().is_some_and(|candidate| {
                                        candidate.eq_ignore_ascii_case(&sequence)
                                    }) {
                                        column.default_sequence = None;
                                        column.default_value = None;
                                        column.default_expr = None;
                                        self.save_session_schema(&schema)?;
                                    }
                                }
                            }
                        }
                    }
                    let existed = self.delete_session_sequence(&sequence)?;
                    if !existed && !if_exists {
                        return Err(SqlError::InvalidSql(format!(
                            "sequence `{sequence}` does not exist"
                        )));
                    }
                }
                Ok(SqlResult::command("DROP SEQUENCE"))
            }
            ObjectType::Type => {
                let mut dropped = BTreeSet::new();
                for name in names {
                    let (schema_name, name) = user_type_identity(name)?;
                    if dropped.contains(&(schema_name.clone(), name.clone())) {
                        continue;
                    }
                    let Some(user_type) = load_user_type(self.db_ref(), &schema_name, &name)?
                    else {
                        if if_exists {
                            continue;
                        }
                        return Err(SqlError::undefined_type(format!("{schema_name}.{name}")));
                    };
                    self.require_object_ownership(
                        &user_type.owner,
                        &format!("type {schema_name}.{name}"),
                        "drop it",
                    )?;
                    self.drop_user_type_definition(&user_type, cascade, &mut dropped)?;
                }
                Ok(SqlResult::command("DROP TYPE"))
            }
            ObjectType::View => {
                for name in names {
                    let view = relation_name(name)?;
                    // Same hole the DROP TABLE gate above closed, left open on
                    // the sibling branch: any authenticated role could destroy
                    // any tenant's view.
                    if let Some(loaded) = load_view(self.db_ref(), &view)? {
                        let owner = loaded.owner.unwrap_or_else(current_role_name);
                        self.require_object_ownership(&owner, &format!("view {view}"), "drop it")?;
                    }
                    let existed = self.delete_session_view(&view)?;
                    if !existed && !if_exists {
                        return Err(SqlError::InvalidCollection(view));
                    }
                }
                Ok(SqlResult::command("DROP VIEW"))
            }
            ObjectType::MaterializedView => Err(SqlError::Unsupported(
                "MATERIALIZED VIEW is not supported".to_string(),
            )),
            ObjectType::Role | ObjectType::User => {
                self.require_role_management_privilege("drop roles")?;
                let mut roles_to_drop = Vec::new();
                for name in names {
                    let role = normalize_role_name(&object_name(name)?);
                    if role == current_role_name() {
                        return Err(SqlError::InvalidSql(format!(
                            "current user cannot be dropped: role \"{role}\""
                        )));
                    }
                    if !role_exists(self.db_ref(), &role)? {
                        if if_exists {
                            continue;
                        }
                        return Err(SqlError::UndefinedRole { name: role });
                    }
                    let target = load_role_schema(self.db_ref(), &role)?
                        .ok_or_else(|| SqlError::UndefinedRole { name: role.clone() })?;
                    self.ensure_role_target_manageable(&target, "drop")?;
                    // PostgreSQL refuses to drop a role that still owns
                    // objects.
                    let table_dependency = list_schemas(self.db_ref())?.into_iter().any(|schema| {
                        schema
                            .owner
                            .as_deref()
                            .is_some_and(|owner| owner.eq_ignore_ascii_case(&role))
                            || schema.policies.iter().any(|policy| {
                                policy
                                    .roles
                                    .iter()
                                    .any(|name| name.eq_ignore_ascii_case(&role))
                            })
                    });
                    let owns_type = list_user_types(self.db_ref())?
                        .into_iter()
                        .any(|object| object.owner.eq_ignore_ascii_case(&role));
                    let owns_sequence = list_sequences(self.db_ref())?
                        .into_iter()
                        .any(|object| object.owner.eq_ignore_ascii_case(&role));
                    let owns_view = list_views(self.db_ref())?.into_iter().any(|object| {
                        object
                            .owner
                            .as_deref()
                            .is_some_and(|owner| owner.eq_ignore_ascii_case(&role))
                    });
                    let owns_routine = list_routines(self.db_ref())?
                        .into_iter()
                        .any(|object| object.owner().eq_ignore_ascii_case(&role));
                    let owns_namespace = list_namespaces(self.db_ref())?
                        .into_iter()
                        .any(|object| object.owner.eq_ignore_ascii_case(&role));
                    let owns_database = list_databases(self.db_ref())?
                        .into_iter()
                        .any(|object| object.owner.eq_ignore_ascii_case(&role));
                    let has_privileges = list_privileges(self.db_ref())?
                        .into_iter()
                        .any(|grant| grant.grantee.eq_ignore_ascii_case(&role));
                    let has_default_privileges = list_default_privileges(self.db_ref())?
                        .into_iter()
                        .any(|grant| {
                            grant.grantee.eq_ignore_ascii_case(&role)
                                || grant.grantor.eq_ignore_ascii_case(&role)
                        });
                    let has_granted_membership = list_role_memberships(self.db_ref())?
                        .into_iter()
                        .any(|grant| {
                            grant.grantor.eq_ignore_ascii_case(&role)
                                && !grant.role.eq_ignore_ascii_case(&role)
                                && !grant.member.eq_ignore_ascii_case(&role)
                        });
                    if table_dependency
                        || owns_type
                        || owns_sequence
                        || owns_view
                        || owns_routine
                        || owns_namespace
                        || owns_database
                        || has_privileges
                        || has_default_privileges
                        || has_granted_membership
                    {
                        return Err(SqlError::dependent_objects_still_exist(format!(
                            "role \"{role}\" cannot be dropped because some objects depend on it"
                        )));
                    }
                    roles_to_drop.push(role);
                }
                // Validate the whole DROP list before deleting its first role.
                // A later dependency failure must not partially deprovision it.
                for role in roles_to_drop {
                    for membership in list_role_memberships(self.db_ref())? {
                        if membership.role.eq_ignore_ascii_case(&role)
                            || membership.member.eq_ignore_ascii_case(&role)
                        {
                            self.capture_membership_undo(&membership.role, &membership.member)?;
                            delete_role_membership(
                                self.db_mut()?,
                                &membership.role,
                                &membership.member,
                            )?;
                        }
                    }
                    self.capture_role_undo(&role)?;
                    delete_role_record(self.db_mut()?, &role)?;
                }
                Ok(SqlResult::command("DROP ROLE"))
            }
            _ => Err(SqlError::Unsupported(format!(
                "DROP {object_type} is not supported"
            ))),
        }
    }

    pub(crate) fn drop_table_row_type_dependents(
        &mut self,
        source: &TableSchema,
        cascade: bool,
    ) -> Result<()> {
        let target_oid = source.row_type_oid();
        let target_relation_oid = table_relation_oid(source);
        let dependent_types = list_user_types(self.db_ref())?
            .into_iter()
            .filter(|user_type| user_type_kind_depends_on_oid(&user_type.kind, target_oid))
            .collect::<Vec<_>>();
        let dependent_columns = list_schemas(self.db_ref())?
            .into_iter()
            .filter(|schema| schema.row_type_oid() != target_oid)
            .flat_map(|schema| {
                schema
                    .columns
                    .iter()
                    .filter(|column| {
                        column.user_type.as_ref().is_some_and(|column_type| {
                            user_type_column_depends_on_oid(column_type, target_oid)
                        })
                    })
                    .map(|column| (schema.name.clone(), column.name.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let dependent_defaults = list_schemas(self.db_ref())?
            .into_iter()
            .filter(|schema| schema.row_type_oid() != target_oid)
            .flat_map(|schema| {
                schema
                    .columns
                    .iter()
                    .filter(|column| {
                        column.pg_type == "regclass"
                            && column.default_value == Some(SqlValue::Int(target_relation_oid))
                    })
                    .map(|column| (schema.name.clone(), column.name.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if !cascade {
            if let Some((table, column)) = dependent_defaults.first() {
                return Err(SqlError::dependent_objects_still_exist(format!(
                    "cannot drop table {} because default value for column {column} of table {table} depends on it",
                    source.name
                )));
            }
            if let Some((table, column)) = dependent_columns.first() {
                return Err(SqlError::dependent_objects_still_exist(format!(
                    "cannot drop table {} because column {column} of table {table} depends on its row type",
                    source.name
                )));
            }
            if let Some(user_type) = dependent_types.first() {
                return Err(SqlError::dependent_objects_still_exist(format!(
                    "cannot drop table {} because type {} depends on its row type",
                    source.name,
                    user_type.column_type(false).formatted_name()
                )));
            }
            return Ok(());
        }

        let mut dropped = BTreeSet::new();
        for user_type in dependent_types {
            if load_user_type(self.db_ref(), &user_type.schema_name, &user_type.name)?.is_some() {
                self.drop_user_type_definition(&user_type, true, &mut dropped)?;
            }
        }
        for (table, column) in dependent_columns {
            let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
                continue;
            };
            if schema.column(&column).is_none() {
                continue;
            }
            self.capture_table_state_undo(&table, &schema)?;
            alter_table_drop_column(self.db_mut()?, &table, &mut schema, &column, false)?;
        }
        for (table, column) in dependent_defaults {
            let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
                continue;
            };
            let Some(column_index) = schema
                .columns
                .iter()
                .position(|candidate| candidate.name == column)
            else {
                continue;
            };
            self.capture_table_state_undo(&table, &schema)?;
            let column_schema = &mut schema.columns[column_index];
            column_schema.default_value = None;
            column_schema.default_expr = None;
            save_schema(self.db_mut()?, &schema)?;
        }
        Ok(())
    }

    pub(crate) fn execute_alter_table(&mut self, alter_table: &AlterTable) -> Result<SqlResult> {
        // CR-3: ALTER TABLE reaches DISABLE ROW LEVEL SECURITY and OWNER TO,
        // so an ungated path let any user switch off a table's RLS or take it
        // over outright.
        {
            let target = relation_name(&alter_table.name)?;
            self.require_table_ownership(&target, "ALTER TABLE")?;
        }
        let mut table = relation_name(&alter_table.name)?;
        let mut schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        if schema.row_type_oid.is_none() {
            schema.row_type_oid = Some(schema.row_type_oid());
        }
        if schema.row_array_type_oid.is_none() {
            schema.row_array_type_oid = Some(schema.row_array_type_oid());
        }
        let mut schema_dirty = false;

        for operation in &alter_table.operations {
            match operation {
                AlterTableOperation::AddColumn {
                    column_def,
                    if_not_exists,
                    ..
                } => {
                    reject_postgres_system_column_name(&ident_value(&column_def.name))?;
                    if !*if_not_exists || schema.column(&ident_value(&column_def.name)).is_none() {
                        self.ensure_table_row_type_change_allowed(&schema)?;
                    }
                    self.capture_table_state_undo(&table, &schema)?;
                    if !self.alter_table_add_generated_column(
                        &table,
                        &mut schema,
                        column_def,
                        *if_not_exists,
                    )? {
                        alter_table_add_column(
                            self.db_mut()?,
                            &table,
                            &mut schema,
                            column_def,
                            *if_not_exists,
                        )?;
                    }
                }
                AlterTableOperation::AddConstraint {
                    constraint,
                    not_valid,
                } => {
                    // Only the FK shape is inspected here: the general
                    // converter rejects forms this path handles specially
                    // (PRIMARY KEY USING INDEX), and gating must not change
                    // which constraints are accepted.
                    if let TableConstraint::ForeignKey(foreign_key) = constraint {
                        self.require_reference_privilege(
                            &table,
                            &[ConstraintSchema::ForeignKey {
                                name: String::new(),
                                columns: Vec::new(),
                                foreign_table: relation_name(&foreign_key.foreign_table)?,
                                referred_columns: Vec::new(),
                                on_delete: ForeignKeyAction::NoAction,
                                on_update: ForeignKeyAction::NoAction,
                                validated: true,
                            }],
                        )?;
                    }
                    self.capture_table_state_undo(&table, &schema)?;
                    if let Some((old_index_name, new_index_name)) = alter_table_add_constraint(
                        self.db_mut()?,
                        &table,
                        &mut schema,
                        constraint,
                        *not_valid,
                    )? {
                        if !self.rename_session_index(&old_index_name, &new_index_name)? {
                            return Err(SqlError::InvalidSql(format!(
                                "index \"{old_index_name}\" does not exist"
                            )));
                        }
                    }
                    schema_dirty = true;
                }
                AlterTableOperation::DropConstraint {
                    if_exists, name, ..
                } => {
                    self.capture_table_state_undo(&table, &schema)?;
                    if let Some(index_name) = alter_table_drop_constraint(
                        &table,
                        &mut schema,
                        &ident_value(name),
                        *if_exists,
                    )? {
                        self.drop_session_index(&index_name)?;
                    }
                    schema_dirty = true;
                }
                AlterTableOperation::RenameConstraint { old_name, new_name } => {
                    self.capture_table_state_undo(&table, &schema)?;
                    if let Some((old_index_name, new_index_name)) = alter_table_rename_constraint(
                        &table,
                        &mut schema,
                        &ident_value(old_name),
                        &ident_value(new_name),
                    )? {
                        if !self.rename_session_index(&old_index_name, &new_index_name)? {
                            return Err(SqlError::InvalidSql(format!(
                                "index \"{old_index_name}\" does not exist"
                            )));
                        }
                    }
                    schema_dirty = true;
                }
                AlterTableOperation::ValidateConstraint { name } => {
                    self.capture_table_state_undo(&table, &schema)?;
                    alter_table_validate_constraint(
                        self.db_ref(),
                        &table,
                        &mut schema,
                        &ident_value(name),
                    )?;
                    schema_dirty = true;
                }
                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    for column in column_names {
                        let column_name = ident_value(column);
                        if !*if_exists || schema.column(&column_name).is_some() {
                            self.ensure_table_row_type_change_allowed(&schema)?;
                        }
                        let owned_sequence = schema
                            .column(&column_name)
                            .and_then(|column| column.default_sequence.clone());
                        self.capture_table_state_undo(&table, &schema)?;
                        alter_table_drop_column(
                            self.db_mut()?,
                            &table,
                            &mut schema,
                            &column_name,
                            *if_exists,
                        )?;
                        self.change_column_privileges(&table, &column_name, None)?;
                        if let Some(sequence_name) = owned_sequence {
                            if load_sequence(self.db_ref(), &sequence_name)?.is_some_and(
                                |sequence| {
                                    sequence.owned_by_table.as_deref() == Some(table.as_str())
                                        && sequence.owned_by_column.as_deref().is_some_and(
                                            |owner_column| {
                                                owner_column.eq_ignore_ascii_case(&column_name)
                                            },
                                        )
                                },
                            ) {
                                self.delete_session_sequence(&sequence_name)?;
                            }
                        }
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let old_column = ident_value(old_column_name);
                    let new_column = ident_value(new_column_name);
                    let previous_schema = schema.clone();
                    let attribute_index = previous_schema
                        .columns
                        .iter()
                        .filter(|column| !column.hidden)
                        .position(|column| column.name.eq_ignore_ascii_case(&old_column));
                    let owned_sequence = schema
                        .column(&old_column)
                        .and_then(|column| column.default_sequence.clone());
                    self.capture_table_state_undo(&table, &schema)?;
                    alter_table_rename_column(
                        self.db_mut()?,
                        &table,
                        &mut schema,
                        &old_column,
                        &new_column,
                    )?;
                    self.change_column_privileges(&table, &old_column, Some(&new_column))?;
                    if let Some(index) = attribute_index {
                        self.apply_table_row_type_change(
                            &previous_schema,
                            &schema,
                            &CompositeAttributeMutation::Rename {
                                index,
                                name: new_column.clone(),
                            },
                        )?;
                    }
                    if let Some(sequence_name) = owned_sequence {
                        let mut sequence = load_sequence_required(self.db_ref(), &sequence_name)?;
                        if sequence.owned_by_table.as_deref() == Some(table.as_str())
                            && sequence
                                .owned_by_column
                                .as_deref()
                                .is_some_and(|column| column.eq_ignore_ascii_case(&old_column))
                        {
                            sequence.owned_by_column = Some(new_column);
                            self.save_session_sequence(&sequence)?;
                        }
                    }
                }
                AlterTableOperation::RenameTable { table_name } => {
                    let previous_schema = schema.clone();
                    let new_table = match table_name {
                        RenameTableNameKind::To(name) => relation_name(name)?,
                        RenameTableNameKind::As(name) => relation_name(name)?,
                    };
                    if load_user_type(self.db_ref(), &schema.schema_name, &new_table)?.is_some() {
                        return Err(SqlError::data_exception(
                            "42710",
                            format!("type \"{new_table}\" already exists"),
                            Some(new_table),
                        ));
                    }
                    let in_tx = self.tx.is_some();
                    if let Some(undo) = alter_table_rename_table(
                        self.db_mut()?,
                        &table,
                        &new_table,
                        &mut schema,
                        in_tx,
                    )? {
                        self.ddl_undo.push(undo);
                    }
                    for mut grant in list_privileges(self.db_ref())? {
                        if grant.object_type == PrivilegeObjectType::Table
                            && grant.object_name == table
                        {
                            self.delete_session_privilege(&grant)?;
                            grant.object_name = new_table.clone();
                            self.save_session_privilege(&grant)?;
                        }
                    }
                    let renamed_type = table_row_type_schema(&schema)
                        .column_type(false)
                        .formatted_name();
                    self.apply_table_row_type_change(
                        &previous_schema,
                        &schema,
                        &CompositeAttributeMutation::RenameType { name: renamed_type },
                    )?;
                    table = new_table;
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    let column_name = ident_value(column_name);
                    if let AlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } = op
                    {
                        self.alter_table_set_column_type(
                            &table,
                            &mut schema,
                            &column_name,
                            data_type,
                            using.as_ref(),
                        )?;
                    } else {
                        self.capture_table_state_undo(&table, &schema)?;
                        alter_table_alter_column(
                            self.db_mut()?,
                            &table,
                            &mut schema,
                            &column_name,
                            op,
                        )?;
                    }
                }
                AlterTableOperation::EnableRowLevelSecurity => {
                    self.capture_table_state_undo(&table, &schema)?;
                    schema.rls_enabled = true;
                    schema_dirty = true;
                }
                AlterTableOperation::DisableRowLevelSecurity => {
                    // The enabled and forced flags are independent, matching
                    // pg_class.relrowsecurity / relforcerowsecurity.
                    self.capture_table_state_undo(&table, &schema)?;
                    schema.rls_enabled = false;
                    schema_dirty = true;
                }
                AlterTableOperation::ForceRowLevelSecurity => {
                    self.capture_table_state_undo(&table, &schema)?;
                    schema.rls_forced = true;
                    schema_dirty = true;
                }
                AlterTableOperation::NoForceRowLevelSecurity => {
                    self.capture_table_state_undo(&table, &schema)?;
                    schema.rls_forced = false;
                    schema_dirty = true;
                }
                AlterTableOperation::OwnerTo { new_owner } => {
                    self.capture_table_state_undo(&table, &schema)?;
                    let owner = match new_owner {
                        Owner::Ident(ident) => normalize_role_name(&ident_value(ident)),
                        Owner::CurrentRole | Owner::CurrentUser => {
                            current_user_from_gucs(&self.session_gucs)
                        }
                        Owner::SessionUser => session_user_from_gucs(&self.session_gucs),
                    };
                    ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                    // Same laundering primitive ALTER TYPE/FUNCTION already
                    // gate: you may only hand an object to a role you hold.
                    self.require_settable_new_owner(&owner, &format!("table {table}"))?;
                    schema.owner = Some(owner);
                    schema_dirty = true;
                }
                AlterTableOperation::DisableTrigger { name } => {
                    self.alter_table_set_trigger_enabled_mode(
                        &table,
                        name,
                        TriggerEnabledMode::Disabled,
                    )?;
                }
                AlterTableOperation::EnableTrigger { name } => {
                    self.alter_table_set_trigger_enabled_mode(
                        &table,
                        name,
                        TriggerEnabledMode::Origin,
                    )?;
                }
                AlterTableOperation::EnableReplicaTrigger { name } => {
                    self.alter_table_set_trigger_enabled_mode(
                        &table,
                        name,
                        TriggerEnabledMode::Replica,
                    )?;
                }
                AlterTableOperation::EnableAlwaysTrigger { name } => {
                    self.alter_table_set_trigger_enabled_mode(
                        &table,
                        name,
                        TriggerEnabledMode::Always,
                    )?;
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "ALTER TABLE operation {other} is not supported"
                    )));
                }
            }
        }

        if schema_dirty {
            save_schema(self.db_mut()?, &schema)?;
            for definition in unique_constraint_index_definitions_for_schema(&schema) {
                let exists = self
                    .db_ref()
                    .index_definitions()
                    .into_iter()
                    .any(|existing| {
                        existing.name.eq_ignore_ascii_case(&definition.name)
                            && existing
                                .collection
                                .eq_ignore_ascii_case(&definition.collection)
                            && existing.unique == definition.unique
                            && existing.kind == definition.kind
                            && index_field_lists_match(&existing.fields, &definition.fields)
                            && existing.predicate == definition.predicate
                            && existing.exclusion == definition.exclusion
                    });
                if !exists {
                    self.create_session_index(definition)?;
                }
            }
        }
        if schema.partitioning.is_some() {
            sync_partition_children_from_parent(self.db_mut()?, &schema)?;
        }
        Ok(SqlResult::command("ALTER TABLE"))
    }

    pub(crate) fn alter_table_set_column_type(
        &mut self,
        table: &str,
        schema: &mut TableSchema,
        column_name: &str,
        data_type: &DataType,
        using: Option<&Expr>,
    ) -> Result<()> {
        let column_index = schema
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| SqlError::UndefinedColumn {
                table: table.to_string(),
                column: column_name.to_string(),
            })?;
        if schema.partition_of.is_some() {
            return Err(SqlError::data_exception(
                "42P16",
                format!("cannot alter inherited column \"{column_name}\""),
                Some(column_name.to_string()),
            ));
        }
        if schema.partitioning.as_ref().is_some_and(|partitioning| {
            partitioning
                .key_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(column_name))
        }) {
            return Err(SqlError::data_exception(
                "42P16",
                format!(
                    "cannot alter column \"{column_name}\" because it is part of the partition key of relation \"{table}\""
                ),
                Some(column_name.to_string()),
            ));
        }
        if let Some(generated) = schema.columns.iter().find(|column| {
            !column.name.eq_ignore_ascii_case(column_name)
                && column
                    .generated_expr
                    .as_deref()
                    .is_some_and(|expr| sql_contains_identifier(expr, column_name))
        }) {
            return Err(SqlError::Unsupported(format!(
                "cannot alter type of a column used by a generated column: column \"{column_name}\" is used by generated column \"{}\"",
                generated.name
            )));
        }
        self.ensure_table_row_type_change_allowed(schema)?;
        self.ensure_column_type_view_dependencies_allow_change(table, column_name)?;

        let previous_column = schema.columns[column_index].clone();
        let synthetic = ColumnDef {
            name: Ident::new(previous_column.name.clone()),
            data_type: data_type.clone(),
            options: Vec::new(),
        };
        let mut updated_column = column_schema_from_def(self.db_ref(), &synthetic)?;
        self.ensure_user_type_usage(updated_column.user_type.as_ref())?;
        updated_column.primary_key = previous_column.primary_key;
        updated_column.hidden = previous_column.hidden;
        updated_column.nullable = previous_column.nullable;
        updated_column.compression = previous_column.compression;
        updated_column.default_sequence = previous_column.default_sequence.clone();
        updated_column.default_value = previous_column.default_value.clone();
        updated_column.default_expr = previous_column.default_expr.clone();
        updated_column.generated_expr = previous_column.generated_expr.clone();
        updated_column.identity = previous_column.identity.clone();
        if updated_column.collation.is_none()
            && (pg_type_is_collatable(&updated_column.pg_type)
                || updated_column
                    .user_type
                    .as_ref()
                    .is_some_and(|user_type| user_type.scalar_collation_oid() != 0))
        {
            updated_column.collation = previous_column.collation.clone();
        }

        if using.is_none() && !column_assignment_cast_allowed(&previous_column, &updated_column) {
            return Err(SqlError::data_exception(
                "42804",
                format!(
                    "column \"{column_name}\" cannot be cast automatically to type {}",
                    updated_column.formatted_pg_type()
                ),
                Some(column_name.to_string()),
            ));
        }
        self.rewrite_altered_column_default(table, &previous_column, &mut updated_column)?;

        let mut updated_parent = schema.clone();
        updated_parent.columns[column_index] = updated_column;
        self.revalidate_altered_column_indexes(&previous_column, &mut updated_parent, column_name)?;

        let mut changes = vec![(schema.clone(), updated_parent.clone())];
        if schema.partitioning.is_some() {
            let mut pending = vec![schema.name.to_ascii_lowercase()];
            let candidates = list_schemas(self.db_ref())?;
            while let Some(parent_name) = pending.pop() {
                for child in &candidates {
                    let is_child = child.partition_of.as_ref().is_some_and(|partition| {
                        partition.parent_table.eq_ignore_ascii_case(&parent_name)
                    });
                    if !is_child
                        || changes
                            .iter()
                            .any(|(previous, _)| previous.name.eq_ignore_ascii_case(&child.name))
                    {
                        continue;
                    }
                    let parent = changes
                        .iter()
                        .find(|(previous, _)| previous.name.eq_ignore_ascii_case(&parent_name))
                        .map(|(_, updated)| updated)
                        .expect("partition parent queued after its type change");
                    let mut updated = schema_with_parent_inheritance(child.clone(), parent);
                    self.revalidate_altered_column_indexes(
                        &previous_column,
                        &mut updated,
                        column_name,
                    )?;
                    pending.push(child.name.to_ascii_lowercase());
                    changes.push((child.clone(), updated));
                }
            }
        }

        let rewrite = using.is_some()
            || column_type_change_requires_rewrite(
                &previous_column,
                &updated_parent.columns[column_index],
            );
        let mut planned = Vec::with_capacity(changes.len());
        for (previous, updated) in changes {
            let mut records = self.db_ref().scan_collection(&previous.name)?;
            if rewrite {
                for record in &mut records {
                    let current = record_column_value(record, &previous, column_name);
                    let value = if let Some(expr) = using {
                        self.eval_target_record_expr(
                            &previous.name,
                            &previous.name,
                            &previous,
                            record,
                            expr,
                        )?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "unsupported ALTER COLUMN TYPE USING expression {expr}"
                            ))
                        })?
                    } else if matches!(current, SqlValue::Null) {
                        SqlValue::Null
                    } else {
                        current
                    };
                    let value = if matches!(value, SqlValue::Null) {
                        SqlValue::Null
                    } else {
                        cast_value_to_column_type(value, &updated.columns[column_index])?
                    };
                    if previous.columns[column_index].primary_key {
                        let mut fields = record_fields_for_schema(record, &previous);
                        fields.insert(column_name.to_string(), value);
                        *record = record_from_fields_with_db(
                            self.db_ref(),
                            &previous.name,
                            Some(&updated),
                            fields,
                        )?;
                    } else {
                        set_record_column(record, Some(&updated), column_name, value)?;
                    }
                }
            }
            self.validate_planned_alter_record_constraints(&updated, &records)?;
            planned.push((previous, updated, records));
        }
        self.validate_planned_alter_foreign_keys(&planned)?;

        for (previous, _, _) in &planned {
            self.capture_table_state_undo(&previous.name, previous)?;
        }
        let snapshots = planned
            .iter()
            .map(|(previous, _, _)| {
                Ok((
                    previous.name.clone(),
                    previous.clone(),
                    self.db_ref().scan_collection(&previous.name)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let apply_result = (|| -> Result<()> {
            for (previous, updated, records) in &planned {
                let updated_ids = records
                    .iter()
                    .map(|record| record.id.clone())
                    .collect::<BTreeSet<_>>();
                for record in records {
                    self.db_mut()?.insert(&previous.name, record.clone())?;
                }
                for old in self.db_ref().scan_collection(&previous.name)? {
                    if !updated_ids.contains(&old.id) {
                        self.db_mut()?.delete(&previous.name, &old.id)?;
                    }
                }
                save_schema(self.db_mut()?, updated)?;
            }
            Ok(())
        })();
        if let Err(error) = apply_result {
            for (name, previous, records) in snapshots.into_iter().rev() {
                restore_table_state(self.db_mut()?, &name, &previous, records)?;
            }
            return Err(error);
        }
        *schema = updated_parent;
        Ok(())
    }

    pub(crate) fn validate_planned_alter_record_constraints(
        &self,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        for record in records {
            validate_record_local_constraints(&schema.name, schema, record)?;
        }
        for arbiter in unique_arbiters_for_table(self.db_ref(), &schema.name, schema).iter() {
            let mut seen = BTreeSet::new();
            for record in records {
                let values = unique_key_values(record, schema, &arbiter.key);
                if unique_key_has_null(&values) {
                    continue;
                }
                let key = typed_unique_key_label(schema, &arbiter.key, &values)?;
                if !seen.insert(key) {
                    return Err(unique_violation(&arbiter.name));
                }
            }
        }
        for constraint in &schema.constraints {
            let ConstraintSchema::Exclusion {
                name,
                equal_columns,
                range,
                predicate,
                ..
            } = constraint
            else {
                continue;
            };
            let predicate = exclusion_predicate_expr(predicate)?;
            validate_exclusion_record_pairs(
                &schema.name,
                schema,
                name,
                equal_columns,
                range,
                predicate.as_ref(),
                records,
                false,
            )?;
        }
        Ok(())
    }

    pub(crate) fn rewrite_altered_column_default(
        &mut self,
        table: &str,
        previous: &ColumnSchema,
        updated: &mut ColumnSchema,
    ) -> Result<()> {
        if previous.default_expr.is_none()
            && previous.default_value.is_none()
            && previous.default_sequence.is_none()
        {
            return Ok(());
        }
        if !column_assignment_cast_allowed(previous, updated) {
            return Err(SqlError::data_exception(
                "42804",
                format!(
                    "default for column \"{}\" cannot be cast automatically to type {}",
                    previous.name,
                    updated.formatted_pg_type()
                ),
                Some(table.to_string()),
            ));
        }
        if let Some(value) = previous.default_value.clone() {
            updated.default_value = Some(cast_value_to_column_type(value, updated)?);
        }
        if previous.default_sequence.is_some()
            && !matches!(updated.pg_type.as_str(), "int2" | "int4" | "int8")
        {
            return Err(SqlError::data_exception(
                "42804",
                format!(
                    "default for column \"{}\" cannot be cast automatically to type {}",
                    previous.name,
                    updated.formatted_pg_type()
                ),
                Some(table.to_string()),
            ));
        }
        Ok(())
    }

    pub(crate) fn revalidate_altered_column_indexes(
        &self,
        previous_column: &ColumnSchema,
        schema: &mut TableSchema,
        column_name: &str,
    ) -> Result<()> {
        let schema_snapshot = schema.clone();
        for index in &mut schema.indexes {
            let expressions = if index.source_expressions.is_empty() {
                index
                    .expression
                    .split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            } else {
                index.source_expressions.clone()
            };
            for (position, expression) in expressions.iter().enumerate() {
                let Ok(expr) = Self::parse_default_expr(expression) else {
                    continue;
                };
                if !sql_contains_identifier(expression, column_name) {
                    continue;
                }
                let Some(pg_type) = projected_expr_pg_type(&expr, Some(&schema_snapshot)) else {
                    continue;
                };
                let Some(current) = index.operator_classes.get(position).cloned() else {
                    continue;
                };
                let previous_default = pg_default_opclass_for_type(
                    self.db_ref(),
                    &index.access_method,
                    &previous_column.pg_type,
                )?;
                if previous_default.is_some_and(|default| {
                    current
                        .rsplit('.')
                        .next()
                        .is_some_and(|name| name == default.name)
                }) {
                    let next = pg_default_opclass_for_type(
                        self.db_ref(),
                        &index.access_method,
                        &pg_type,
                    )?
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "data type {pg_type} has no default operator class for access method \"{}\"",
                            index.access_method
                        ))
                    })?;
                    index.operator_classes[position] = next.name.to_string();
                } else if pg_opclass_named_for_type(
                    self.db_ref(),
                    &index.access_method,
                    &pg_type,
                    &current,
                )?
                .is_none()
                {
                    return Err(SqlError::data_exception(
                        "42804",
                        format!("operator class \"{current}\" does not accept data type {pg_type}"),
                        Some(pg_type),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_planned_alter_foreign_keys(
        &self,
        planned: &[(TableSchema, TableSchema, Vec<Record>)],
    ) -> Result<()> {
        let mut schemas = list_schemas(self.db_ref())?;
        for (_, updated, _) in planned {
            if let Some(schema) = schemas
                .iter_mut()
                .find(|schema| schema.name.eq_ignore_ascii_case(&updated.name))
            {
                *schema = updated.clone();
            }
        }
        let changed = planned
            .iter()
            .map(|(_, updated, _)| updated.name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();

        for local_schema in &schemas {
            for constraint in &local_schema.constraints {
                let ConstraintSchema::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } = constraint
                else {
                    continue;
                };
                if !changed.contains(&local_schema.name.to_ascii_lowercase())
                    && !changed.contains(&foreign_table.to_ascii_lowercase())
                {
                    continue;
                }
                let foreign_schema = schemas
                    .iter()
                    .find(|schema| schema.name.eq_ignore_ascii_case(foreign_table))
                    .ok_or_else(|| foreign_key_violation(&local_schema.name, name))?;
                for (local, referred) in columns.iter().zip(referred_columns) {
                    let local_column = local_schema.column(local).ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "foreign key constraint \"{name}\" references a missing column"
                        ))
                    })?;
                    let foreign_column = foreign_schema.column(referred).ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "foreign key constraint \"{name}\" references a missing column"
                        ))
                    })?;
                    validate_foreign_key_type_pair(
                        name,
                        local,
                        local_column,
                        referred,
                        foreign_column,
                    )?;
                }

                let local_records = planned
                    .iter()
                    .find(|(_, updated, _)| updated.name.eq_ignore_ascii_case(&local_schema.name))
                    .map(|(_, _, records)| records.clone())
                    .unwrap_or(self.db_ref().scan_collection(&local_schema.name)?);
                let foreign_records = planned
                    .iter()
                    .find(|(_, updated, _)| updated.name.eq_ignore_ascii_case(foreign_table))
                    .map(|(_, _, records)| records.clone())
                    .unwrap_or(self.db_ref().scan_collection(foreign_table)?);
                for record in local_records {
                    let key = record_column_values(&record, local_schema, columns);
                    if key.iter().all(|value| matches!(value, SqlValue::Null)) {
                        continue;
                    }
                    let mut found = false;
                    for foreign_record in &foreign_records {
                        let foreign_key =
                            record_column_values(foreign_record, foreign_schema, referred_columns);
                        if typed_column_values_not_distinct(
                            foreign_schema,
                            referred_columns,
                            &foreign_key,
                            &key,
                        )? {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Err(foreign_key_violation(&local_schema.name, name));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn ensure_column_type_view_dependencies_allow_change(
        &self,
        table: &str,
        column_name: &str,
    ) -> Result<()> {
        for view in list_views(self.db_ref())? {
            if sql_contains_identifier(&view.query_sql, table)
                && sql_contains_identifier(&view.query_sql, column_name)
            {
                return Err(SqlError::Unsupported(format!(
                    "cannot alter type of a column used by view or rule: view {} depends on column \"{column_name}\"",
                    view.name
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn alter_table_add_generated_column(
        &mut self,
        table: &str,
        schema: &mut TableSchema,
        column_def: &ColumnDef,
        if_not_exists: bool,
    ) -> Result<bool> {
        let serial_type = serial_type(&column_def.data_type);
        let identity = identity_options(column_def)?;
        if serial_type.is_none() && identity.is_none() {
            return Ok(false);
        }

        let mut column = column_schema_from_def(self.db_ref(), column_def)?;
        self.ensure_user_type_usage(column.user_type.as_ref())?;
        if schema.column(&column.name).is_some() {
            if if_not_exists {
                return Ok(true);
            }
            return Err(SqlError::InvalidSql(format!(
                "column \"{}\" of relation \"{table}\" already exists",
                column.name
            )));
        }
        if identity.is_some() && !matches!(column.pg_type.as_str(), "int2" | "int4" | "int8") {
            return Err(SqlError::invalid_parameter_value(
                "identity column type must be smallint, integer, or bigint",
            ));
        }

        let sequence_name = sequence_name_for_column(table, &column.name);
        let mut sequence = if let Some(serial_type) = serial_type {
            SequenceSchema::new_typed(sequence_name.clone(), serial_type, 1)
        } else {
            let (_, options) = identity.clone().expect("generated column kind checked");
            sequence_from_options(sequence_name.clone(), &column.pg_type, options)?
        };
        sequence.owned_by_table = Some(table.to_string());
        sequence.owned_by_column = Some(column.name.clone());

        column.nullable = false;
        column.default_sequence = Some(sequence_name.clone());
        column.default_value = None;
        column.default_expr = None;
        column.identity = identity.map(|(kind, _)| kind);

        self.create_session_sequence_if_missing(sequence, false)?;
        let mut records = self.db_ref().scan_collection(table)?;
        let values = self.nextvals(&sequence_name, records.len())?;
        let mut updated_schema = schema.clone();
        updated_schema.add_column(column.clone());
        for (record, value) in records.iter_mut().zip(values) {
            set_record_column(
                record,
                Some(&updated_schema),
                &column.name,
                SqlValue::Int(value),
            )?;
            self.db_mut()?.insert(table, record.clone())?;
        }
        save_schema(self.db_mut()?, &updated_schema)?;
        *schema = updated_schema;
        Ok(true)
    }

    pub(crate) fn alter_table_set_trigger_enabled_mode(
        &mut self,
        table: &str,
        name: &Ident,
        mode: TriggerEnabledMode,
    ) -> Result<()> {
        let trigger_name = ident_value(name);
        if matches!(
            mode,
            TriggerEnabledMode::Origin | TriggerEnabledMode::Disabled
        ) && (trigger_name.eq_ignore_ascii_case("all")
            || trigger_name.eq_ignore_ascii_case("user"))
        {
            for mut trigger in list_triggers(self.db_ref())?
                .into_iter()
                .filter(|trigger| trigger.table_name.eq_ignore_ascii_case(table))
            {
                trigger.set_enabled_mode(mode);
                self.save_session_trigger(&trigger)?;
            }
            return Ok(());
        }

        let mut trigger = load_trigger(self.db_ref(), table, &trigger_name)?.ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "trigger \"{trigger_name}\" for relation \"{table}\" does not exist"
            ))
        })?;
        trigger.set_enabled_mode(mode);
        self.save_session_trigger(&trigger)
    }

    pub(crate) fn execute_create_policy(
        &mut self,
        create_policy: &CreatePolicy,
    ) -> Result<SqlResult> {
        let table = normalize_object_name(&object_name(&create_policy.table_name)?);
        // CR-4: an ungated CREATE POLICY let any user attach a permissive
        // `USING (true)` policy to someone else's RLS table.
        self.require_table_ownership(&table, "CREATE POLICY")?;
        let mut schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        let name = normalize_object_name(&ident_value(&create_policy.name));
        if schema
            .policies
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&name))
        {
            return Err(SqlError::InvalidSql(format!(
                "policy \"{name}\" for table \"{table}\" already exists"
            )));
        }
        let command = match create_policy.command {
            None | Some(CreatePolicyCommand::All) => PolicyCommand::All,
            Some(CreatePolicyCommand::Select) => PolicyCommand::Select,
            Some(CreatePolicyCommand::Insert) => PolicyCommand::Insert,
            Some(CreatePolicyCommand::Update) => PolicyCommand::Update,
            Some(CreatePolicyCommand::Delete) => PolicyCommand::Delete,
        };
        validate_policy_clauses(
            command,
            create_policy.using.is_some(),
            create_policy.with_check.is_some(),
        )?;
        let using_expr = create_policy
            .using
            .as_ref()
            .map(|expr| self.validated_policy_expression(&schema, expr))
            .transpose()?;
        let check_expr = create_policy
            .with_check
            .as_ref()
            .map(|expr| self.validated_policy_expression(&schema, expr))
            .transpose()?;
        let roles = self.policy_roles_from_owners(create_policy.to.as_deref())?;
        schema.policies.push(PolicySchema {
            name,
            command,
            using_expr,
            check_expr,
            permissive: !matches!(
                create_policy.policy_type,
                Some(CreatePolicyType::Restrictive)
            ),
            enforced: true,
            roles,
        });
        self.save_session_schema(&schema)?;
        Ok(SqlResult::command("CREATE POLICY"))
    }

    pub(crate) fn execute_alter_policy(&mut self, alter_policy: &AlterPolicy) -> Result<SqlResult> {
        let table = normalize_object_name(&object_name(&alter_policy.table_name)?);
        self.require_table_ownership(&table, "ALTER POLICY")?;
        let mut schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        let name = normalize_object_name(&ident_value(&alter_policy.name));
        let position = schema
            .policies
            .iter()
            .position(|policy| policy.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "policy \"{name}\" for table \"{table}\" does not exist"
                ))
            })?;
        match &alter_policy.operation {
            AlterPolicyOperation::Rename { new_name } => {
                let new_name = normalize_object_name(&ident_value(new_name));
                if schema
                    .policies
                    .iter()
                    .any(|policy| policy.name.eq_ignore_ascii_case(&new_name))
                {
                    return Err(SqlError::InvalidSql(format!(
                        "policy \"{new_name}\" for table \"{table}\" already exists"
                    )));
                }
                schema.policies[position].name = new_name;
            }
            AlterPolicyOperation::Apply {
                to,
                using,
                with_check,
            } => {
                let command = schema.policies[position].command;
                validate_policy_clauses(
                    command,
                    using.is_some() || schema.policies[position].using_expr.is_some(),
                    with_check.is_some() || schema.policies[position].check_expr.is_some(),
                )?;
                if let Some(using) = using {
                    schema.policies[position].using_expr =
                        Some(self.validated_policy_expression(&schema, using)?);
                }
                if let Some(with_check) = with_check {
                    schema.policies[position].check_expr =
                        Some(self.validated_policy_expression(&schema, with_check)?);
                }
                if to.is_some() {
                    schema.policies[position].roles =
                        self.policy_roles_from_owners(to.as_deref())?;
                }
            }
        }
        self.save_session_schema(&schema)?;
        Ok(SqlResult::command("ALTER POLICY"))
    }

    pub(crate) fn execute_drop_policy(&mut self, drop_policy: &DropPolicy) -> Result<SqlResult> {
        let table = normalize_object_name(&object_name(&drop_policy.table_name)?);
        // Dropping the isolating policy is as powerful as adding a permissive
        // one, so it needs the same gate.
        self.require_table_ownership(&table, "DROP POLICY")?;
        let mut schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        let name = normalize_object_name(&ident_value(&drop_policy.name));
        // Policies have no dependents, so CASCADE and RESTRICT are both no-ops.
        let original_len = schema.policies.len();
        schema
            .policies
            .retain(|policy| !policy.name.eq_ignore_ascii_case(&name));
        if schema.policies.len() == original_len && !drop_policy.if_exists {
            return Err(SqlError::InvalidSql(format!(
                "policy \"{name}\" for table \"{table}\" does not exist"
            )));
        }
        self.save_session_schema(&schema)?;
        Ok(SqlResult::command("DROP POLICY"))
    }

    pub(crate) fn validated_policy_expression(
        &self,
        schema: &TableSchema,
        expr: &Expr,
    ) -> Result<String> {
        if expr_contains_aggregate(expr) {
            return Err(SqlError::InvalidSql(
                "aggregate functions are not allowed in policy expressions".to_string(),
            ));
        }
        validate_policy_expression_columns(schema, expr)?;
        let text = format!("({expr})");
        // Guarantee the stored text round-trips through the policy-expression
        // parser used at enforcement time.
        parse_policy_expression(&text)?;
        Ok(text)
    }

    pub(crate) fn policy_roles_from_owners(&self, to: Option<&[Owner]>) -> Result<Vec<String>> {
        let Some(owners) = to else {
            return Ok(default_policy_roles());
        };
        let known_roles = list_roles(self.db_ref())?;
        let mut roles = Vec::new();
        for owner in owners {
            let role = match owner {
                Owner::Ident(ident) => normalize_role_name(&ident_value(ident)),
                Owner::CurrentRole | Owner::CurrentUser => {
                    current_user_from_gucs(&self.session_gucs)
                }
                Owner::SessionUser => session_user_from_gucs(&self.session_gucs),
            };
            if role != "public" {
                ensure_known_role(&known_roles, &role)?;
            }
            if !roles.contains(&role) {
                roles.push(role);
            }
        }
        if roles.is_empty() {
            return Ok(default_policy_roles());
        }
        Ok(roles)
    }

    /// The recognized `DO $carrier_*$` blocks are provisioning shortcuts that
    /// write the privilege store directly, so none of the statement-level
    /// gates apply to them. They are only ever issued by role-sql
    /// provisioning, which runs as a superuser.
    fn require_superuser_for_provisioning_block(&self, block: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be superuser to run the {block} provisioning block"
        ))))
    }

    pub(crate) fn execute_raw_do_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let normalized = normalize_sql(sql);
        if normalized.starts_with("do $carrier_revoke_relations$")
            && normalized.contains("from pg_catalog.pg_class as class")
            && normalized.contains("revoke all on table %s from public, carrier_app")
        {
            // This block deletes EVERY table grant to public/carrier_app in
            // one shot, straight through the privilege store rather than via
            // REVOKE, so the grantor gate never sees it. Provisioning-only:
            // require the authority that provisioning runs with.
            self.require_superuser_for_provisioning_block("carrier_revoke_relations")?;
            let revocations = list_privileges(self.db_ref())?
                .into_iter()
                .filter(|privilege| {
                    privilege.object_type == PrivilegeObjectType::Table
                        && (privilege.grantee.eq_ignore_ascii_case("public")
                            || privilege.grantee.eq_ignore_ascii_case("carrier_app"))
                })
                .collect::<Vec<_>>();
            for privilege in revocations {
                self.delete_session_privilege(&privilege)?;
            }
            return Ok(Some(SqlResult::command("DO")));
        }
        if normalized.starts_with("do $carrier_runtime_role$")
            && normalized.contains("target_role constant text :=")
            && normalized.contains("execute format('create role %i ")
        {
            // Carrier's runtime-role block loops over CREATE/ALTER ROLE with
            // PL/pgSQL retry handlers for PostgreSQL's cluster-global catalog
            // race. BicDB's role store has no such race, so the block reduces
            // to one CREATE or ALTER ROLE with the flags it names.
            self.require_superuser_for_provisioning_block("carrier_runtime_role")?;
            let block = parse_raw_do_carrier_runtime_role(sql)?;
            let exists = list_roles(self.db_ref())?
                .iter()
                .any(|role| role.name.eq_ignore_ascii_case(&block.role));
            let verb = if exists { "ALTER" } else { "CREATE" };
            self.execute(&format!(
                "{verb} ROLE \"{}\" {}",
                block.role.replace('"', "\"\""),
                block.options
            ))?;
            return Ok(Some(SqlResult::command("DO")));
        }
        if normalized.starts_with("do $carrier_context_key$")
            && normalized.contains("insert into carrier_private.context_signing_keys")
            && normalized.contains("current_setting('carrier.context_signing_key', true)")
        {
            let supplied_key = self
                .session_gucs
                .get("carrier.context_signing_key")
                .filter(|key| key.len() >= 32)
                .ok_or_else(|| {
                    SqlError::BicDb(BicDbError::Authorization(
                        "set transaction-local carrier.context_signing_key to at least 32 random bytes before applying role-sql"
                            .to_string(),
                    ))
                })?
                .clone();
            self.execute(&format!(
                "INSERT INTO carrier_private.context_signing_keys \
                 (key_id, secret, installed_at) VALUES (1, {}, clock_timestamp()) \
                 ON CONFLICT (key_id) DO UPDATE SET secret = EXCLUDED.secret, \
                 installed_at = EXCLUDED.installed_at",
                pg_quote_literal(&supplied_key)
            ))?;
            return Ok(Some(SqlResult::command("DO")));
        }
        if normalized.starts_with("do $carrier_context_crypto$")
            && normalized.contains("public.hmac(bytea, bytea, text)")
            && normalized.contains("public.digest(bytea, text)")
        {
            // hmac/digest are extension-provided BicDB builtins, not catalog
            // routines, and execute with no relation authority of their own.
            return Ok(Some(SqlResult::command("DO")));
        }
        if let Some(block) = parse_raw_do_carrier_function_dependency_grants(sql)? {
            self.require_superuser_for_provisioning_block("carrier_function_dependency_grants")?;
            let previous = list_privileges(self.db_ref())?;
            let result = apply_carrier_function_dependency_grants(self.db_mut()?, &block.role);
            if self.tx.is_some() {
                for grant in list_privileges(self.db_ref())? {
                    if !previous.contains(&grant) {
                        self.ddl_undo.push(DdlUndo::DeletePrivilege { grant });
                    }
                }
            }
            result?;
            return Ok(Some(SqlResult::command("DO")));
        }
        if let Some(block) = parse_raw_do_relation_compatibility(sql)? {
            for (legacy, canonical) in block.relation_pairs {
                let legacy_exists = load_schema(self.db_ref(), &legacy)?.is_some()
                    || load_view(self.db_ref(), &legacy)?.is_some();
                let canonical_exists = load_schema(self.db_ref(), &canonical)?.is_some()
                    || load_view(self.db_ref(), &canonical)?.is_some();
                if !canonical_exists {
                    if !legacy_exists {
                        return Err(SqlError::InvalidSql(format!(
                            "cannot establish canonical relation {canonical}: legacy relation {legacy} is also missing"
                        )));
                    }
                    self.execute(&format!(
                        "ALTER TABLE public.{legacy} RENAME TO {canonical}"
                    ))?;
                }
                if load_schema(self.db_ref(), &legacy)?.is_none()
                    && load_view(self.db_ref(), &legacy)?.is_none()
                {
                    self.execute(&format!(
                        "CREATE VIEW public.{legacy} WITH (security_invoker = true) AS SELECT * FROM public.{canonical}"
                    ))?;
                }
            }
            return Ok(Some(SqlResult::command("DO")));
        }
        if let Some(block) = parse_raw_do_compatibility_view_grants(sql)? {
            if list_roles(self.db_ref())?
                .iter()
                .any(|role| role.name.eq_ignore_ascii_case(&block.role))
            {
                for view in block.views {
                    self.execute(&format!(
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON public.{view} TO {}",
                        block.role
                    ))?;
                }
            }
            return Ok(Some(SqlResult::command("DO")));
        }
        match parse_raw_do_if_exists_ddl(sql) {
            Ok(Some(block)) => {
                let mut should_execute = true;
                for guard in &block.guards {
                    let rows = self.sql_engine().execute(&guard.exists_sql)?.rows;
                    let exists = !rows.is_empty();
                    should_execute &= if guard.negated { !exists } else { exists };
                }
                if should_execute {
                    self.execute(&block.ddl)?;
                }
                return Ok(Some(SqlResult::command("DO")));
            }
            Ok(None) => return Ok(None),
            // Not the IF-EXISTS shape: run the body as a real anonymous
            // PLpgSQL block through the routine interpreter — the same
            // machinery stored procedures and triggers execute with, so
            // DECLARE, FOR ... IN query LOOP, IF, and EXECUTE all behave.
            Err(_) => {}
        }
        let Some(body) = raw_do_block_body(sql)? else {
            return Ok(None);
        };
        let (mut declarations, mut statements, mut exception_handlers) = parse_plpgsql_body(&body)?;
        let symbols = routine_symbol_names(&[], &declarations, &statements, &exception_handlers);
        bind_routine_expressions(
            &symbols,
            &mut declarations,
            &mut statements,
            &mut exception_handlers,
        );
        let owns_transaction = self.tx.is_none()
            && (routine_declarations_may_write(&declarations)
                || routine_block_may_write(&statements)
                || routine_handlers_may_write(&exception_handlers));
        if owns_transaction {
            self.execute("BEGIN")?;
        }
        // Anonymous blocks own temporary AST nodes. Do not cache them under a
        // surrounding stored routine's stable IR identity.
        let previous_ir = self.current_routine_ir.take();
        let previous_owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = (|| {
            let mut frame = RoutineFrame::new_with_symbols(&[], &[], &symbols)?;
            self.initialize_routine_frame(&mut frame, &declarations)?;
            self.execute_routine_block(&mut frame, &statements, &exception_handlers)?;
            if owns_transaction {
                self.fire_deferred_row_triggers()?;
            }
            Ok(())
        })();
        self.current_routine_ir = previous_ir;
        self.ir_owned_statement = previous_owned;
        match result {
            Ok(()) => {
                if owns_transaction && !self.defer_commit {
                    self.commit()?;
                }
            }
            Err(error) => {
                if owns_transaction {
                    self.execute("ROLLBACK")?;
                }
                return Err(error);
            }
        }
        Ok(Some(SqlResult::command("DO")))
    }

    pub(crate) fn execute_create_index(&mut self, create_index: &CreateIndex) -> Result<SqlResult> {
        if create_index.concurrently {
            // Refused rather than silently stripped: BicDB's index build
            // holds the database's exclusive write lock for its whole
            // duration, which is the OPPOSITE of what CONCURRENTLY promises.
            // A client that asked for a non-blocking build must not get a
            // blocking one without knowing. Design for the real thing:
            // docs/create-index-concurrently-design.md.
            return Err(SqlError::Unsupported(
                "CREATE INDEX CONCURRENTLY is not supported yet: BicDB index builds \
                 currently serialize writes for the build's duration. Run CREATE INDEX \
                 (blocking) in a maintenance window, or track \
                 docs/create-index-concurrently-design.md for the non-blocking design."
                    .to_string(),
            ));
        }
        let table = relation_name(&create_index.table_name)?;
        // Building an index changes the storage and write cost of someone
        // else's relation, so it takes that relation's authority.
        self.require_table_ownership(&table, "create an index on it")?;
        let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
            if let Some(mut view) = load_view(self.db_ref(), &table)? {
                if !view.materialized {
                    return Err(SqlError::Unsupported(
                        "indexes on logical views are not supported".to_string(),
                    ));
                }
                let index_name = create_index
                    .name
                    .as_ref()
                    .map(relation_name)
                    .transpose()?
                    .unwrap_or_else(|| format!("idx_{}_{}", table, view.indexes.len() + 1));
                if view.indexes.iter().any(|index| index.name == index_name) {
                    if create_index.if_not_exists {
                        return Ok(SqlResult::command("CREATE INDEX"));
                    }
                    return Err(SqlError::InvalidSql(format!(
                        "index `{index_name}` already exists"
                    )));
                }
                view.indexes.push(IndexSchema {
                    name: index_name,
                    expression: create_index
                        .columns
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    source_expressions: Vec::new(),
                    operator_classes: create_index
                        .columns
                        .iter()
                        .filter_map(|column| {
                            column.operator_class.as_ref().map(ToString::to_string)
                        })
                        .collect(),
                    internal_index_names: Vec::new(),
                    collations: create_index
                        .columns
                        .iter()
                        .map(|column| index_expr_collation_oid(&view.columns, &column.column.expr))
                        .collect::<Result<Vec<_>>>()?,
                    unique: create_index.unique,
                    access_method: index_access_method_name(create_index.using.as_ref())
                        .to_string(),
                    metadata_only: true,
                });
                save_view(self.db_mut()?, &view)?;
                return Ok(SqlResult::command("CREATE INDEX"));
            }
            return Err(SqlError::InvalidCollection(table.clone()));
        };
        let index_name = create_index
            .name
            .as_ref()
            .map(relation_name)
            .transpose()?
            .unwrap_or_else(|| format!("idx_{}_{}", table, schema.indexes.len() + 1));
        let expression = create_index
            .columns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let collations = create_index
            .columns
            .iter()
            .map(|column| index_expr_collation_oid(&schema.columns, &column.column.expr))
            .collect::<Result<Vec<_>>>()?;
        if schema.indexes.iter().any(|index| index.name == index_name) {
            if create_index.if_not_exists {
                return Ok(SqlResult::command("CREATE INDEX"));
            }
            return Err(SqlError::InvalidSql(format!(
                "index `{index_name}` already exists"
            )));
        }
        let access_method = index_access_method_name(create_index.using.as_ref()).to_string();
        if access_method_oid(&access_method) == 0 {
            return Err(SqlError::undefined_object(format!(
                "access method \"{access_method}\" does not exist"
            )));
        }
        if create_index.unique && access_method == "hash" {
            return Err(SqlError::Unsupported(
                "access method \"hash\" does not support unique indexes".to_string(),
            ));
        }
        let geometric_types = create_index
            .columns
            .iter()
            .map(|column| projected_expr_pg_type(&column.column.expr, Some(&schema)))
            .collect::<Vec<_>>();
        if geometric_types
            .iter()
            .flatten()
            .any(|pg_type| is_geometric_type(Some(pg_type)))
        {
            if create_index.unique && matches!(access_method.as_str(), "gist" | "spgist" | "brin") {
                return Err(SqlError::Unsupported(format!(
                    "access method \"{access_method}\" does not support unique indexes"
                )));
            }
            if geometric_types.iter().any(|pg_type| {
                pg_type
                    .as_deref()
                    .is_none_or(|pg_type| !is_geometric_type(Some(pg_type)))
            }) {
                return Err(SqlError::Unsupported(
                    "mixed planar geometric and non-geometric index columns are not supported"
                        .to_string(),
                ));
            }
            for (column, pg_type) in create_index.columns.iter().zip(&geometric_types) {
                let pg_type = pg_type.as_deref().unwrap_or("unknown");
                let default_opclass = geometric_index_default_opclass(&access_method, pg_type)
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "data type {pg_type} has no default operator class for access method \"{access_method}\""
                        ))
                    })?;
                if let Some(operator_class) = column.operator_class.as_ref() {
                    let operator_class = operator_class.to_string();
                    if !geometric_index_opclass_supported(&access_method, pg_type, &operator_class)
                    {
                        return Err(SqlError::data_exception(
                            "42804",
                            format!(
                                "operator class \"{operator_class}\" does not accept data type {pg_type}"
                            ),
                            Some(pg_type.to_string()),
                        ));
                    }
                } else if default_opclass.is_empty() {
                    unreachable!("validated geometric index has a default operator class");
                }
            }
        }
        let trigram = create_index.columns.iter().any(|column| {
            column
                .operator_class
                .as_ref()
                .is_some_and(|class| class.to_string().rsplit('.').next() == Some("gin_trgm_ops"))
        });
        if trigram
            && (access_method != "gin" || create_index.unique || create_index.columns.len() != 1)
        {
            return Err(SqlError::Unsupported(
                "gin_trgm_ops requires one non-unique GIN text expression".into(),
            ));
        }
        let mut selected_operator_classes = Vec::with_capacity(create_index.columns.len());
        for column in &create_index.columns {
            let Some(pg_type) = projected_expr_pg_type(&column.column.expr, Some(&schema)) else {
                continue;
            };
            if let Some(operator_class) = column.operator_class.as_ref() {
                let operator_class = operator_class.to_string();
                if pg_opclass_named_for_type(
                    self.db_ref(),
                    &access_method,
                    &pg_type,
                    &operator_class,
                )?
                .is_none()
                {
                    let bare_name = operator_class.rsplit('.').next().unwrap_or(&operator_class);
                    let exists = pg_opclass_specs().iter().any(|spec| {
                        spec.method_oid == access_method_oid(&access_method)
                            && spec.name == bare_name
                    });
                    if exists {
                        return Err(SqlError::data_exception(
                            "42804",
                            format!(
                                "operator class \"{operator_class}\" does not accept data type {pg_type}"
                            ),
                            Some(pg_type),
                        ));
                    }
                    return Err(SqlError::undefined_object(format!(
                        "operator class \"{operator_class}\" does not exist for access method \"{access_method}\""
                    )));
                }
                selected_operator_classes.push(operator_class);
            } else {
                let Some(default) =
                    pg_default_opclass_for_type(self.db_ref(), &access_method, &pg_type)?
                else {
                    return Err(SqlError::undefined_object(format!(
                        "data type {pg_type} has no default operator class for access method \"{access_method}\""
                    )));
                };
                selected_operator_classes.push(default.name.to_string());
            }
        }
        if let Some(pg_type) = create_index.columns.iter().find_map(|column| {
            type_without_comparison_operators(
                projected_expr_pg_type(&column.column.expr, Some(&schema)).as_deref(),
            )
        }) {
            return Err(SqlError::undefined_object(format!(
                "data type {pg_type} has no default operator class for access method \"{access_method}\""
            )));
        }
        if access_method == "spgist" {
            if create_index.columns.len() > 1 {
                return Err(SqlError::Unsupported(
                    "access method \"spgist\" does not support multicolumn indexes".to_string(),
                ));
            }
            if let Some(pg_type) = create_index
                .columns
                .first()
                .and_then(|column| projected_expr_pg_type(&column.column.expr, Some(&schema)))
                .filter(|pg_type| is_builtin_multirange_type(pg_type))
            {
                return Err(SqlError::undefined_object(format!(
                    "data type {pg_type} has no default operator class for access method \"spgist\""
                )));
            }
        }
        if is_executable_geometric_index(create_index, &schema) {
            let mut records = match self.tx.as_ref() {
                Some(tx) => tx.scan_collection(&table).map_err(SqlError::from)?,
                None => self
                    .db_ref()
                    .scan_collection(&table)
                    .map_err(SqlError::from)?,
            };
            let mut source_expressions = Vec::with_capacity(create_index.columns.len());
            let mut operator_classes = Vec::with_capacity(create_index.columns.len());
            let mut internal_index_names = Vec::with_capacity(create_index.columns.len());
            let mut projections = Vec::with_capacity(create_index.columns.len());
            for (position, column) in create_index.columns.iter().enumerate() {
                let expression_ast = column.column.expr.clone();
                let pg_type = projected_expr_pg_type(&expression_ast, Some(&schema))
                    .expect("executable geometric index has a known type");
                let projection = geometric_projection_field(&index_name, position);
                self.materialize_geometric_projection(
                    &table,
                    &schema,
                    &projection,
                    &expression_ast,
                    &pg_type,
                    &mut records,
                )?;
                source_expressions.push(expression_ast.to_string());
                operator_classes.push(
                    column
                        .operator_class
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| {
                            geometric_index_default_opclass(&access_method, &pg_type)
                                .expect("validated geometric index opclass")
                                .to_string()
                        }),
                );
                internal_index_names.push(geometric_internal_index_name(&index_name, position));
                projections.push(projection);
            }
            if !records.is_empty() {
                self.insert_session_records(&table, records)?;
            }
            for (internal_name, projection) in internal_index_names.iter().zip(projections) {
                self.create_session_index(IndexDefinition {
                    name: internal_name.clone(),
                    collection: table.clone(),
                    fields: vec![IndexField::MetadataPath(vec![projection])],
                    unique: false,
                    kind: IndexKind::Spatial,
                    predicate: None,
                    exclusion: None,
                })?;
            }
            schema.indexes.push(IndexSchema {
                name: index_name,
                expression,
                source_expressions,
                operator_classes,
                internal_index_names,
                collations,
                unique: false,
                access_method,
                metadata_only: false,
            });
            self.save_session_schema(&schema)?;
            return Ok(SqlResult::command("CREATE INDEX"));
        }
        if is_executable_jsonb_index(create_index, &schema) {
            let expression_ast = create_index.columns[0].column.expr.clone();
            let mut records = match self.tx.as_ref() {
                Some(tx) => tx.scan_collection(&table).map_err(SqlError::from)?,
                None => self
                    .db_ref()
                    .scan_collection(&table)
                    .map_err(SqlError::from)?,
            };
            let projection = jsonb_projection_field(&index_name);
            self.materialize_inverted_projection(
                &table,
                &schema,
                &projection,
                &expression_ast,
                IndexKind::Jsonb,
                &mut records,
            )?;
            if !records.is_empty() {
                self.insert_session_records(&table, records)?;
            }
            self.create_session_index(IndexDefinition {
                name: index_name.clone(),
                collection: table.clone(),
                fields: vec![IndexField::MetadataPath(vec![projection])],
                unique: false,
                kind: IndexKind::Jsonb,
                predicate: None,
                exclusion: None,
            })?;
            schema.indexes.push(IndexSchema {
                name: index_name,
                expression,
                source_expressions: Vec::new(),
                operator_classes: selected_operator_classes.clone(),
                internal_index_names: Vec::new(),
                collations,
                unique: false,
                access_method,
                metadata_only: false,
            });
            self.save_session_schema(&schema)?;
            return Ok(SqlResult::command("CREATE INDEX"));
        }
        if trigram || is_executable_array_index(create_index, &schema) {
            let expression_ast = create_index.columns[0].column.expr.clone();
            let mut records = match self.tx.as_ref() {
                Some(tx) => tx.scan_collection(&table).map_err(SqlError::from)?,
                None => self
                    .db_ref()
                    .scan_collection(&table)
                    .map_err(SqlError::from)?,
            };
            let projection = if trigram {
                format!("{TRIGRAM_PROJECTION_PREFIX}{index_name}")
            } else {
                array_projection_field(&index_name)
            };
            self.materialize_inverted_projection(
                &table,
                &schema,
                &projection,
                &expression_ast,
                IndexKind::Array,
                &mut records,
            )?;
            if !records.is_empty() {
                self.insert_session_records(&table, records)?;
            }
            self.create_session_index(IndexDefinition {
                name: index_name.clone(),
                collection: table.clone(),
                fields: vec![IndexField::MetadataPath(vec![projection])],
                unique: false,
                kind: IndexKind::Array,
                predicate: None,
                exclusion: None,
            })?;
            schema.indexes.push(IndexSchema {
                name: index_name,
                expression,
                source_expressions: if trigram {
                    vec![expression_ast.to_string()]
                } else {
                    Vec::new()
                },
                operator_classes: selected_operator_classes.clone(),
                internal_index_names: Vec::new(),
                collations,
                unique: false,
                access_method,
                metadata_only: false,
            });
            self.save_session_schema(&schema)?;
            return Ok(SqlResult::command("CREATE INDEX"));
        }
        if is_executable_fts_index(create_index, &schema) {
            let expression_ast = create_index.columns[0].column.expr.clone();
            // Paged mode outside a transaction: write one compact doc-terms
            // blob per row and a pair of bounded sorted posting runs. The
            // checkpoint advances after every batch, so an interrupted CREATE
            // resumes strictly after the last durable pk. Posting-run sorting
            // and compression are engine-owned and parallel; SQL only
            // evaluates the declared expression for each bounded row batch.
            if self.tx.is_none() && self.db_ref().is_server_paged() {
                let signature = expression_ast.to_string();
                let progress = self
                    .db_ref()
                    .prepare_full_text_build(&index_name, &table, &signature)
                    .map_err(SqlError::from)?;
                if progress.needs_tokenization {
                    let batch_rows = (progress.memory_bytes / (64 * 1024)).clamp(64, 8_192);
                    let blob_budget = (progress.memory_bytes / 2).max(64 * 1024);
                    let mut sql_error = None;
                    let streamed = self
                        .db_ref()
                        .for_each_record_batch_after_cancellable(
                            &table,
                            progress.resume_after.as_deref(),
                            batch_rows,
                            &self.cancellation,
                            |batch| {
                                let tokenized = match self.parallel_full_text_doc_blobs(
                                    &table,
                                    &schema,
                                    &expression_ast,
                                    &batch,
                                    progress.workers,
                                ) {
                                    Ok(blobs) => blobs,
                                    Err(error) => {
                                        sql_error = Some(error);
                                        return Ok(false);
                                    }
                                };
                                let mut blobs = Vec::new();
                                let mut blob_bytes = 0usize;
                                for (pk, blob) in tokenized {
                                    blob_bytes = blob_bytes
                                        .saturating_add(pk.len())
                                        .saturating_add(blob.len())
                                        .saturating_add(32);
                                    blobs.push((pk, blob));
                                    if blob_bytes >= blob_budget {
                                        self.db_ref()
                                            .append_full_text_build_batch(&index_name, &blobs)?;
                                        blobs.clear();
                                        blob_bytes = 0;
                                    }
                                }
                                self.db_ref()
                                    .append_full_text_build_batch(&index_name, &blobs)?;
                                Ok(true)
                            },
                        )
                        .map_err(SqlError::from)?;
                    if let Some(error) = sql_error {
                        return Err(error);
                    }
                    if !streamed {
                        return Err(SqlError::Unsupported(
                            "bounded full-text build requires server_paged storage".to_string(),
                        ));
                    }
                    self.db_ref()
                        .finish_full_text_tokenization(&index_name)
                        .map_err(SqlError::from)?;
                }
            } else {
                let mut records = match self.tx.as_ref() {
                    Some(tx) => tx.scan_collection(&table).map_err(SqlError::from)?,
                    None => self
                        .db_ref()
                        .scan_collection(&table)
                        .map_err(SqlError::from)?,
                };
                self.materialize_full_text_projection(
                    &table,
                    &schema,
                    &full_text_projection_field(&index_name),
                    &expression_ast,
                    &mut records,
                )?;
                if !records.is_empty() {
                    self.insert_session_records(&table, records)?;
                }
            }
            self.create_session_index(IndexDefinition {
                name: index_name.clone(),
                collection: table.clone(),
                fields: vec![IndexField::MetadataPath(vec![full_text_projection_field(
                    &index_name,
                )])],
                unique: false,
                kind: IndexKind::FullText,
                predicate: None,
                exclusion: None,
            })?;
            schema.indexes.push(IndexSchema {
                name: index_name,
                expression,
                source_expressions: Vec::new(),
                operator_classes: selected_operator_classes.clone(),
                internal_index_names: Vec::new(),
                collations,
                unique: create_index.unique,
                access_method,
                metadata_only: false,
            });
            self.save_session_schema(&schema)?;
            return Ok(SqlResult::command("CREATE INDEX"));
        }
        let fields = match create_index
            .columns
            .iter()
            .map(|column| index_field_from_expr_for_schema(&schema, &column.column.expr))
            .collect::<Result<Vec<_>>>()
        {
            Ok(fields) => fields,
            Err(_) => {
                schema.indexes.push(IndexSchema {
                    name: index_name,
                    expression,
                    source_expressions: Vec::new(),
                    operator_classes: selected_operator_classes.clone(),
                    internal_index_names: Vec::new(),
                    collations,
                    unique: create_index.unique,
                    access_method,
                    metadata_only: true,
                });
                self.save_session_schema(&schema)?;
                return Ok(SqlResult::command("CREATE INDEX"));
            }
        };
        self.create_session_index(IndexDefinition {
            name: index_name.clone(),
            collection: table.clone(),
            fields,
            unique: create_index.unique,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })?;
        schema.indexes.push(IndexSchema {
            name: index_name,
            expression,
            source_expressions: Vec::new(),
            operator_classes: selected_operator_classes,
            internal_index_names: Vec::new(),
            collations,
            unique: create_index.unique,
            access_method,
            metadata_only: false,
        });
        self.save_session_schema(&schema)?;
        Ok(SqlResult::command("CREATE INDEX"))
    }

    pub(crate) fn execute_create_function(
        &mut self,
        create_function: &CreateFunction,
    ) -> Result<SqlResult> {
        if create_function.temporary {
            return Err(SqlError::Unsupported(
                "temporary functions are not supported".to_string(),
            ));
        }
        if create_function.or_alter {
            return Err(SqlError::Unsupported(
                "CREATE OR ALTER FUNCTION is not supported".to_string(),
            ));
        }
        // Routines key on the BARE name everywhere (routine_key, the raw
        // CREATE FUNCTION path, the raw trigger path, and the runtime fire
        // site). `relation_name` would hash a non-public schema into the
        // key, making this function invisible to every one of those — the
        // restored-trigger "function does not exist" failure.
        let (function_schema, name) = routine_schema_and_name(&object_name(&create_function.name)?);
        let (return_type, returns_set) = create_function
            .return_type
            .as_ref()
            .map(routine_return_type)
            .transpose()?
            .unwrap_or_else(|| ("void".to_string(), false));
        let requested_language = create_function
            .language
            .as_ref()
            .map(ident_value)
            .unwrap_or_else(|| "sql".to_string())
            .to_ascii_lowercase();
        let (language, internal_symbol) = if requested_language == "internal" {
            let symbol = create_function_internal_symbol(create_function)?;
            if pg_internal_codec_type(&symbol).is_none()
                && !matches!(
                    symbol.as_str(),
                    "int4range_canonical" | "int8range_canonical" | "daterange_canonical"
                )
            {
                return Err(SqlError::Unsupported(format!(
                    "LANGUAGE internal symbol {symbol} is not a registered safe BicDB codec"
                )));
            }
            (requested_language, Some(symbol))
        } else {
            (
                routine_language(create_function.language.as_ref(), &return_type)?,
                None,
            )
        };
        let (return_type_schema, return_type_declaration) = create_function
            .return_type
            .as_ref()
            .map(routine_return_type_schema)
            .transpose()?
            .unwrap_or((
                RoutineTypeSchema {
                    pg_type: "void".to_string(),
                    type_modifier: None,
                },
                "void".to_string(),
            ));
        let arg_types = routine_argument_type_schemas(create_function.args.as_ref())?;
        let (input_types, mut output_types) =
            routine_argument_directions(create_function.args.as_ref(), &arg_types);
        output_types.extend(explicit_return_type_schemas(
            create_function.return_type.as_ref(),
        )?);
        let trigger_return = matches!(return_type.as_str(), "trigger" | "event_trigger")
            .then_some(return_type.as_str());
        validate_routine_pseudo_types(
            &input_types,
            &output_types,
            arg_types.len(),
            trigger_return,
            &language,
        )?;
        let routine = RoutineSchema {
            name,
            schema: function_schema,
            kind: RoutineKind::Function,
            args: create_function
                .args
                .as_ref()
                .map(|args| args.iter().map(ToString::to_string).collect())
                .unwrap_or_default(),
            arg_types,
            return_type,
            return_type_modifier: return_type_schema.type_modifier,
            return_type_declaration: Some(return_type_declaration),
            returns_set,
            language,
            definition: create_function.to_string(),
            internal_symbol,
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            security_definer: matches!(create_function.security, Some(FunctionSecurity::Definer)),
        };
        if create_function.or_replace
            && list_user_types(self.db_ref())?
                .iter()
                .any(|user_type| base_type_uses_routine(user_type, &routine.name))
        {
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot replace function {} because a base type depends on it",
                routine.name
            )));
        }
        self.save_session_routine_if_missing(
            routine,
            create_function.if_not_exists,
            create_function.or_replace,
        )?;
        Ok(SqlResult::command("CREATE FUNCTION"))
    }
}

struct CarrierRuntimeRoleBlock {
    role: String,
    options: String,
}

/// The role name and role flags a `DO $carrier_runtime_role$` block names:
/// `target_role CONSTANT TEXT := 'carrier_app'` and
/// `EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER ...', target_role)`.
fn parse_raw_do_carrier_runtime_role(sql: &str) -> Result<CarrierRuntimeRoleBlock> {
    let role_pattern = regex::Regex::new(r"(?i)target_role\s+CONSTANT\s+TEXT\s*:=\s*'([^']+)'")
        .expect("static regex");
    let options_pattern =
        regex::Regex::new(r"(?i)CREATE ROLE %I\s+([A-Z][A-Z ]*?)\s*'").expect("static regex");
    let role = role_pattern
        .captures(sql)
        .and_then(|captures| captures.get(1))
        .map(|capture| capture.as_str().to_string());
    let options = options_pattern
        .captures(sql)
        .and_then(|captures| captures.get(1))
        .map(|capture| capture.as_str().trim().to_string());
    match (role, options) {
        (Some(role), Some(options)) if !role.is_empty() && !options.is_empty() => {
            Ok(CarrierRuntimeRoleBlock { role, options })
        }
        _ => Err(SqlError::InvalidSql(
            "carrier_runtime_role block must name a target_role and its CREATE ROLE flags"
                .to_string(),
        )),
    }
}
