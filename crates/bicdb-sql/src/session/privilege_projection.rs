//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl<'db> SqlSession<'db> {
    pub(crate) fn save_session_privilege(&mut self, grant: &PrivilegeGrant) -> Result<()> {
        let existed = self.tx.is_some()
            && list_privileges(self.db_ref())?
                .into_iter()
                .any(|existing| existing == *grant);
        save_privilege(self.db_mut()?, grant)?;
        if self.tx.is_some() && !existed {
            self.ddl_undo.push(DdlUndo::DeletePrivilege {
                grant: grant.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn delete_session_privilege(&mut self, grant: &PrivilegeGrant) -> Result<()> {
        let existed = self.tx.is_some()
            && list_privileges(self.db_ref())?
                .into_iter()
                .any(|existing| existing == *grant);
        delete_privilege(self.db_mut()?, grant)?;
        if self.tx.is_some() && existed {
            self.ddl_undo.push(DdlUndo::RestorePrivilege {
                grant: grant.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn execute_raw_alter_default_privileges(
        &mut self,
        ddl: RawAlterDefaultPrivileges,
    ) -> Result<()> {
        if ddl.schema_name != "*"
            && load_namespace(self.db_ref(), &ddl.schema_name)?.is_none()
            && ddl.schema_name != "public"
        {
            return Err(SqlError::InvalidSql(format!(
                "schema \"{}\" does not exist",
                ddl.schema_name
            )));
        }
        let roles = list_roles(self.db_ref())?;
        for grantee in &ddl.grantees {
            if grantee != "public" && !roles.iter().any(|role| role.name == *grantee) {
                return Err(SqlError::UndefinedRole {
                    name: grantee.clone(),
                });
            }
        }
        let grantor = current_user_from_gucs(&self.session_gucs);
        for grantee in ddl.grantees {
            for privilege in &ddl.privileges {
                let grant = DefaultPrivilegeGrant {
                    grantor: grantor.clone(),
                    schema_name: ddl.schema_name.clone(),
                    object_type: ddl.object_type,
                    grantee: grantee.clone(),
                    privilege: privilege.clone(),
                };
                if ddl.grant {
                    self.save_session_default_privilege(&grant)?;
                } else {
                    self.delete_session_default_privilege(&grant)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn save_session_default_privilege(
        &mut self,
        grant: &DefaultPrivilegeGrant,
    ) -> Result<()> {
        let existed = self.tx.is_some()
            && list_default_privileges(self.db_ref())?
                .into_iter()
                .any(|existing| existing == *grant);
        save_default_privilege(self.db_mut()?, grant)?;
        if self.tx.is_some() && !existed {
            self.ddl_undo.push(DdlUndo::DeleteDefaultPrivilege {
                grant: grant.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn delete_session_default_privilege(
        &mut self,
        grant: &DefaultPrivilegeGrant,
    ) -> Result<()> {
        let existed = self.tx.is_some()
            && list_default_privileges(self.db_ref())?
                .into_iter()
                .any(|existing| existing == *grant);
        delete_default_privilege(self.db_mut()?, grant)?;
        if self.tx.is_some() && existed {
            self.ddl_undo.push(DdlUndo::RestoreDefaultPrivilege {
                grant: grant.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn apply_default_privileges(
        &mut self,
        owner: &str,
        schema_name: &str,
        object_type: PrivilegeObjectType,
        object_name: &str,
    ) -> Result<()> {
        let defaults = list_default_privileges(self.db_ref())?;
        for default in defaults.into_iter().filter(|default| {
            default.grantor.eq_ignore_ascii_case(owner)
                && (default.schema_name == "*"
                    || default.schema_name.eq_ignore_ascii_case(schema_name))
                && default.object_type == object_type
        }) {
            self.save_session_privilege(&PrivilegeGrant {
                column: None,
                object_type,
                object_name: object_name.to_string(),
                grantee: default.grantee,
                privilege: default.privilege,
            })?;
        }
        Ok(())
    }

    pub(crate) fn save_session_routine(&mut self, routine: &RoutineSchema) -> Result<()> {
        let previous = load_routine(self.db_ref(), routine.kind, &routine.name)?;
        let created = previous.is_none();
        save_routine(self.db_mut()?, routine)?;
        if self.tx.is_some() {
            match previous.clone() {
                Some(previous) => self
                    .ddl_undo
                    .push(DdlUndo::RestoreRoutine { routine: previous }),
                None => self.ddl_undo.push(DdlUndo::DeleteRoutine {
                    kind: routine.kind,
                    name: routine.name.clone(),
                }),
            }
        }
        if created {
            self.apply_default_privileges(
                routine.owner(),
                routine_schema_name(routine),
                PrivilegeObjectType::Function,
                &routine.name,
            )?;
        }
        Ok(())
    }

    pub(crate) fn save_session_routine_if_missing(
        &mut self,
        routine: RoutineSchema,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<()> {
        if load_routine(self.db_ref(), routine.kind, &routine.name)?.is_some() {
            if if_not_exists {
                return Ok(());
            }
            if !or_replace {
                return Err(SqlError::InvalidSql(format!(
                    "routine \"{}\" already exists",
                    routine.name
                )));
            }
        }
        self.save_session_routine(&routine)
    }

    pub(crate) fn delete_session_routine(&mut self, routine: &RoutineSchema) -> Result<bool> {
        let existed = delete_routine(self.db_mut()?, routine.kind, &routine.name)?;
        if existed && self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreRoutine {
                routine: routine.clone(),
            });
        }
        Ok(existed)
    }

    pub(crate) fn execute_create_table(&mut self, create_table: &CreateTable) -> Result<SqlResult> {
        self.execute_create_table_with_compression(create_table, &[])
    }

    pub(crate) fn execute_create_table_with_compression(
        &mut self,
        create_table: &CreateTable,
        compression: &[RawColumnCompression],
    ) -> Result<SqlResult> {
        if create_table.query.is_some() {
            return Err(SqlError::Unsupported(
                "CREATE TABLE AS SELECT is not supported".to_string(),
            ));
        }

        let table = relation_name(&create_table.name)?;
        let schema_name = relation_schema_name(&create_table.name);
        if !matches!(
            schema_name.as_str(),
            "public" | "pg_catalog" | "information_schema"
        ) {
            self.create_session_namespace_if_missing(
                NamespaceSchema {
                    name: schema_name.clone(),
                    owner: current_user_from_gucs(&self.session_gucs),
                },
                true,
            )?;
        }
        let relation_exists = load_schema(self.db_ref(), &table)?.is_some()
            || load_sequence(self.db_ref(), &table)?.is_some()
            || load_view(self.db_ref(), &table)?.is_some()
            || load_user_type(self.db_ref(), &schema_name, &table)?.is_some()
            || user_collection_names(self.db_ref())
                .iter()
                .any(|name| name == &table);
        if relation_exists {
            if create_table.if_not_exists {
                return Ok(SqlResult::command("CREATE TABLE"));
            }
            return Err(SqlError::InvalidSql(format!(
                "relation \"{table}\" already exists"
            )));
        }

        let has_like_clause = create_table.like.is_some();
        let mut columns = if let Some(like) = &create_table.like {
            columns_from_create_table_like(self.db_ref(), like)?
        } else {
            Vec::new()
        };
        let mut constraints = Vec::new();
        let mut primary_key_name = None;
        for column in &create_table.columns {
            let mut column_schema = column_schema_from_def(self.db_ref(), column)?;
            if let Some(compression) = compression
                .iter()
                .find(|compression| compression.column == column_schema.name)
            {
                Self::validate_column_compression(&column_schema)?;
                column_schema.compression = compression.compression;
            }
            reject_postgres_system_column_name(&column_schema.name)?;
            self.ensure_user_type_usage(column_schema.user_type.as_ref())?;
            if columns
                .iter()
                .any(|existing| existing.name == column_schema.name)
            {
                return Err(SqlError::InvalidSql(format!(
                    "column \"{}\" of relation \"{}\" already exists",
                    column_schema.name, table
                )));
            }
            let has_oid_alias_default = is_oid_alias_type(&column_schema.pg_type)
                || column_schema
                    .pg_type
                    .strip_suffix("[]")
                    .is_some_and(is_oid_alias_type);
            let constant_default = if has_oid_alias_default {
                column
                    .options
                    .iter()
                    .find_map(|option| match &option.option {
                        ColumnOption::Default(expr) => Some(expr),
                        _ => None,
                    })
                    .map(|expr| {
                        literal_default_value_from_expr_with_db(
                            self.db_ref(),
                            expr,
                            &column_schema.pg_type,
                        )
                    })
                    .transpose()?
                    .flatten()
            } else {
                column_default_value(column)?
            };
            if let Some(serial_type) = serial_type(&column.data_type) {
                column_schema.pg_type = serial_type.to_string();
                column_schema.nullable = false;
                let sequence_name = sequence_name_for_column(&table, &column_schema.name);
                column_schema.default_sequence = Some(sequence_name.clone());
                self.create_session_sequence_if_missing(
                    SequenceSchema {
                        owned_by_table: Some(table.clone()),
                        owned_by_column: Some(column_schema.name.clone()),
                        ..SequenceSchema::new_typed(sequence_name, serial_type, 1)
                    },
                    false,
                )?;
            }
            if let Some((identity_kind, sequence_options)) = identity_options(column)? {
                if !matches!(column_schema.pg_type.as_str(), "int2" | "int4" | "int8") {
                    return Err(SqlError::invalid_parameter_value(
                        "identity column type must be smallint, integer, or bigint",
                    ));
                }
                let sequence_name = sequence_name_for_column(&table, &column_schema.name);
                column_schema.default_sequence = Some(sequence_name.clone());
                column_schema.identity = Some(identity_kind);
                column_schema.nullable = false;
                let mut sequence =
                    sequence_from_options(sequence_name, &column_schema.pg_type, sequence_options)?;
                sequence.owned_by_table = Some(table.clone());
                sequence.owned_by_column = Some(column_schema.name.clone());
                self.create_session_sequence_if_missing(sequence, false)?;
            }
            if column_schema.default_sequence.is_none() {
                column_schema.default_sequence = column
                    .options
                    .iter()
                    .find_map(|option| match &option.option {
                        ColumnOption::Default(expr) => Some(nextval_sequence_from_expr(expr)),
                        _ => None,
                    })
                    .transpose()?
                    .flatten();
            }
            if column_schema.default_sequence.is_none() {
                column_schema.default_value = constant_default;
            } else {
                column_schema.default_expr = None;
            }
            if column_schema.primary_key && primary_key_name.is_none() {
                primary_key_name = column.options.iter().find_map(|option| {
                    if matches!(option.option, ColumnOption::PrimaryKey(_)) {
                        option.name.as_ref().map(ident_value)
                    } else {
                        None
                    }
                });
            }
            constraints.extend(column_constraints_from_def(&table, column, &column_schema)?);
            columns.push(column_schema);
        }
        for requested in compression {
            if !columns.iter().any(|column| column.name == requested.column) {
                return Err(SqlError::UndefinedColumn {
                    table: table.clone(),
                    column: requested.column.clone(),
                });
            }
        }

        for constraint in &create_table.constraints {
            match constraint {
                TableConstraint::PrimaryKey(primary_key) => {
                    reject_unsupported_constraint_characteristics(
                        primary_key.characteristics.as_ref(),
                    )?;
                    if let Some(name) = primary_key.name.as_ref().map(ident_value) {
                        primary_key_name = Some(name);
                    }
                    constraints.extend(table_constraint_schema(&table, constraint)?);
                    for name in simple_index_column_names(&primary_key.columns)? {
                        if let Some(column) = columns.iter_mut().find(|column| column.name == name)
                        {
                            column.primary_key = true;
                            column.nullable = false;
                        }
                    }
                }
                other => {
                    constraints.extend(table_constraint_schema(&table, other)?);
                }
            }
        }

        for column in &mut columns {
            if column.primary_key {
                column.nullable = false;
            }
        }

        let primary_columns = columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        reject_missing_default_operator_class(self.db_ref(), &columns, &primary_columns, "btree")?;
        if !primary_columns.is_empty() && primary_key_name.is_none() {
            primary_key_name = Some(default_primary_key_name(&table));
        }
        self.require_reference_privilege(&table, &constraints)?;
        for constraint in &constraints {
            if let ConstraintSchema::Unique {
                columns: unique_columns,
                ..
            } = constraint
            {
                reject_missing_default_operator_class(
                    self.db_ref(),
                    &columns,
                    unique_columns,
                    "btree",
                )?;
            }
        }

        if !has_like_clause && !columns.iter().any(|column| column.primary_key) {
            if let Some(id_column) = columns.iter_mut().find(|column| column.name == "id") {
                id_column.primary_key = true;
                id_column.nullable = false;
            }
        }

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

        let partitioning = create_table
            .partition_by
            .as_deref()
            .map(partitioning_schema_from_expr)
            .transpose()?;

        self.create_session_collection(&table)?;
        let row_type_oid = table_row_type_oid(namespace_oid(&schema_name), &table);
        let row_array_type_oid = table_row_array_type_oid(namespace_oid(&schema_name), &table);
        let schema = TableSchema {
            name: table,
            schema_name,
            row_type_oid: Some(row_type_oid),
            row_array_type_oid: Some(row_array_type_oid),
            columns,
            primary_key_name,
            indexes: Vec::new(),
            constraints,
            rls_enabled: false,
            rls_forced: false,
            policies: Vec::new(),
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            partitioning,
            partition_of: None,
        };
        self.save_session_schema(&schema)?;
        if let Some(definition) = primary_key_index_definition_for_schema(&schema) {
            self.create_session_index(definition)?;
        }
        for definition in unique_constraint_index_definitions_for_schema(&schema) {
            self.create_session_index(definition)?;
        }
        Ok(SqlResult::command("CREATE TABLE"))
    }

    pub(crate) fn execute_create_type(
        &mut self,
        name: &ObjectName,
        representation: Option<&UserDefinedTypeRepresentation>,
    ) -> Result<SqlResult> {
        let (schema_name, name) = user_type_identity(name)?;
        let existing = load_user_type(self.db_ref(), &schema_name, &name)?;
        let finalizing_shell = matches!(
            (
                representation,
                existing.as_ref().map(|user_type| &user_type.kind)
            ),
            (
                Some(
                    UserDefinedTypeRepresentation::SqlDefinition { .. }
                        | UserDefinedTypeRepresentation::Range { .. }
                ),
                Some(UserTypeKind::Shell)
            )
        );
        if (schema_name == "pg_catalog" && pg_type_oid_by_name(&name).is_some())
            || (existing.is_some() && !finalizing_shell)
            || list_schemas(self.db_ref())?
                .iter()
                .any(|schema| schema.schema_name == schema_name && schema.name == name)
        {
            return Err(SqlError::data_exception(
                "42710",
                format!("type \"{name}\" already exists"),
                Some(name),
            ));
        }
        if !matches!(
            schema_name.as_str(),
            "public" | "pg_catalog" | "information_schema"
        ) && load_namespace(self.db_ref(), &schema_name)?.is_none()
        {
            return Err(SqlError::data_exception(
                "3F000",
                format!("schema \"{schema_name}\" does not exist"),
                None,
            ));
        }
        let mut paired_user_type = None;
        let (oid, array_oid, kind) = match representation {
            None => {
                let allocated = allocate_user_type_oids(self.db_mut()?, 1)?;
                (allocated[0], 0, UserTypeKind::Shell)
            }
            Some(UserDefinedTypeRepresentation::SqlDefinition { options }) => {
                let shell = existing.as_ref().ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "type {schema_name}.{name} must first be created as a shell type"
                    ))
                })?;
                let definition = parse_base_type_definition(options)?;
                let input_name = definition.input.as_deref().ok_or_else(|| {
                    SqlError::InvalidSql("CREATE TYPE requires an INPUT function".to_string())
                })?;
                let output_name = definition.output.as_deref().ok_or_else(|| {
                    SqlError::InvalidSql("CREATE TYPE requires an OUTPUT function".to_string())
                })?;
                let type_name = if schema_name == "public" {
                    name.clone()
                } else {
                    format!("{schema_name}.{name}")
                };
                let (_, input_spec) = resolve_base_codec_routine(
                    self.db_ref(),
                    input_name,
                    "cstring",
                    &type_name,
                    PgInternalCodecDirection::Input,
                )?;
                let (_, output_spec) = resolve_base_codec_routine(
                    self.db_ref(),
                    output_name,
                    &type_name,
                    "cstring",
                    PgInternalCodecDirection::Output,
                )?;
                if input_spec.name != output_spec.name {
                    return Err(SqlError::InvalidSql(
                        "CREATE TYPE INPUT and OUTPUT functions use different storage codecs"
                            .to_string(),
                    ));
                }
                if definition.receive.is_some() != definition.send.is_some() {
                    return Err(SqlError::InvalidSql(
                        "CREATE TYPE RECEIVE and SEND functions must be specified together"
                            .to_string(),
                    ));
                }
                if let (Some(receive), Some(send)) =
                    (definition.receive.as_deref(), definition.send.as_deref())
                {
                    let (_, receive_spec) = resolve_base_codec_routine(
                        self.db_ref(),
                        receive,
                        "internal",
                        &type_name,
                        PgInternalCodecDirection::Receive,
                    )?;
                    let (_, send_spec) = resolve_base_codec_routine(
                        self.db_ref(),
                        send,
                        &type_name,
                        "bytea",
                        PgInternalCodecDirection::Send,
                    )?;
                    if receive_spec.name != input_spec.name || send_spec.name != input_spec.name {
                        return Err(SqlError::InvalidSql(
                            "CREATE TYPE binary and text functions use different storage codecs"
                                .to_string(),
                        ));
                    }
                }

                let like_spec = definition
                    .like_type
                    .as_deref()
                    .map(|like_type| {
                        pg_type_spec(like_type)
                            .ok_or_else(|| SqlError::undefined_type(like_type.to_string()))
                    })
                    .transpose()?;
                if like_spec.is_some_and(|spec| spec.name != input_spec.name) {
                    return Err(SqlError::InvalidSql(
                        "CREATE TYPE LIKE must match the registered codec representation"
                            .to_string(),
                    ));
                }
                let internal_length = definition
                    .internal_length
                    .or_else(|| like_spec.map(|spec| i64::from(spec.len)))
                    .unwrap_or(-1);
                let alignment = definition
                    .alignment
                    .or_else(|| like_spec.map(|spec| spec.align))
                    .unwrap_or(if internal_length == -1 { 'i' } else { 'c' });
                let storage = definition
                    .storage
                    .or_else(|| like_spec.map(|spec| spec.storage))
                    .unwrap_or('p');
                let passed_by_value = definition
                    .passed_by_value
                    .or_else(|| like_spec.map(|spec| spec.by_value))
                    .unwrap_or(false);
                if internal_length != i64::from(input_spec.len)
                    || passed_by_value != input_spec.by_value
                    || alignment != input_spec.align
                    || storage != input_spec.storage
                {
                    return Err(SqlError::InvalidSql(format!(
                        "CREATE TYPE storage declaration does not match registered {} codec",
                        input_spec.name
                    )));
                }
                if definition.collatable && !input_spec.collatable {
                    return Err(SqlError::InvalidSql(format!(
                        "registered {} codec is not collatable",
                        input_spec.name
                    )));
                }
                let allocated = allocate_user_type_oids(self.db_mut()?, 1)?;
                (
                    shell.oid,
                    allocated[0],
                    UserTypeKind::Base {
                        codec_type: input_spec.name.to_string(),
                        input: input_name.to_string(),
                        output: output_name.to_string(),
                        receive: definition.receive,
                        send: definition.send,
                        internal_length,
                        passed_by_value,
                        alignment,
                        storage,
                        category: definition.category.unwrap_or('U'),
                        preferred: definition.preferred,
                        default_expr: definition.default_expr,
                        element_type: definition.element_type,
                        delimiter: definition.delimiter.unwrap_or(','),
                        collatable: definition.collatable,
                    },
                )
            }
            Some(UserDefinedTypeRepresentation::Enum { labels }) => {
                let mut seen = BTreeSet::new();
                let labels = labels
                    .iter()
                    .map(|label| label.value.clone())
                    .map(|label| {
                        if seen.insert(label.clone()) {
                            Ok(label)
                        } else {
                            Err(SqlError::data_exception(
                                "42710",
                                format!("enum label \"{label}\" specified more than once"),
                                None,
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let allocated = allocate_user_type_oids(self.db_mut()?, labels.len() + 2)?;
                let kind = UserTypeKind::Enum {
                    labels: labels
                        .into_iter()
                        .zip(allocated.iter().copied().skip(2))
                        .enumerate()
                        .map(|(index, (label, oid))| EnumLabelSchema {
                            oid,
                            sort_order: index as f64 + 1.0,
                            label,
                        })
                        .collect(),
                };
                (allocated[0], allocated[1], kind)
            }
            Some(UserDefinedTypeRepresentation::Composite { attributes }) => {
                let mut seen = BTreeSet::new();
                let mut stored_attributes = Vec::with_capacity(attributes.len());
                for attribute in attributes {
                    let attribute_name = ident_value(&attribute.name);
                    if !seen.insert(attribute_name.clone()) {
                        return Err(SqlError::data_exception(
                            "42701",
                            format!("column \"{attribute_name}\" specified more than once"),
                            Some(attribute_name),
                        ));
                    }
                    let user_type =
                        user_type_column_from_data_type(self.db_ref(), &attribute.data_type)?;
                    self.ensure_user_type_usage(user_type.as_ref())?;
                    let pg_type = match &user_type {
                        Some(user_type) => user_type.formatted_name(),
                        None => pg_type_from_data_type(&attribute.data_type)?.0,
                    };
                    let collation = attribute
                        .collation
                        .as_ref()
                        .map(normalize_column_collation)
                        .transpose()?;
                    stored_attributes.push(CompositeAttributeSchema {
                        name: attribute_name,
                        pg_type,
                        user_type,
                        collation,
                        type_modifier: pg_type_modifier_from_data_type(&attribute.data_type)?,
                        array_ndims: array_ndims_from_data_type(&attribute.data_type),
                        dropped: false,
                    });
                }
                let allocated = allocate_user_type_oids(self.db_mut()?, 3)?;
                (
                    allocated[0],
                    allocated[1],
                    UserTypeKind::Composite {
                        relation_oid: allocated[2],
                        attributes: stored_attributes,
                    },
                )
            }
            Some(UserDefinedTypeRepresentation::Range { options }) => {
                let definition = parse_range_type_definition(options)?;
                let subtype_data_type = definition.subtype.as_ref().ok_or_else(|| {
                    SqlError::InvalidSql("CREATE TYPE AS RANGE requires SUBTYPE".to_string())
                })?;
                let subtype_user_type =
                    user_type_column_from_data_type(self.db_ref(), subtype_data_type)?;
                if subtype_user_type.is_some() {
                    return Err(SqlError::Unsupported(
                        "user-defined range subtypes are not yet supported".to_string(),
                    ));
                }
                let (subtype, _) = pg_type_from_data_type(subtype_data_type)?;
                let subtype = subtype.to_ascii_lowercase();
                let range_spec = builtin_range_spec_for_subtype(&subtype).ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "range subtype {subtype} has no registered BicDB range comparator"
                    ))
                })?;
                if definition.collation.is_some() {
                    return Err(SqlError::data_exception(
                        "42804",
                        format!("range subtype {subtype} does not support collation"),
                        None,
                    ));
                }
                let type_name = if schema_name == "public" {
                    name.clone()
                } else {
                    format!("{schema_name}.{name}")
                };
                let (canonical, canonical_oid, canonical_discrete) = if let Some(canonical) =
                    definition.canonical
                {
                    let expected_symbol =
                        range_canonical_symbol_for_subtype(&subtype).ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "range subtype {subtype} has no registered canonical range hook"
                            ))
                        })?;
                    let routine = load_routine(self.db_ref(), RoutineKind::Function, &canonical)?
                        .ok_or_else(|| {
                        SqlError::undefined_function(format!(
                            "range canonical function {canonical} does not exist"
                        ))
                    })?;
                    if routine.language != "internal"
                        || routine.internal_symbol.as_deref() != Some(expected_symbol)
                        || routine.arg_types.len() != 1
                        || !routine_type_matches(&routine.arg_types[0].pg_type, &type_name)
                        || !routine_type_matches(&routine.return_type, &type_name)
                    {
                        return Err(SqlError::InvalidSql(format!(
                            "range canonical function {canonical} must be a one-argument registered {expected_symbol} hook for {type_name}"
                        )));
                    }
                    (
                        Some(canonical),
                        routine_oid(routine.kind, &routine.name),
                        true,
                    )
                } else {
                    (None, 0, false)
                };

                let expected_opclass = default_range_opclass_name(&subtype);
                let subtype_opclass = definition
                    .subtype_opclass
                    .unwrap_or_else(|| expected_opclass.to_string());
                let bare_opclass = subtype_opclass
                    .strip_prefix("pg_catalog.")
                    .unwrap_or(&subtype_opclass);
                if bare_opclass != expected_opclass {
                    return Err(SqlError::undefined_object(format!(
                        "operator class \"{subtype_opclass}\" does not exist for access method btree"
                    )));
                }

                let (subtype_diff, subtype_diff_oid) =
                    if let Some(subtype_diff) = definition.subtype_diff {
                        let expected = default_range_subdiff_name(&subtype);
                        let bare = subtype_diff
                            .strip_prefix("pg_catalog.")
                            .unwrap_or(&subtype_diff);
                        if bare == expected {
                            (
                                Some(subtype_diff),
                                i64::from(range_spec.range_subdiff_oid().unwrap_or_default()),
                            )
                        } else {
                            let routine =
                                load_routine(self.db_ref(), RoutineKind::Function, &subtype_diff)?
                                    .ok_or_else(|| {
                                        SqlError::undefined_function(format!(
                                        "range subtype diff function {subtype_diff} does not exist"
                                    ))
                                    })?;
                            (Some(subtype_diff), routine_oid(routine.kind, &routine.name))
                        }
                    } else {
                        (None, 0)
                    };

                let (multirange_schema_name, multirange_name) = definition
                    .multirange_type_name
                    .as_ref()
                    .map(|name| paired_type_identity(name, &schema_name))
                    .transpose()?
                    .unwrap_or_else(|| (schema_name.clone(), default_multirange_name(&name)));
                if multirange_schema_name == schema_name && multirange_name == name {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("type \"{name}\" already exists"),
                        Some(name.clone()),
                    ));
                }
                if load_user_type(self.db_ref(), &multirange_schema_name, &multirange_name)?
                    .is_some()
                    || (multirange_schema_name == "pg_catalog"
                        && pg_type_oid_by_name(&multirange_name).is_some())
                {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("type \"{multirange_name}\" already exists"),
                        Some(multirange_name),
                    ));
                }
                if multirange_schema_name != schema_name
                    && !matches!(
                        multirange_schema_name.as_str(),
                        "public" | "pg_catalog" | "information_schema"
                    )
                    && load_namespace(self.db_ref(), &multirange_schema_name)?.is_none()
                {
                    return Err(SqlError::data_exception(
                        "3F000",
                        format!("schema \"{multirange_schema_name}\" does not exist"),
                        None,
                    ));
                }

                let allocated =
                    allocate_user_type_oids(self.db_mut()?, if finalizing_shell { 3 } else { 4 })?;
                let (range_oid, range_array_oid, multirange_oid, multirange_array_oid) =
                    if let Some(shell) = existing.as_ref() {
                        (shell.oid, allocated[0], allocated[1], allocated[2])
                    } else {
                        (allocated[0], allocated[1], allocated[2], allocated[3])
                    };
                let value = UserRangeValueSchema {
                    subtype: subtype.clone(),
                    subtype_user_type: None,
                    subtype_oid: i64::from(pg_type_oid(&subtype)),
                    subtype_opclass,
                    subtype_opclass_oid: i64::from(
                        range_spec.range_subopclass_oid().unwrap_or_default(),
                    ),
                    collation: definition.collation,
                    canonical,
                    canonical_oid,
                    canonical_discrete,
                    subtype_diff,
                    subtype_diff_oid,
                };
                paired_user_type = Some(UserTypeSchema {
                    name: multirange_name.clone(),
                    schema_name: multirange_schema_name.clone(),
                    owner: current_user_from_gucs(&self.session_gucs),
                    comment: None,
                    acl_explicit: false,
                    oid: multirange_oid,
                    array_oid: multirange_array_oid,
                    kind: UserTypeKind::Multirange {
                        value: value.clone(),
                        range_schema_name: schema_name.clone(),
                        range_name: name.clone(),
                        range_oid,
                    },
                });
                (
                    range_oid,
                    range_array_oid,
                    UserTypeKind::Range {
                        value,
                        multirange_schema_name,
                        multirange_name,
                        multirange_oid,
                    },
                )
            }
        };
        let existing_comment = existing
            .as_ref()
            .and_then(|user_type| user_type.comment.clone());
        let existing_acl_explicit = existing
            .as_ref()
            .is_some_and(|user_type| user_type.acl_explicit);
        let existing_owner = existing
            .as_ref()
            .map(|user_type| user_type.owner.clone())
            .unwrap_or_else(|| current_user_from_gucs(&self.session_gucs));
        let user_type = UserTypeSchema {
            name,
            schema_name,
            owner: existing_owner,
            comment: existing_comment,
            acl_explicit: existing_acl_explicit,
            oid,
            array_oid,
            kind,
        };
        self.save_session_user_type(&user_type)?;
        if let Some(paired_user_type) = paired_user_type {
            self.save_session_user_type(&paired_user_type)?;
        }
        Ok(SqlResult::command("CREATE TYPE"))
    }

    pub(crate) fn execute_create_domain(
        &mut self,
        create_domain: &sqlparser::ast::CreateDomain,
    ) -> Result<SqlResult> {
        let (schema_name, name) = user_type_identity(&create_domain.name)?;
        if (schema_name == "pg_catalog" && pg_type_oid_by_name(&name).is_some())
            || load_user_type(self.db_ref(), &schema_name, &name)?.is_some()
            || list_schemas(self.db_ref())?
                .iter()
                .any(|schema| schema.schema_name == schema_name && schema.name == name)
        {
            return Err(SqlError::data_exception(
                "42710",
                format!("type \"{name}\" already exists"),
                Some(name),
            ));
        }
        if !matches!(
            schema_name.as_str(),
            "public" | "pg_catalog" | "information_schema"
        ) && load_namespace(self.db_ref(), &schema_name)?.is_none()
        {
            return Err(SqlError::data_exception(
                "3F000",
                format!("schema \"{schema_name}\" does not exist"),
                None,
            ));
        }

        let base_user_type =
            user_type_column_from_data_type(self.db_ref(), &create_domain.data_type)?;
        self.ensure_user_type_usage(base_user_type.as_ref())?;
        let (base_type, type_modifier) = if let Some(base_user_type) = &base_user_type {
            (base_user_type.formatted_name(), None)
        } else {
            let (base_type, _) = pg_type_from_data_type(&create_domain.data_type)?;
            (
                base_type,
                pg_type_modifier_from_data_type(&create_domain.data_type)?,
            )
        };
        let collation = create_domain
            .collation
            .as_ref()
            .map(|collation| collation.value.clone());
        if collation.is_some() && !pg_type_is_collatable(&base_type) {
            return Err(SqlError::data_exception(
                "42804",
                format!("collations are not supported by type {base_type}"),
                Some(base_type),
            ));
        }
        let mut unnamed_check = 0_usize;
        let mut not_null = false;
        let mut not_null_constraint_name = None;
        let mut constraints = Vec::new();
        for constraint in &create_domain.constraints {
            let TableConstraint::Check(check) = constraint else {
                return Err(SqlError::Unsupported(format!(
                    "constraint {constraint} is not valid for domain {name}"
                )));
            };
            if let Some(marker_name) = check.name.as_ref().and_then(domain_not_null_marker_name) {
                not_null = true;
                not_null_constraint_name =
                    Some(marker_name.unwrap_or_else(|| format!("{name}_not_null")));
                continue;
            }
            let constraint_name = check.name.as_ref().map(ident_value).unwrap_or_else(|| {
                let suffix = if unnamed_check == 0 {
                    String::new()
                } else {
                    unnamed_check.to_string()
                };
                unnamed_check += 1;
                format!("{name}_check{suffix}")
            });
            constraints.push(DomainConstraintSchema {
                name: constraint_name,
                expression: check.expr.to_string(),
                validated: true,
            });
        }
        let allocated = allocate_user_type_oids(self.db_mut()?, 2)?;
        let user_type = UserTypeSchema {
            name,
            schema_name,
            owner: current_user_from_gucs(&self.session_gucs),
            comment: None,
            acl_explicit: false,
            oid: allocated[0],
            array_oid: allocated[1],
            kind: UserTypeKind::Domain {
                base_type,
                base_user_type: base_user_type.map(Box::new),
                type_modifier,
                collation,
                default_expr: create_domain.default.as_ref().map(ToString::to_string),
                not_null,
                not_null_constraint_name,
                constraints,
            },
        };
        if let Some(default_expr) = &create_domain.default {
            let value = self.eval_session_expr(default_expr)?;
            cast_value_to_user_type(value, &user_type.column_type(false))?;
        }
        self.save_session_user_type(&user_type)?;
        Ok(SqlResult::command("CREATE DOMAIN"))
    }

    pub(crate) fn execute_raw_drop_domain(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let normalized = normalize_sql(sql);
        if !normalized.starts_with("drop domain ") {
            return Ok(None);
        }
        let transformed = replace_ascii_case_insensitive(sql, "DROP DOMAIN ", "DROP TYPE ")
            .ok_or_else(|| SqlError::InvalidSql("invalid DROP DOMAIN statement".to_string()))?;
        let statements = parse_statements(&transformed)?;
        let [Statement::Drop {
            object_type: ObjectType::Type,
            if_exists,
            names,
            cascade,
            ..
        }] = statements.as_slice()
        else {
            return Err(SqlError::InvalidSql(
                "invalid DROP DOMAIN statement".to_string(),
            ));
        };
        for domain_name in names {
            let (schema_name, name) = user_type_identity(domain_name)?;
            if let Some(user_type) = load_user_type(self.db_ref(), &schema_name, &name)? {
                if !matches!(user_type.kind, UserTypeKind::Domain { .. }) {
                    return Err(SqlError::data_exception(
                        "42809",
                        format!("{schema_name}.{name} is not a domain"),
                        Some(name),
                    ));
                }
            }
        }
        self.execute_drop(ObjectType::Type, *if_exists, names, *cascade)?;
        Ok(Some(SqlResult::command("DROP DOMAIN")))
    }

    pub(crate) fn execute_raw_alter_domain(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let rewritten;
        let sql = if let Some(value) = rewrite_domain_not_null_constraint(sql) {
            rewritten = value;
            rewritten.as_str()
        } else {
            sql
        };
        let dialect = PostgreSqlDialect {};
        let mut tokens = Tokenizer::new(&dialect, sql)
            .tokenize()
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        tokens.retain(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon));
        let is_keyword = |token: &Token, keyword: &str| matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword));
        if tokens.len() < 4 || !is_keyword(&tokens[0], "alter") || !is_keyword(&tokens[1], "domain")
        {
            return Ok(None);
        }
        let operation_index = (2..tokens.len())
            .find(|index| {
                ["set", "drop", "add", "rename", "validate"]
                    .iter()
                    .any(|keyword| is_keyword(&tokens[*index], keyword))
            })
            .ok_or_else(|| SqlError::InvalidSql("ALTER DOMAIN operation is missing".to_string()))?;
        if operation_index == 2 {
            return Err(SqlError::InvalidSql(
                "ALTER DOMAIN type name is missing".to_string(),
            ));
        }
        let domain_name = tokens[2..operation_index]
            .iter()
            .map(ToString::to_string)
            .collect::<String>();
        let operation_tokens = &tokens[operation_index..];
        let operation_sql = operation_tokens
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let column_operation = operation_tokens.len() >= 2
            && (is_keyword(&operation_tokens[0], "set")
                || is_keyword(&operation_tokens[0], "drop"))
            && (is_keyword(&operation_tokens[1], "default")
                || is_keyword(&operation_tokens[1], "not"));
        let transformed = if column_operation {
            format!("ALTER TABLE {domain_name} ALTER COLUMN value {operation_sql}")
        } else {
            format!("ALTER TABLE {domain_name} {operation_sql}")
        };
        let mut statements = parse_statements(&transformed)?;
        let Statement::AlterTable(alter_table) = statements
            .pop()
            .ok_or_else(|| SqlError::InvalidSql("ALTER DOMAIN operation is missing".to_string()))?
        else {
            return Err(SqlError::InvalidSql(
                "invalid ALTER DOMAIN operation".to_string(),
            ));
        };
        if !statements.is_empty() || alter_table.operations.len() != 1 {
            return Err(SqlError::InvalidSql(
                "ALTER DOMAIN accepts one operation at a time".to_string(),
            ));
        }
        let (schema_name, name) = user_type_identity(&alter_table.name)?;
        let mut user_type = load_user_type(self.db_ref(), &schema_name, &name)?
            .ok_or_else(|| SqlError::undefined_type(format!("{schema_name}.{name}")))?;
        if !matches!(user_type.kind, UserTypeKind::Domain { .. }) {
            return Err(SqlError::data_exception(
                "42809",
                format!("{schema_name}.{name} is not a domain"),
                Some(name),
            ));
        }

        match alter_table.operations.into_iter().next().unwrap() {
            AlterTableOperation::AlterColumn { op, .. } => match op {
                AlterColumnOperation::SetDefault { value } => {
                    let default_value = self.eval_session_expr(&value)?;
                    cast_value_to_user_type(default_value, &user_type.column_type(false))?;
                    let UserTypeKind::Domain { default_expr, .. } = &mut user_type.kind else {
                        unreachable!()
                    };
                    *default_expr = Some(value.to_string());
                }
                AlterColumnOperation::DropDefault => {
                    let UserTypeKind::Domain { default_expr, .. } = &mut user_type.kind else {
                        unreachable!()
                    };
                    *default_expr = None;
                }
                AlterColumnOperation::SetNotNull => {
                    let UserTypeKind::Domain {
                        not_null,
                        not_null_constraint_name,
                        ..
                    } = &mut user_type.kind
                    else {
                        unreachable!()
                    };
                    *not_null = true;
                    *not_null_constraint_name = Some(format!("{}_not_null", user_type.name));
                }
                AlterColumnOperation::DropNotNull => {
                    let UserTypeKind::Domain {
                        not_null,
                        not_null_constraint_name,
                        ..
                    } = &mut user_type.kind
                    else {
                        unreachable!()
                    };
                    *not_null = false;
                    *not_null_constraint_name = None;
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "ALTER DOMAIN {other} is not supported"
                    )));
                }
            },
            AlterTableOperation::AddConstraint {
                constraint,
                not_valid,
            } => {
                let TableConstraint::Check(check) = constraint else {
                    return Err(SqlError::InvalidSql(
                        "domains accept only CHECK constraints".to_string(),
                    ));
                };
                if let Some(marker_name) = check.name.as_ref().and_then(domain_not_null_marker_name)
                {
                    let UserTypeKind::Domain {
                        not_null,
                        not_null_constraint_name,
                        ..
                    } = &mut user_type.kind
                    else {
                        unreachable!()
                    };
                    let constraint_name =
                        marker_name.unwrap_or_else(|| format!("{}_not_null", user_type.name));
                    if *not_null {
                        return Err(SqlError::data_exception(
                            "42710",
                            format!("constraint \"{constraint_name}\" already exists"),
                            Some(constraint_name),
                        ));
                    }
                    *not_null = true;
                    *not_null_constraint_name = Some(constraint_name);
                    self.sync_user_type_columns(&user_type, None)?;
                    self.save_session_user_type(&user_type)?;
                    return Ok(Some(SqlResult::command("ALTER DOMAIN")));
                }
                let UserTypeKind::Domain { constraints, .. } = &mut user_type.kind else {
                    unreachable!()
                };
                let constraint_name = if let Some(name) = check.name.as_ref() {
                    ident_value(name)
                } else {
                    let mut suffix = 0_usize;
                    loop {
                        let suffix_text = if suffix == 0 {
                            String::new()
                        } else {
                            suffix.to_string()
                        };
                        let candidate = format!("{}_check{suffix_text}", user_type.name);
                        if !constraints
                            .iter()
                            .any(|constraint| constraint.name == candidate)
                        {
                            break candidate;
                        }
                        suffix += 1;
                    }
                };
                if constraints
                    .iter()
                    .any(|constraint| constraint.name == constraint_name)
                {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("constraint \"{constraint_name}\" already exists"),
                        Some(constraint_name),
                    ));
                }
                let expression = check.expr.to_string();
                parse_check_expression(&expression)?;
                constraints.push(DomainConstraintSchema {
                    name: constraint_name,
                    expression,
                    validated: !not_valid,
                });
            }
            AlterTableOperation::DropConstraint {
                if_exists, name, ..
            } => {
                let constraint_name = ident_value(&name);
                let UserTypeKind::Domain {
                    not_null,
                    not_null_constraint_name,
                    constraints,
                    ..
                } = &mut user_type.kind
                else {
                    unreachable!()
                };
                if not_null_constraint_name.as_deref() == Some(&constraint_name) {
                    *not_null = false;
                    *not_null_constraint_name = None;
                } else {
                    let previous_len = constraints.len();
                    constraints.retain(|constraint| constraint.name != constraint_name);
                    if constraints.len() == previous_len && !if_exists {
                        return Err(SqlError::undefined_object(format!(
                            "constraint \"{constraint_name}\" of domain \"{}\" does not exist",
                            user_type.name
                        )));
                    }
                }
            }
            AlterTableOperation::RenameConstraint { old_name, new_name } => {
                let old_name = ident_value(&old_name);
                let new_name = ident_value(&new_name);
                let UserTypeKind::Domain {
                    not_null_constraint_name,
                    constraints,
                    ..
                } = &mut user_type.kind
                else {
                    unreachable!()
                };
                if not_null_constraint_name.as_deref() == Some(&new_name)
                    || constraints
                        .iter()
                        .any(|constraint| constraint.name == new_name)
                {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("constraint \"{new_name}\" already exists"),
                        Some(new_name),
                    ));
                }
                if not_null_constraint_name.as_deref() == Some(&old_name) {
                    *not_null_constraint_name = Some(new_name);
                    self.sync_user_type_columns(&user_type, None)?;
                    self.save_session_user_type(&user_type)?;
                    return Ok(Some(SqlResult::command("ALTER DOMAIN")));
                }
                let constraint = constraints
                    .iter_mut()
                    .find(|constraint| constraint.name == old_name)
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "constraint \"{old_name}\" of domain \"{}\" does not exist",
                            user_type.name
                        ))
                    })?;
                constraint.name = new_name;
            }
            AlterTableOperation::ValidateConstraint { name } => {
                let constraint_name = ident_value(&name);
                let UserTypeKind::Domain {
                    not_null_constraint_name,
                    constraints,
                    ..
                } = &mut user_type.kind
                else {
                    unreachable!()
                };
                if not_null_constraint_name.as_deref() == Some(&constraint_name) {
                    return Err(SqlError::data_exception(
                        "42809",
                        format!(
                            "constraint \"{constraint_name}\" of domain \"{}\" is not a check constraint",
                            user_type.name
                        ),
                        Some(constraint_name),
                    ));
                }
                let constraint = constraints
                    .iter_mut()
                    .find(|constraint| constraint.name == constraint_name)
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "constraint \"{constraint_name}\" of domain \"{}\" does not exist",
                            user_type.name
                        ))
                    })?;
                constraint.validated = true;
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "ALTER DOMAIN {other} is not supported"
                )));
            }
        }
        self.sync_user_type_columns(&user_type, None)?;
        self.save_session_user_type(&user_type)?;
        Ok(Some(SqlResult::command("ALTER DOMAIN")))
    }

    pub(crate) fn execute_raw_alter_composite_type(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let dialect = PostgreSqlDialect {};
        let mut tokens = Tokenizer::new(&dialect, sql)
            .tokenize()
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        tokens.retain(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon));
        let is_keyword = |token: &Token, keyword: &str| matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword));
        if tokens.len() < 5 || !is_keyword(&tokens[0], "alter") || !is_keyword(&tokens[1], "type") {
            return Ok(None);
        }
        let Some(operation_index) = (2..tokens.len()).find(|index| {
            ["add", "drop", "alter", "rename"]
                .iter()
                .any(|keyword| is_keyword(&tokens[*index], keyword))
        }) else {
            return Ok(None);
        };
        if operation_index + 1 >= tokens.len()
            || !is_keyword(&tokens[operation_index + 1], "attribute")
        {
            return Ok(None);
        }
        if operation_index == 2 {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE type name is missing".to_string(),
            ));
        }

        let type_name = tokens[2..operation_index]
            .iter()
            .map(ToString::to_string)
            .collect::<String>();
        let mut operation_tokens = tokens[operation_index..].to_vec();
        if operation_tokens
            .last()
            .is_some_and(|token| is_keyword(token, "cascade") || is_keyword(token, "restrict"))
        {
            operation_tokens.pop();
        }
        operation_tokens[1] = Token::make_keyword("COLUMN");
        let operation_sql = operation_tokens
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let transformed = format!("ALTER TABLE {type_name} {operation_sql}");
        let mut statements = parse_statements(&transformed)?;
        let Statement::AlterTable(alter_table) = statements
            .pop()
            .ok_or_else(|| SqlError::InvalidSql("ALTER TYPE operation is missing".to_string()))?
        else {
            return Err(SqlError::InvalidSql(
                "invalid ALTER TYPE operation".to_string(),
            ));
        };
        if !statements.is_empty() || alter_table.operations.len() != 1 {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE accepts one attribute operation at a time".to_string(),
            ));
        }

        let (schema_name, name) = user_type_identity(&alter_table.name)?;
        let previous = load_user_type(self.db_ref(), &schema_name, &name)?
            .ok_or_else(|| SqlError::undefined_type(format!("{schema_name}.{name}")))?;
        let mut updated = previous.clone();
        let UserTypeKind::Composite { attributes, .. } = &mut updated.kind else {
            return Err(SqlError::data_exception(
                "42809",
                format!("{schema_name}.{name} is not a composite type"),
                Some(name),
            ));
        };

        let mutation = match alter_table.operations.into_iter().next().unwrap() {
            AlterTableOperation::AddColumn {
                column_def,
                if_not_exists,
                ..
            } => {
                let column = column_schema_from_def(self.db_ref(), &column_def)?;
                self.ensure_user_type_usage(column.user_type.as_ref())?;
                if attributes.iter().any(|attribute| {
                    !attribute.dropped && attribute.name.eq_ignore_ascii_case(&column.name)
                }) {
                    if if_not_exists {
                        return Ok(Some(SqlResult::command("ALTER TYPE")));
                    }
                    return Err(SqlError::data_exception(
                        "42701",
                        format!("attribute \"{}\" already exists", column.name),
                        Some(column.name),
                    ));
                }
                if column.default_expr.is_some()
                    || column.default_value.is_some()
                    || column.default_sequence.is_some()
                    || column.primary_key
                    || !column.nullable
                {
                    return Err(SqlError::InvalidSql(
                        "composite type attributes cannot have constraints or defaults".to_string(),
                    ));
                }
                let attribute = composite_attribute_from_column_schema(column);
                attributes.push(attribute.clone());
                CompositeAttributeMutation::Add(attribute)
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                let [attribute_name] = column_names.as_slice() else {
                    return Err(SqlError::InvalidSql(
                        "ALTER TYPE DROP ATTRIBUTE accepts one attribute".to_string(),
                    ));
                };
                let attribute_name = ident_value(attribute_name);
                let Some(stored_index) = attributes.iter().position(|attribute| {
                    !attribute.dropped && attribute.name.eq_ignore_ascii_case(&attribute_name)
                }) else {
                    if if_exists {
                        return Ok(Some(SqlResult::command("ALTER TYPE")));
                    }
                    return Err(SqlError::undefined_object(format!(
                        "attribute \"{attribute_name}\" of composite type \"{}\" does not exist",
                        updated.name
                    )));
                };
                let active_index = attributes[..stored_index]
                    .iter()
                    .filter(|attribute| !attribute.dropped)
                    .count();
                let attribute = &mut attributes[stored_index];
                attribute.dropped = true;
                attribute.user_type = None;
                attribute.pg_type.clear();
                attribute.collation = None;
                attribute.type_modifier = None;
                attribute.array_ndims = 0;
                CompositeAttributeMutation::Drop(active_index)
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_name = ident_value(&old_column_name);
                let new_name = ident_value(&new_column_name);
                if attributes.iter().any(|attribute| {
                    !attribute.dropped && attribute.name.eq_ignore_ascii_case(&new_name)
                }) {
                    return Err(SqlError::data_exception(
                        "42701",
                        format!("attribute \"{new_name}\" already exists"),
                        Some(new_name),
                    ));
                }
                let stored_index = attributes
                    .iter()
                    .position(|attribute| {
                        !attribute.dropped && attribute.name.eq_ignore_ascii_case(&old_name)
                    })
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "attribute \"{old_name}\" of composite type \"{}\" does not exist",
                            updated.name
                        ))
                    })?;
                let index = attributes[..stored_index]
                    .iter()
                    .filter(|attribute| !attribute.dropped)
                    .count();
                attributes[stored_index].name = new_name.clone();
                CompositeAttributeMutation::Rename {
                    index,
                    name: new_name,
                }
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let attribute_name = ident_value(&column_name);
                let stored_index = attributes
                    .iter()
                    .position(|attribute| {
                        !attribute.dropped
                            && attribute.name.eq_ignore_ascii_case(&attribute_name)
                    })
                    .ok_or_else(|| {
                        SqlError::undefined_object(format!(
                            "attribute \"{attribute_name}\" of composite type \"{}\" does not exist",
                            updated.name
                        ))
                    })?;
                let AlterColumnOperation::SetDataType {
                    data_type, using, ..
                } = op
                else {
                    return Err(SqlError::Unsupported(
                        "only ALTER ATTRIBUTE TYPE is supported for composite types".to_string(),
                    ));
                };
                if using.is_some() {
                    return Err(SqlError::InvalidSql(
                        "ALTER TYPE ALTER ATTRIBUTE does not accept USING".to_string(),
                    ));
                }
                let synthetic = ColumnDef {
                    name: column_name,
                    data_type,
                    options: Vec::new(),
                };
                let column = column_schema_from_def(self.db_ref(), &synthetic)?;
                self.ensure_user_type_usage(column.user_type.as_ref())?;
                let mut attribute = composite_attribute_from_column_schema(column);
                attribute.collation = attributes[stored_index].collation.clone();
                let index = attributes[..stored_index]
                    .iter()
                    .filter(|attribute| !attribute.dropped)
                    .count();
                attributes[stored_index] = attribute.clone();
                CompositeAttributeMutation::AlterType { index, attribute }
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "ALTER TYPE attribute operation {other} is not supported"
                )));
            }
        };

        if matches!(mutation, CompositeAttributeMutation::AlterType { .. })
            && list_schemas(self.db_ref())?.iter().any(|schema| {
                schema.columns.iter().any(|column| {
                    column.user_type.as_ref().is_some_and(|column_type| {
                        user_type_column_depends_on_oid(column_type, previous.oid)
                    })
                })
            })
        {
            return Err(SqlError::Unsupported(format!(
                "cannot alter composite type {} because table columns use it",
                previous.column_type(false).formatted_name()
            )));
        }
        self.apply_composite_type_change(&previous, &updated, &mutation, true)?;
        Ok(Some(SqlResult::command("ALTER TYPE")))
    }

    pub(crate) fn apply_composite_type_change(
        &mut self,
        previous: &UserTypeSchema,
        updated: &UserTypeSchema,
        mutation: &CompositeAttributeMutation,
        persist_root: bool,
    ) -> Result<()> {
        let mut replacements = BTreeMap::new();
        replacements.insert(updated.oid, updated.clone());
        let all_types = list_user_types(self.db_ref())?;
        loop {
            let mut changed = false;
            for candidate in &all_types {
                let mut candidate = replacements
                    .get(&candidate.oid)
                    .cloned()
                    .unwrap_or_else(|| candidate.clone());
                let before = candidate.clone();
                replace_user_type_kind_references(&mut candidate.kind, &replacements);
                if candidate != before || replacements.contains_key(&candidate.oid) {
                    if replacements.get(&candidate.oid) != Some(&candidate) {
                        replacements.insert(candidate.oid, candidate);
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let mut table_updates = Vec::new();
        for schema in list_schemas(self.db_ref())? {
            let mut next_schema = schema.clone();
            let changed_columns = next_schema
                .columns
                .iter_mut()
                .enumerate()
                .filter_map(|(index, column)| {
                    replace_column_user_type_references(column, &replacements).then_some(index)
                })
                .collect::<Vec<_>>();
            if changed_columns.is_empty() {
                continue;
            }
            let mut records = self.db_ref().scan_collection(&schema.name)?;
            for record in &mut records {
                for index in &changed_columns {
                    let old_column = &schema.columns[*index];
                    if old_column.primary_key {
                        continue;
                    }
                    let value = record_column_value(record, &schema, &old_column.name);
                    let value = rewrite_composite_value(
                        value,
                        previous.oid,
                        &previous.column_type(false).formatted_name(),
                        mutation,
                    )?;
                    let value = cast_value_to_column_type(value, &next_schema.columns[*index])?;
                    set_record_column(
                        record,
                        Some(&next_schema),
                        &next_schema.columns[*index].name,
                        value,
                    )?;
                }
            }
            table_updates.push((schema, next_schema, records));
        }

        for user_type in replacements
            .values()
            .filter(|user_type| persist_root || user_type.oid != updated.oid)
        {
            self.save_session_user_type(user_type)?;
        }
        for (previous_schema, next_schema, records) in table_updates {
            self.capture_table_state_undo(&previous_schema.name, &previous_schema)?;
            for record in records {
                self.db_mut()?.insert(&previous_schema.name, record)?;
            }
            self.save_session_schema(&next_schema)?;
        }
        Ok(())
    }

    pub(crate) fn table_row_type_dependency(&self, schema: &TableSchema) -> Result<Option<String>> {
        let target_oid = schema.row_type_oid();
        for candidate in list_schemas(self.db_ref())? {
            if candidate.row_type_oid() == target_oid {
                continue;
            }
            if let Some(column) = candidate.columns.iter().find(|column| {
                column.user_type.as_ref().is_some_and(|column_type| {
                    user_type_column_depends_on_oid(column_type, target_oid)
                })
            }) {
                return Ok(Some(format!(
                    "column \"{}.{}\" uses its row type",
                    candidate.name, column.name
                )));
            }
        }
        if let Some(user_type) = list_user_types(self.db_ref())?
            .into_iter()
            .find(|user_type| user_type_kind_depends_on_oid(&user_type.kind, target_oid))
        {
            return Ok(Some(format!(
                "type \"{}\" uses its row type",
                user_type.column_type(false).formatted_name()
            )));
        }
        Ok(None)
    }

    pub(crate) fn ensure_table_row_type_change_allowed(&self, schema: &TableSchema) -> Result<()> {
        if let Some(dependency) = self.table_row_type_dependency(schema)? {
            return Err(SqlError::Unsupported(format!(
                "cannot alter table \"{}\" because {dependency}",
                schema.name
            )));
        }
        Ok(())
    }

    pub(crate) fn apply_table_row_type_change(
        &mut self,
        previous_schema: &TableSchema,
        updated_schema: &TableSchema,
        mutation: &CompositeAttributeMutation,
    ) -> Result<()> {
        let previous = table_row_type_schema(previous_schema);
        let updated = table_row_type_schema(updated_schema);
        self.apply_composite_type_change(&previous, &updated, mutation, false)
    }

    pub(crate) fn execute_alter_type(
        &mut self,
        alter_type: &sqlparser::ast::AlterType,
    ) -> Result<SqlResult> {
        use sqlparser::ast::{AlterTypeAddValuePosition, AlterTypeOperation};

        let (schema_name, name) = user_type_identity(&alter_type.name)?;
        let mut user_type = load_user_type(self.db_ref(), &schema_name, &name)?
            .ok_or_else(|| SqlError::undefined_type(format!("{schema_name}.{name}")))?;
        match &alter_type.operation {
            AlterTypeOperation::AddValue(operation) => {
                let UserTypeKind::Enum { labels } = &mut user_type.kind else {
                    return Err(SqlError::data_exception(
                        "42809",
                        format!("{schema_name}.{name} is not an enum"),
                        Some(name),
                    ));
                };
                let label = operation.value.value.clone();
                if labels.iter().any(|existing| existing.label == label) {
                    if operation.if_not_exists {
                        return Ok(SqlResult::command("ALTER TYPE"));
                    }
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("enum label \"{label}\" already exists"),
                        Some(name),
                    ));
                }
                let insertion_index = match &operation.position {
                    None => labels.len(),
                    Some(AlterTypeAddValuePosition::Before(neighbor)) => labels
                        .iter()
                        .position(|existing| existing.label == neighbor.value)
                        .ok_or_else(|| {
                            SqlError::invalid_parameter_value(format!(
                                "\"{}\" is not an existing enum label",
                                neighbor.value
                            ))
                        })?,
                    Some(AlterTypeAddValuePosition::After(neighbor)) => labels
                        .iter()
                        .position(|existing| existing.label == neighbor.value)
                        .map(|index| index + 1)
                        .ok_or_else(|| {
                            SqlError::invalid_parameter_value(format!(
                                "\"{}\" is not an existing enum label",
                                neighbor.value
                            ))
                        })?,
                };
                let oid = allocate_user_type_oids(self.db_mut()?, 1)?[0];
                labels.insert(
                    insertion_index,
                    EnumLabelSchema {
                        oid,
                        sort_order: 0.0,
                        label,
                    },
                );
                for (index, label) in labels.iter_mut().enumerate() {
                    label.sort_order = index as f64 + 1.0;
                }
                self.save_session_user_type(&user_type)?;
                self.sync_user_type_columns(&user_type, None)?;
            }
            AlterTypeOperation::RenameValue(operation) => {
                let UserTypeKind::Enum { labels } = &mut user_type.kind else {
                    return Err(SqlError::data_exception(
                        "42809",
                        format!("{schema_name}.{name} is not an enum"),
                        Some(name),
                    ));
                };
                if labels.iter().any(|label| label.label == operation.to.value) {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("enum label \"{}\" already exists", operation.to.value),
                        Some(name),
                    ));
                }
                let label = labels
                    .iter_mut()
                    .find(|label| label.label == operation.from.value)
                    .ok_or_else(|| {
                        SqlError::invalid_parameter_value(format!(
                            "\"{}\" is not an existing enum label",
                            operation.from.value
                        ))
                    })?;
                label.label = operation.to.value.clone();
                self.save_session_user_type(&user_type)?;
                self.sync_user_type_columns(
                    &user_type,
                    Some((&operation.from.value, &operation.to.value)),
                )?;
            }
            AlterTypeOperation::Rename(operation) => {
                let new_name = ident_value(&operation.new_name);
                if load_user_type(self.db_ref(), &schema_name, &new_name)?.is_some()
                    || (schema_name == "pg_catalog" && pg_type_oid_by_name(&new_name).is_some())
                {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("type \"{new_name}\" already exists"),
                        Some(new_name),
                    ));
                }
                let previous = user_type.clone();
                user_type.name = new_name;
                self.relocate_user_type(&previous, &user_type)?;
            }
        }
        Ok(SqlResult::command("ALTER TYPE"))
    }

    pub(crate) fn relocate_user_type(
        &mut self,
        previous: &UserTypeSchema,
        updated: &UserTypeSchema,
    ) -> Result<()> {
        save_user_type(self.db_mut()?, updated)?;
        if let Err(error) = delete_user_type(self.db_mut()?, &previous.schema_name, &previous.name)
        {
            let _ = delete_user_type(self.db_mut()?, &updated.schema_name, &updated.name);
            return Err(error);
        }
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreUserType {
                user_type: previous.clone(),
            });
            self.ddl_undo.push(DdlUndo::DeleteUserType {
                schema_name: updated.schema_name.clone(),
                name: updated.name.clone(),
            });
        }

        let previous_privilege_name =
            user_type_privilege_name(&previous.schema_name, &previous.name);
        let updated_privilege_name = user_type_privilege_name(&updated.schema_name, &updated.name);
        for grant in list_privileges(self.db_ref())?.into_iter().filter(|grant| {
            grant.object_type == PrivilegeObjectType::Type
                && grant.object_name == previous_privilege_name
        }) {
            self.delete_session_privilege(&grant)?;
            let mut moved = grant;
            moved.object_name = updated_privilege_name.clone();
            self.save_session_privilege(&moved)?;
        }

        self.sync_user_type_columns(updated, None)?;
        for mut routine in list_routines(self.db_ref())? {
            let mut changed =
                rewrite_routine_type_identity(&mut routine.return_type, previous, updated);
            if let Some(declaration) = &mut routine.return_type_declaration {
                changed |= rewrite_routine_type_identity(declaration, previous, updated);
            }
            for index in 0..routine.arg_types.len() {
                let argument_changed = rewrite_routine_type_identity(
                    &mut routine.arg_types[index].pg_type,
                    previous,
                    updated,
                );
                if argument_changed {
                    if let Some(argument) = routine.args.get_mut(index) {
                        rewrite_routine_argument_type_identity(argument, previous, updated);
                    }
                    changed = true;
                }
            }
            if changed {
                self.save_session_routine(&routine)?;
            }
        }
        for mut paired in list_user_types(self.db_ref())? {
            if paired.oid == updated.oid {
                continue;
            }
            let changed = match &mut paired.kind {
                UserTypeKind::Range {
                    multirange_schema_name,
                    multirange_name,
                    multirange_oid,
                    ..
                } if *multirange_oid == updated.oid => {
                    *multirange_schema_name = updated.schema_name.clone();
                    *multirange_name = updated.name.clone();
                    true
                }
                UserTypeKind::Multirange {
                    range_schema_name,
                    range_name,
                    range_oid,
                    ..
                } if *range_oid == updated.oid => {
                    *range_schema_name = updated.schema_name.clone();
                    *range_name = updated.name.clone();
                    true
                }
                _ => false,
            };
            if changed {
                self.save_session_user_type(&paired)?;
                self.sync_user_type_columns(&paired, None)?;
            }
        }
        Ok(())
    }

    pub(crate) fn save_session_user_type(&mut self, user_type: &UserTypeSchema) -> Result<()> {
        let previous = if self.tx.is_some() {
            load_user_type(self.db_ref(), &user_type.schema_name, &user_type.name)?
        } else {
            None
        };
        save_user_type(self.db_mut()?, user_type)?;
        if self.tx.is_some() {
            match previous {
                Some(previous) => self.ddl_undo.push(DdlUndo::RestoreUserType {
                    user_type: previous,
                }),
                None => self.ddl_undo.push(DdlUndo::DeleteUserType {
                    schema_name: user_type.schema_name.clone(),
                    name: user_type.name.clone(),
                }),
            }
        }
        Ok(())
    }

    pub(crate) fn delete_session_user_type(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<bool> {
        let previous = if self.tx.is_some() {
            load_user_type(self.db_ref(), schema_name, name)?
        } else {
            None
        };
        let existed = delete_user_type(self.db_mut()?, schema_name, name)?;
        if let Some(user_type) = previous {
            self.ddl_undo.push(DdlUndo::RestoreUserType { user_type });
        }
        Ok(existed)
    }
}
