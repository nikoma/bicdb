//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;
#[allow(unused_imports)]
use crate::*;

impl<'db> SqlEngine<'db> {
    pub(crate) fn pg_dump_extension_fk_dependency_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some((constraint_alias, depend_alias)) = pg_dump_extension_fk_dependency_aliases(from)?
        else {
            return Ok(None);
        };
        if !pg_dump_extension_fk_dependency_projection_matches(&select.projection)
            || !pg_dump_extension_fk_dependency_selection_matches(
                select.selection.as_ref(),
                &constraint_alias,
                &depend_alias,
            )
            || query.order_by.is_some()
            || !group_by_exprs(select)?.is_empty()
            || has_aggregates(&select.projection)
        {
            return Ok(None);
        }

        Ok(Some(SqlResult::new(
            vec!["conrelid".to_string(), "confrelid".to_string()],
            Vec::new(),
        )))
    }

    pub(crate) fn pg_dump_proc_inventory_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some(aliases) = pg_dump_proc_inventory_aliases(from)? else {
            return Ok(None);
        };
        if query.order_by.is_some() {
            return Ok(None);
        }
        let Some(projection_kind) = pg_dump_proc_inventory_projection_kind(&select.projection)
        else {
            return Ok(None);
        };
        let Some(selection_kind) =
            pg_dump_proc_inventory_selection_kind(select.selection.as_ref(), &aliases)
        else {
            return Ok(None);
        };
        if projection_kind != selection_kind {
            return Ok(None);
        }

        let mut materialized_rows = Vec::new();
        let mut result_rows = Vec::new();
        let internal_proc_oids = pg_depend_rows(self.db_ref())?
            .into_iter()
            .filter_map(|row| {
                let classid = sql_value_i64(&virtual_cell(&row, "classid"))?;
                let deptype = sql_value_text(&virtual_cell(&row, "deptype"))?;
                (classid == PG_PROC_CATALOG_OID && deptype.eq_ignore_ascii_case("i"))
                    .then(|| sql_value_i64(&virtual_cell(&row, "objid")))
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        for (idx, proc_row) in pg_proc_rows(self.db_ref())?.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            if !pg_dump_proc_inventory_row_matches(&proc_row, selection_kind) {
                continue;
            }
            if sql_value_i64(&virtual_cell(&proc_row, "oid"))
                .is_some_and(|oid| internal_proc_oids.contains(&oid))
            {
                continue;
            }
            materialized_rows.push(pg_dump_proc_inventory_context_row(&aliases, &proc_row));
            result_rows.push(pg_dump_proc_inventory_result_row(selection_kind, &proc_row));
        }
        sql_profile_sql_rows_materialized(&materialized_rows);
        apply_row_limit(&mut result_rows, query)?;
        Ok(Some(SqlResult::new(
            pg_dump_proc_inventory_columns(selection_kind),
            result_rows,
        )))
    }

    pub(crate) fn pg_dump_type_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        if pg_dump_type_query_alias(from)?.is_none()
            || select.selection.is_some()
            || query.order_by.is_some()
            || !group_by_exprs(select)?.is_empty()
            || has_aggregates(&select.projection)
            || !pg_dump_type_query_projection_matches(&select.projection)
        {
            return Ok(None);
        }

        let type_rows = pg_type_rows_for_db(self.db_ref())?;
        sql_profile_sql_rows_materialized(&type_rows);
        let type_rows_by_oid = virtual_rows_by_oid(type_rows.clone());
        let class_rows_by_oid = if type_rows
            .iter()
            .any(|row| sql_value_i64(&virtual_cell(row, "typrelid")).is_some_and(|oid| oid != 0))
        {
            Some(virtual_rows_by_oid(pg_class_rows(self.db_ref())?))
        } else {
            None
        };
        let mut result_rows = type_rows
            .iter()
            .map(|row| pg_dump_type_query_result_row(row, &type_rows_by_oid, &class_rows_by_oid))
            .collect::<Vec<_>>();
        apply_row_limit(&mut result_rows, query)?;
        Ok(Some(SqlResult::new(
            pg_dump_type_query_columns(),
            result_rows,
        )))
    }

    pub(crate) fn pg_dump_cast_inventory_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some((_, cast_alias)) = table_factor_relation_alias(&from.relation, "pg_cast")? else {
            return Ok(None);
        };
        if !from.joins.is_empty()
            || !pg_dump_cast_inventory_projection_matches(&select.projection)
            || !pg_dump_cast_inventory_selection_matches(select.selection.as_ref(), &cast_alias)
        {
            return Ok(None);
        }
        let mut rows = pg_dump_cast_inventory_rows(self.db_ref())?;
        apply_row_limit(&mut rows, query)?;
        Ok(Some(SqlResult::new(pg_dump_cast_inventory_columns(), rows)))
    }

    pub(crate) fn pg_class_index_join_row_set(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<Option<RowSet>> {
        let Some((_, table_alias)) = table_factor_relation_alias(&from.relation, "pg_class")?
        else {
            return Ok(None);
        };

        let mut index_alias = None;
        let mut index_class_alias = None;
        let mut namespace_alias = None;
        for join in &from.joins {
            let Some(constraint) = join_operator_constraint(&join.join_operator) else {
                return Ok(None);
            };
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_index")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &table_alias,
                    "pg_class",
                    "oid",
                    &alias,
                    "pg_index",
                    "indrelid",
                ) {
                    return Ok(None);
                }
                index_alias = Some(alias);
                continue;
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_class")? {
                let Some(ref pg_index_alias) = index_alias else {
                    return Ok(None);
                };
                if !join_constraint_has_column_equality(
                    constraint,
                    pg_index_alias,
                    "pg_index",
                    "indexrelid",
                    &alias,
                    "pg_class",
                    "oid",
                ) {
                    return Ok(None);
                }
                index_class_alias = Some(alias);
                continue;
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_namespace")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &table_alias,
                    "pg_class",
                    "relnamespace",
                    &alias,
                    "pg_namespace",
                    "oid",
                ) {
                    return Ok(None);
                }
                namespace_alias = Some(alias);
                continue;
            }
            return Ok(None);
        }

        let Some(index_alias) = index_alias else {
            return Ok(None);
        };
        let Some(index_class_alias) = index_class_alias else {
            return Ok(None);
        };

        let table_names =
            string_filter_values_from_selection(selection, &table_alias, "pg_class", &["relname"])?;
        let schema_names = if let Some(namespace_alias) = namespace_alias.as_deref() {
            string_filter_values_from_selection(
                selection,
                namespace_alias,
                "pg_namespace",
                &["nspname"],
            )?
        } else {
            None
        };
        let index_names = string_filter_values_from_selection(
            selection,
            &index_class_alias,
            "pg_class",
            &["relname"],
        )?;
        let index_relkinds = string_filter_values_from_selection(
            selection,
            &index_class_alias,
            "pg_class",
            &["relkind"],
        )?;
        if index_relkinds
            .as_ref()
            .is_some_and(|kinds| !catalog_relkind_matches(Some(kinds), "i"))
        {
            return Ok(Some(RowSet {
                rows: Vec::new(),
                columns: pg_class_index_join_columns(
                    &table_alias,
                    &index_alias,
                    &index_class_alias,
                    namespace_alias.as_deref(),
                ),
            }));
        }
        let primary_filter =
            boolean_filter_from_selection(selection, &index_alias, "pg_index", "indisprimary");

        let filtered_relation_names =
            relation_names_for_identifier_filters(None, table_names.as_ref());
        let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
            load_relation_schemas_by_name(self.db_ref(), relation_names)?
        } else {
            relation_schemas(self.db_ref())?
        };
        let catalog_indexes = catalog_indexes(&schemas, self.db_ref());
        let table_oids = table_oids(self.db_ref());
        let triggers = list_triggers(self.db_ref())?;
        let mut rows = Vec::new();

        for schema in &schemas {
            if !catalog_name_matches(table_names.as_ref(), &schema.name)
                || !catalog_name_matches(schema_names.as_ref(), &schema.schema_name)
            {
                continue;
            }
            let table_row = pg_class_table_row_for_schema(
                self.db_ref(),
                schema,
                &catalog_indexes,
                &triggers,
                &table_oids,
            )?;
            let namespace_row = namespace_catalog_row(self.db_ref(), &schema.schema_name)?;

            if primary_filter != Some(false) {
                let primary_key_columns = primary_key_columns_for_schema(schema);
                if !primary_key_columns.is_empty() {
                    let index_name = schema.primary_key_constraint_name();
                    if catalog_name_matches(index_names.as_ref(), &index_name) {
                        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
                        let index_oid = primary_index_oid(table_oid);
                        let expression = index_expression_for_columns(&primary_key_columns);
                        let definition = pg_indexdef_string(
                            &schema.schema_name,
                            &index_name,
                            &schema.name,
                            true,
                            "btree",
                            &expression,
                        );
                        rows.push(pg_class_index_join_row(
                            &table_alias,
                            &index_alias,
                            &index_class_alias,
                            namespace_alias.as_deref(),
                            &table_row,
                            &namespace_row,
                            pg_index_row(
                                index_oid,
                                table_oid,
                                true,
                                true,
                                constraint_attnums(&schemas, &schema.name, &primary_key_columns),
                                constraint_collation_oids(
                                    &schemas,
                                    &schema.name,
                                    &primary_key_columns,
                                ),
                                default_index_opclass_oids(
                                    self.db_ref(),
                                    &schemas,
                                    &schema.name,
                                    &primary_key_columns,
                                    "btree",
                                )?,
                                None,
                                false,
                                None,
                                definition.clone(),
                            ),
                            pg_class_index_row(
                                index_oid,
                                &index_name,
                                &schema.schema_name,
                                primary_key_columns.len() as i64,
                                403,
                            ),
                        ));
                    }
                }
            }

            if primary_filter != Some(true) {
                for index in catalog_indexes
                    .iter()
                    .filter(|index| index.collection.eq_ignore_ascii_case(&schema.name))
                {
                    if !catalog_name_matches(index_names.as_ref(), &index.name) {
                        continue;
                    }
                    let table_oid = *table_oids.get(&index.collection).unwrap_or(&0);
                    let index_oid = secondary_index_oid(&index.schema_name, &index.name);
                    let definition = catalog_indexdef_string(index);
                    rows.push(pg_class_index_join_row(
                        &table_alias,
                        &index_alias,
                        &index_class_alias,
                        namespace_alias.as_deref(),
                        &table_row,
                        &namespace_row,
                        pg_index_row(
                            index_oid,
                            table_oid,
                            false,
                            index.unique,
                            index.indkey.clone(),
                            index.collations.clone(),
                            index_operator_class_oids(
                                &index.access_method,
                                &index.operator_classes,
                            ),
                            index.indexprs.clone(),
                            index.exclusion,
                            index.predicate.clone(),
                            definition.clone(),
                        ),
                        pg_class_index_row(
                            index_oid,
                            &index.name,
                            &index.schema_name,
                            index.relnatts,
                            access_method_oid(&index.access_method),
                        ),
                    ));
                }
            }
        }

        let columns = pg_class_index_join_columns(
            &table_alias,
            &index_alias,
            &index_class_alias,
            namespace_alias.as_deref(),
        );
        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(row_set_from_sql_rows(rows, columns)))
    }

    pub(crate) fn pg_class_namespace_join_row_set(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
        projection: Option<&[SelectItem]>,
        order_by: Option<&OrderBy>,
    ) -> Result<Option<RowSet>> {
        let Some((_, class_alias)) = table_factor_relation_alias(&from.relation, "pg_class")?
        else {
            return Ok(None);
        };
        let [join] = from.joins.as_slice() else {
            return Ok(None);
        };
        let Some(constraint) = join_operator_constraint(&join.join_operator) else {
            return Ok(None);
        };
        let Some((_, namespace_alias)) =
            table_factor_relation_alias(&join.relation, "pg_namespace")?
        else {
            return Ok(None);
        };
        if !join_constraint_has_column_equality(
            constraint,
            &class_alias,
            "pg_class",
            "relnamespace",
            &namespace_alias,
            "pg_namespace",
            "oid",
        ) {
            return Ok(None);
        }

        let relnames =
            string_filter_values_from_selection(selection, &class_alias, "pg_class", &["relname"])?;
        let relkinds =
            string_filter_values_from_selection(selection, &class_alias, "pg_class", &["relkind"])?;
        let schema_names = string_filter_values_from_selection(
            selection,
            &namespace_alias,
            "pg_namespace",
            &["nspname"],
        )?;
        let schema_regex_names = regex_exact_values_from_selection(
            selection,
            &namespace_alias,
            "pg_namespace",
            &["nspname"],
        )?;
        let schema_names = intersect_optional_string_filters(schema_names, schema_regex_names);
        let columns = pg_class_namespace_join_columns(&class_alias, &namespace_alias);
        let summary_path_relkinds = relkinds
            .as_ref()
            .is_some_and(|kinds| !catalog_relkind_matches(Some(kinds), "i"));
        if summary_path_relkinds
            && pg_class_namespace_summary_selection_covered(
                selection,
                &class_alias,
                &namespace_alias,
            )?
            && pg_class_namespace_summary_path_allowed(
                selection,
                projection,
                order_by,
                &class_alias,
                &namespace_alias,
            )
        {
            let rows = pg_class_namespace_summary_join_rows(
                self.db_ref(),
                &class_alias,
                &namespace_alias,
                relnames.as_ref(),
                relkinds.as_ref(),
                schema_names.as_ref(),
            )?;
            sql_profile_sql_rows_materialized(&rows);
            return Ok(Some(row_set_from_sql_rows(rows, columns)));
        }
        let namespace_rows = pg_namespace_catalog_rows_by_oid(self.db_ref())?;
        let mut rows = Vec::new();
        for class_row in
            pg_class_rows_filtered(self.db_ref(), relnames.as_ref(), relkinds.as_ref(), None)?
        {
            let namespace_row = sql_value_i64(&virtual_cell(&class_row, "relnamespace"))
                .and_then(|oid| namespace_rows.get(&oid).cloned());
            let Some(namespace_row) = namespace_row else {
                continue;
            };
            if let SqlValue::String(namespace_name) = virtual_cell(&namespace_row, "nspname") {
                if !catalog_name_matches(schema_names.as_ref(), &namespace_name) {
                    continue;
                }
            }
            rows.push(pg_class_namespace_join_row(
                &class_alias,
                &namespace_alias,
                class_row,
                namespace_row,
            ));
        }

        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(row_set_from_sql_rows(rows, columns)))
    }

    pub(crate) fn pg_class_indexrelid_join_row_set(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<Option<RowSet>> {
        let Some((_, class_alias)) = table_factor_relation_alias(&from.relation, "pg_class")?
        else {
            return Ok(None);
        };

        let mut index_alias = None;
        let mut namespace_alias = None;
        for join in &from.joins {
            let Some(constraint) = join_operator_constraint(&join.join_operator) else {
                return Ok(None);
            };
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_index")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &class_alias,
                    "pg_class",
                    "oid",
                    &alias,
                    "pg_index",
                    "indexrelid",
                ) {
                    return Ok(None);
                }
                index_alias = Some(alias);
                continue;
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_namespace")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &class_alias,
                    "pg_class",
                    "relnamespace",
                    &alias,
                    "pg_namespace",
                    "oid",
                ) {
                    return Ok(None);
                }
                namespace_alias = Some(alias);
                continue;
            }
            return Ok(None);
        }

        let Some(index_alias) = index_alias else {
            return Ok(None);
        };
        let columns = pg_class_indexrelid_join_columns(
            &class_alias,
            &index_alias,
            namespace_alias.as_deref(),
        );
        let empty = || {
            Ok(Some(RowSet {
                rows: Vec::new(),
                columns: columns.clone(),
            }))
        };

        let index_names =
            string_filter_values_from_selection(selection, &class_alias, "pg_class", &["relname"])?;
        let index_relkinds =
            string_filter_values_from_selection(selection, &class_alias, "pg_class", &["relkind"])?;
        if index_relkinds
            .as_ref()
            .is_some_and(|kinds| !catalog_relkind_matches(Some(kinds), "i"))
        {
            return empty();
        }
        let schema_names = if let Some(namespace_alias) = namespace_alias.as_deref() {
            string_filter_values_from_selection(
                selection,
                namespace_alias,
                "pg_namespace",
                &["nspname"],
            )?
        } else {
            None
        };
        let index_oids = index_oids_for_name_filters(
            self.db_ref(),
            index_names.as_ref(),
            schema_names.as_ref(),
        )?;
        if index_oids.as_ref().is_some_and(BTreeSet::is_empty) {
            return empty();
        }

        let pg_index_rows = pg_index_rows_filtered(self.db_ref(), None, None, index_oids.as_ref())?;
        let class_oids = pg_index_rows
            .iter()
            .filter_map(|row| sql_value_i64(&virtual_cell(row, "indexrelid")))
            .collect::<BTreeSet<_>>();
        let class_rows = pg_class_catalog_rows_by_oid(self.db_ref(), &class_oids)?;
        let namespace_rows = if namespace_alias.is_some() {
            Some(pg_namespace_catalog_rows_by_oid(self.db_ref())?)
        } else {
            None
        };
        let mut rows = Vec::new();
        for pg_index_row in pg_index_rows {
            let Some(index_oid) = sql_value_i64(&virtual_cell(&pg_index_row, "indexrelid")) else {
                continue;
            };
            let Some(class_row) = class_rows.get(&index_oid).cloned() else {
                continue;
            };
            let namespace_row = match namespace_alias.as_deref() {
                Some(_) => {
                    let oid =
                        sql_value_i64(&virtual_cell(&class_row, "relnamespace")).unwrap_or(2200);
                    namespace_rows
                        .as_ref()
                        .and_then(|rows| rows.get(&oid).cloned())
                }
                None => None,
            };
            rows.push(pg_class_indexrelid_join_row(
                &class_alias,
                &index_alias,
                namespace_alias.as_deref(),
                class_row,
                pg_index_row,
                namespace_row,
            ));
        }

        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(row_set_from_sql_rows(rows, columns)))
    }

    pub(crate) fn pg_constraint_class_namespace_join_row_set(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<Option<RowSet>> {
        let Some((_, constraint_alias)) =
            table_factor_relation_alias(&from.relation, "pg_constraint")?
        else {
            return Ok(None);
        };

        let mut class_alias = None;
        let mut namespace_alias = None;
        for join in &from.joins {
            let Some(constraint) = join_operator_constraint(&join.join_operator) else {
                return Ok(None);
            };
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_class")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &constraint_alias,
                    "pg_constraint",
                    "conrelid",
                    &alias,
                    "pg_class",
                    "oid",
                ) {
                    return Ok(None);
                }
                class_alias = Some(alias);
                continue;
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_namespace")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &constraint_alias,
                    "pg_constraint",
                    "connamespace",
                    &alias,
                    "pg_namespace",
                    "oid",
                ) {
                    return Ok(None);
                }
                namespace_alias = Some(alias);
                continue;
            }
            return Ok(None);
        }

        let Some(class_alias) = class_alias else {
            return Ok(None);
        };
        let columns = pg_constraint_class_namespace_join_columns(
            &constraint_alias,
            &class_alias,
            namespace_alias.as_deref(),
        );
        let empty = || {
            Ok(Some(RowSet {
                rows: Vec::new(),
                columns: columns.clone(),
            }))
        };

        let constraint_names = string_filter_values_from_selection(
            selection,
            &constraint_alias,
            "pg_constraint",
            &["conname"],
        )?;
        let constraint_types = string_filter_values_from_selection(
            selection,
            &constraint_alias,
            "pg_constraint",
            &["contype"],
        )?;
        let table_names =
            string_filter_values_from_selection(selection, &class_alias, "pg_class", &["relname"])?;
        let namespace_names = if let Some(namespace_alias) = namespace_alias.as_deref() {
            string_filter_values_from_selection(
                selection,
                namespace_alias,
                "pg_namespace",
                &["nspname"],
            )?
        } else {
            None
        };

        let table_oids = table_oids(self.db_ref());
        let schemas = if let Some(relation_names) =
            relation_names_for_identifier_filters(None, table_names.as_ref())
        {
            load_relation_schemas_by_name(self.db_ref(), &relation_names)?
        } else {
            list_schemas(self.db_ref())?
        };
        let relation_oids = if table_names.is_some() || namespace_names.is_some() {
            let oids = schemas
                .iter()
                .filter(|schema| {
                    catalog_name_matches(table_names.as_ref(), &schema.name)
                        && catalog_name_matches(namespace_names.as_ref(), &schema.schema_name)
                })
                .filter_map(|schema| table_oids.get(&schema.name).copied())
                .collect::<BTreeSet<_>>();
            if oids.is_empty() {
                return empty();
            }
            Some(oids)
        } else {
            None
        };

        let constraint_rows = pg_constraint_rows_filtered(
            self.db_ref(),
            constraint_names.as_ref(),
            constraint_types.as_ref(),
            relation_oids.as_ref(),
        )?;
        let class_oids = constraint_rows
            .iter()
            .filter_map(|row| sql_value_i64(&virtual_cell(row, "conrelid")))
            .collect::<BTreeSet<_>>();
        let class_rows = pg_class_catalog_rows_by_oid(self.db_ref(), &class_oids)?;
        let namespace_rows = if namespace_alias.is_some() {
            Some(pg_namespace_catalog_rows_by_oid(self.db_ref())?)
        } else {
            None
        };
        let mut rows = Vec::new();
        for constraint_row in constraint_rows {
            let Some(conrelid) = sql_value_i64(&virtual_cell(&constraint_row, "conrelid")) else {
                continue;
            };
            let Some(class_row) = class_rows.get(&conrelid).cloned() else {
                continue;
            };
            let namespace_row = match namespace_alias.as_deref() {
                Some(_) => {
                    let oid = sql_value_i64(&virtual_cell(&constraint_row, "connamespace"))
                        .unwrap_or(2200);
                    namespace_rows
                        .as_ref()
                        .and_then(|rows| rows.get(&oid).cloned())
                }
                None => None,
            };
            rows.push(pg_constraint_class_namespace_join_row(
                &constraint_alias,
                &class_alias,
                namespace_alias.as_deref(),
                &constraint_row,
                class_row,
                namespace_row,
            ));
        }

        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(row_set_from_sql_rows(rows, columns)))
    }

    pub(crate) fn pg_locks_class_join_row_set(
        &self,
        from: &TableWithJoins,
    ) -> Result<Option<RowSet>> {
        let Some((_, locks_alias)) = table_factor_relation_alias(&from.relation, "pg_locks")?
        else {
            return Ok(None);
        };
        let [join] = from.joins.as_slice() else {
            return Ok(None);
        };
        let Some(constraint) = join_operator_constraint(&join.join_operator) else {
            return Ok(None);
        };
        let Some((_, class_alias)) = table_factor_relation_alias(&join.relation, "pg_class")?
        else {
            return Ok(None);
        };
        if !join_constraint_has_column_equality(
            constraint,
            &locks_alias,
            "pg_locks",
            "relation",
            &class_alias,
            "pg_class",
            "oid",
        ) {
            return Ok(None);
        }

        Ok(Some(RowSet {
            rows: Vec::new(),
            columns: pg_locks_class_join_columns(&locks_alias, &class_alias),
        }))
    }

    pub(crate) fn pg_constraint_foreign_key_join_row_set(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<Option<RowSet>> {
        let Some((_, constraint_alias)) =
            table_factor_relation_alias(&from.relation, "pg_constraint")?
        else {
            return Ok(None);
        };

        let mut constrained_class_alias = None;
        let mut referenced_class_alias = None;
        let mut constrained_attribute_alias = None;
        let mut referenced_attribute_alias = None;
        let mut namespace_alias = None;

        for join in &from.joins {
            let Some(constraint) = join_operator_constraint(&join.join_operator) else {
                return Ok(None);
            };
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_class")? {
                if join_constraint_has_column_equality(
                    constraint,
                    &constraint_alias,
                    "pg_constraint",
                    "conrelid",
                    &alias,
                    "pg_class",
                    "oid",
                ) {
                    constrained_class_alias = Some(alias);
                    continue;
                }
                if join_constraint_has_column_equality(
                    constraint,
                    &constraint_alias,
                    "pg_constraint",
                    "confrelid",
                    &alias,
                    "pg_class",
                    "oid",
                ) {
                    referenced_class_alias = Some(alias);
                    continue;
                }
                return Ok(None);
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_attribute")? {
                if let Some(class_alias) = constrained_class_alias.as_deref() {
                    if join_constraint_has_column_equality(
                        constraint,
                        &alias,
                        "pg_attribute",
                        "attrelid",
                        class_alias,
                        "pg_class",
                        "oid",
                    ) && join_constraint_has_attribute_key_equality(
                        constraint,
                        &alias,
                        &constraint_alias,
                        "conkey",
                    ) {
                        constrained_attribute_alias = Some(alias);
                        continue;
                    }
                }
                if let Some(class_alias) = referenced_class_alias.as_deref() {
                    if join_constraint_has_column_equality(
                        constraint,
                        &alias,
                        "pg_attribute",
                        "attrelid",
                        class_alias,
                        "pg_class",
                        "oid",
                    ) && join_constraint_has_attribute_key_equality(
                        constraint,
                        &alias,
                        &constraint_alias,
                        "confkey",
                    ) {
                        referenced_attribute_alias = Some(alias);
                        continue;
                    }
                }
                return Ok(None);
            }
            if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_namespace")? {
                if !join_constraint_has_column_equality(
                    constraint,
                    &constraint_alias,
                    "pg_constraint",
                    "connamespace",
                    &alias,
                    "pg_namespace",
                    "oid",
                ) {
                    return Ok(None);
                }
                namespace_alias = Some(alias);
                continue;
            }
            return Ok(None);
        }

        let Some(constrained_class_alias) = constrained_class_alias else {
            return Ok(None);
        };
        let Some(referenced_class_alias) = referenced_class_alias else {
            return Ok(None);
        };

        let columns = pg_constraint_foreign_key_join_columns(
            &constraint_alias,
            &constrained_class_alias,
            &referenced_class_alias,
            constrained_attribute_alias.as_deref(),
            referenced_attribute_alias.as_deref(),
            namespace_alias.as_deref(),
        );
        let empty = || {
            Ok(Some(RowSet {
                rows: Vec::new(),
                columns: columns.clone(),
            }))
        };

        let requested_types = string_filter_values_from_selection(
            selection,
            &constraint_alias,
            "pg_constraint",
            &["contype"],
        )?;
        if requested_types
            .as_ref()
            .is_some_and(|types| !catalog_name_matches(Some(types), "f"))
        {
            return empty();
        }
        let constraint_names = string_filter_values_from_selection(
            selection,
            &constraint_alias,
            "pg_constraint",
            &["conname"],
        )?;
        let constrained_table_names = string_filter_values_from_selection(
            selection,
            &constrained_class_alias,
            "pg_class",
            &["relname"],
        )?;
        let referenced_table_names = string_filter_values_from_selection(
            selection,
            &referenced_class_alias,
            "pg_class",
            &["relname"],
        )?;
        let namespace_names = if let Some(namespace_alias) = namespace_alias.as_deref() {
            string_filter_values_from_selection(
                selection,
                namespace_alias,
                "pg_namespace",
                &["nspname"],
            )?
        } else {
            None
        };

        let table_oids = table_oids(self.db_ref());
        let schemas = if let Some(relation_names) =
            relation_names_for_identifier_filters(None, constrained_table_names.as_ref())
        {
            load_relation_schemas_by_name(self.db_ref(), &relation_names)?
        } else {
            list_schemas(self.db_ref())?
        };
        let constrained_oids = if constrained_table_names.is_some() || namespace_names.is_some() {
            let oids = schemas
                .iter()
                .filter(|schema| {
                    catalog_name_matches(constrained_table_names.as_ref(), &schema.name)
                        && catalog_name_matches(namespace_names.as_ref(), &schema.schema_name)
                })
                .filter_map(|schema| table_oids.get(&schema.name).copied())
                .collect::<BTreeSet<_>>();
            if oids.is_empty() {
                return empty();
            }
            Some(oids)
        } else {
            None
        };
        let fk_types = BTreeSet::from(["f".to_string()]);
        let oid_to_relation = table_oids
            .iter()
            .map(|(relation, oid)| (*oid, relation.clone()))
            .collect::<BTreeMap<_, _>>();
        let constraint_rows = pg_constraint_rows_filtered(
            self.db_ref(),
            constraint_names.as_ref(),
            Some(&fk_types),
            constrained_oids.as_ref(),
        )?;
        let class_oids = constraint_rows
            .iter()
            .filter_map(|row| sql_value_i64(&virtual_cell(row, "conrelid")))
            .chain(
                constraint_rows
                    .iter()
                    .filter_map(|row| sql_value_i64(&virtual_cell(row, "confrelid"))),
            )
            .collect::<BTreeSet<_>>();
        let class_rows = pg_class_catalog_rows_by_oid(self.db_ref(), &class_oids)?;
        let namespace_rows = if namespace_alias.is_some() {
            Some(pg_namespace_catalog_rows_by_oid(self.db_ref())?)
        } else {
            None
        };
        let mut rows = Vec::new();

        for constraint_row in constraint_rows {
            let Some(constrained_oid) = sql_value_i64(&virtual_cell(&constraint_row, "conrelid"))
            else {
                continue;
            };
            let Some(referenced_oid) = sql_value_i64(&virtual_cell(&constraint_row, "confrelid"))
            else {
                continue;
            };
            let Some(referenced_relation) = oid_to_relation.get(&referenced_oid) else {
                continue;
            };
            let (_, referenced_name) = relation_schema_and_name(referenced_relation);
            if !catalog_name_matches(referenced_table_names.as_ref(), referenced_name) {
                continue;
            }
            let Some(constrained_class_row) = class_rows.get(&constrained_oid).cloned() else {
                continue;
            };
            let Some(referenced_class_row) = class_rows.get(&referenced_oid).cloned() else {
                continue;
            };
            let constrained_attribute_row = match constrained_attribute_alias.as_deref() {
                Some(_) => {
                    let Some(attnum) =
                        constraint_attnums_from_value(&virtual_cell(&constraint_row, "conkey"))
                            .first()
                            .copied()
                    else {
                        continue;
                    };
                    let Some(row) = pg_attribute_catalog_row_for_attnum(
                        self.db_ref(),
                        constrained_oid,
                        attnum,
                    )?
                    else {
                        continue;
                    };
                    Some(row)
                }
                None => None,
            };
            let referenced_attribute_row = match referenced_attribute_alias.as_deref() {
                Some(_) => {
                    let Some(attnum) =
                        constraint_attnums_from_value(&virtual_cell(&constraint_row, "confkey"))
                            .first()
                            .copied()
                    else {
                        continue;
                    };
                    let Some(row) =
                        pg_attribute_catalog_row_for_attnum(self.db_ref(), referenced_oid, attnum)?
                    else {
                        continue;
                    };
                    Some(row)
                }
                None => None,
            };
            let namespace_row = match namespace_alias.as_deref() {
                Some(_) => {
                    let oid = sql_value_i64(&virtual_cell(&constraint_row, "connamespace"))
                        .unwrap_or(2200);
                    namespace_rows
                        .as_ref()
                        .and_then(|rows| rows.get(&oid).cloned())
                }
                None => None,
            };

            rows.push(pg_constraint_foreign_key_join_row(
                &constraint_alias,
                &constrained_class_alias,
                &referenced_class_alias,
                constrained_attribute_alias.as_deref(),
                referenced_attribute_alias.as_deref(),
                namespace_alias.as_deref(),
                &constraint_row,
                constrained_class_row,
                referenced_class_row,
                constrained_attribute_row,
                referenced_attribute_row,
                namespace_row,
            ));
        }

        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(row_set_from_sql_rows(rows, columns)))
    }

    pub(crate) fn apply_pushable_base_predicates(
        &self,
        row_set: &mut RowSet,
        selection: Option<&Expr>,
    ) -> Result<()> {
        let Some(selection) = selection else {
            return Ok(());
        };
        let columns = row_set.columns.iter().cloned().collect::<BTreeSet<_>>();
        let pushable = and_terms(selection)
            .into_iter()
            .filter(|expr| predicate_references_only_columns(expr, &columns))
            .collect::<Vec<_>>();
        if pushable.is_empty() {
            return Ok(());
        }
        let (scope, context) = self.bound_row_context(&row_set.columns);
        let bound_pushable = pushable
            .iter()
            .map(|predicate| (*predicate, scope.bind(predicate)))
            .collect::<Vec<_>>();

        let mut filtered = Vec::new();
        for (idx, row) in row_set.rows.drain(..).enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut matched = true;
            for (predicate, bound) in &bound_pushable {
                let predicate_matched = match bound {
                    Some(bound) => bound
                        .eval_truth(&BoundExprFrame {
                            user_calls: &[],
                            db: self.db_ref(),
                            columns: BoundExprColumns::Values(&row),
                            vars: &context.var_values,
                        })?
                        .unwrap_or(false),
                    None => self.eval_slot_row_predicate(&row, &context, predicate)?,
                };
                if !predicate_matched {
                    matched = false;
                    break;
                }
            }
            if matched {
                filtered.push(row);
            }
        }
        row_set.rows = filtered;
        Ok(())
    }

    pub(crate) fn base_table_selection_with_outer(
        &self,
        relation: &TableFactor,
        selection: Option<&Expr>,
    ) -> Result<Option<Expr>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = relation
        else {
            return Ok(None);
        };
        let table = relation_name(name)?;
        if self.cte(&table).is_some()
            || is_virtual_table(&table)
            || load_view(self.db_ref(), &table)?.is_some()
            || load_sequence(self.db_ref(), &table)?.is_some()
        {
            return Ok(None);
        }
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let schema = load_schema(self.db_ref(), &table)?;
        let mut pushdown = None;
        for term in and_terms(selection) {
            if !self.predicate_references_only_table_or_outer(
                term,
                &table,
                &alias_name,
                schema.as_ref(),
            )? {
                continue;
            }
            pushdown = Some(match pushdown {
                Some(existing) => and_expr(existing, term.clone()),
                None => term.clone(),
            });
        }
        Ok(pushdown)
    }
}
