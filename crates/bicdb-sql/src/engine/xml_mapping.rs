use crate::*;
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

struct ReadOnlyXmlQuery;

impl Visitor for ReadOnlyXmlQuery {
    type Break = String;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            other => ControlFlow::Break(
                other
                    .to_string()
                    .split_whitespace()
                    .next()
                    .unwrap_or("SQL")
                    .to_string(),
            ),
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(lock) = query.locks.first() {
            return ControlFlow::Break(format!("SELECT {}", lock.lock_type));
        }
        if query
            .body
            .as_select()
            .is_some_and(|select| select.into.is_some())
        {
            return ControlFlow::Break("SELECT INTO".to_string());
        }
        ControlFlow::Continue(())
    }
}

impl SqlEngine<'_> {
    pub(crate) fn eval_xml_mapping(
        &self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
        let (source, count, nulls, forest, namespace) = match (name, args) {
            ("cursor_to_xml", [source, count, nulls, forest, namespace]) => {
                (Some(source), Some(count), nulls, forest, namespace)
            }
            (
                "table_to_xml" | "query_to_xml" | "query_to_xmlschema" | "schema_to_xml",
                [source, nulls, forest, namespace],
            ) => (Some(source), None, nulls, forest, namespace),
            ("database_to_xml", [nulls, forest, namespace]) => {
                (None, None, nulls, forest, namespace)
            }
            ("database_to_xml", _) => {
                return Err(SqlError::undefined_function(format!(
                    "{name} expects three arguments"
                )));
            }
            ("cursor_to_xml", _) => {
                return Err(SqlError::undefined_function(format!(
                    "{name} expects five arguments"
                )));
            }
            ("table_to_xml" | "query_to_xml" | "query_to_xmlschema" | "schema_to_xml", _) => {
                return Err(SqlError::undefined_function(format!(
                    "{name} expects four arguments"
                )));
            }
            _ => return Ok(None),
        };
        if args.iter().any(|value| matches!(value, SqlValue::Null)) {
            return Ok(Some(SqlValue::Null));
        }
        let nulls = sql_value_bool(nulls)
            .ok_or_else(|| SqlError::invalid_parameter_value("nulls must be boolean"))?;
        let forest = sql_value_bool(forest)
            .ok_or_else(|| SqlError::invalid_parameter_value("tableforest must be boolean"))?;
        let namespace = namespace.to_cell();
        let Some(source) = source else {
            return self
                .map_database_rows(nulls, forest, &namespace)
                .map(|value| Some(SqlValue::String(value)));
        };
        if let Some(count) = count {
            let count = sql_value_i64(count)
                .and_then(|count| i32::try_from(count).ok())
                .ok_or_else(|| {
                    SqlError::invalid_parameter_value("cursor count must be an integer")
                })?;
            let mut result = crate::session::fetch_xml_cursor_rows(&source.to_cell(), count)?;
            self.normalize_xml_result_types(&mut result)?;
            return crate::xml::map_query_rows(
                &result,
                "table",
                "row",
                nulls,
                forest,
                Some(&namespace),
                || self.check_cancellation(),
            )
            .map(|value| Some(SqlValue::String(value)));
        }
        if name == "query_to_xmlschema" {
            return self
                .map_query_schema(&source.to_cell(), nulls, forest, &namespace)
                .map(|value| Some(SqlValue::String(value)));
        }
        if name == "schema_to_xml" {
            return self
                .map_schema_rows(&source.to_cell(), nulls, forest, Some(&namespace))
                .map(|value| Some(SqlValue::String(value)));
        }
        let (query, document_name, row_name) = if name == "table_to_xml" {
            let oid = i64::from(oid_alias_numeric_value(self.db_ref(), "regclass", source)?);
            let relation = resolve_regclass_name(self.db_ref(), oid)?.ok_or_else(|| {
                SqlError::data_exception_public(
                    "XX000",
                    format!("cache lookup failed for relation {oid}"),
                    None,
                )
            })?;
            let parts = crate::virtual_tables::sql_identifier_parts(&relation)?;
            let label = parts.last().expect("resolved relation has a name").clone();
            let relation = parts
                .iter()
                .map(|part| pg_quote_ident(part))
                .collect::<Vec<_>>()
                .join(".");
            let row_name = if forest {
                label.clone()
            } else {
                "row".to_string()
            };
            (format!("SELECT * FROM {relation}"), label, row_name)
        } else {
            (source.to_cell(), "table".to_string(), "row".to_string())
        };

        let result = self.xml_query_result(&query)?;
        crate::xml::map_query_rows(
            &result,
            &document_name,
            &row_name,
            nulls,
            forest,
            Some(&namespace),
            || self.check_cancellation(),
        )
        .map(|value| Some(SqlValue::String(value)))
    }

    fn map_query_schema(
        &self,
        query: &str,
        nulls: bool,
        forest: bool,
        namespace: &str,
    ) -> Result<String> {
        let multi_query_error = || {
            SqlError::data_exception_public("42P11", "cannot open multi-query plan as cursor", None)
        };
        let commands = split_sql_statements(query);
        let [command] = commands.as_slice() else {
            return Err(multi_query_error());
        };
        let mut queries = read_only_xml_queries(command)?;
        if queries.len() != 1 {
            return Err(multi_query_error());
        }
        let query = queries.pop().expect("one cursor query");
        self.authorize_read_relations(&query)?;
        // A schema-only call describes the cursor without consuming it.
        let columns = self
            .query_describe_output_columns(&query)
            .ok_or_else(multi_query_error)?;
        Ok(crate::xml::schema_mapping::query_schema(
            &columns,
            &current_database_from_gucs(&self.session_gucs),
            namespace,
            nulls,
            forest,
            self.session_gucs
                .get("xmlbinary")
                .is_some_and(|mode| mode == "hex"),
        ))
    }

    fn map_database_rows(&self, nulls: bool, forest: bool, namespace: &str) -> Result<String> {
        let mut namespaces = list_namespaces(self.db_ref())?;
        namespaces.sort_by(|left, right| left.name.cmp(&right.name));
        let role = current_user_from_gucs(&self.session_gucs);
        let mut contents = String::new();
        for schema in namespaces {
            self.check_cancellation()?;
            if schema.name.starts_with("pg_")
                || schema.name == "information_schema"
                || !role_can_use_schema(self.db_ref(), &role, &schema.name)?
            {
                continue;
            }
            contents.push_str(&self.map_schema_rows(&schema.name, nulls, forest, None)?);
            contents.push('\n');
        }
        let database = current_database_from_gucs(&self.session_gucs);
        Ok(crate::xml::wrap_xml_mapping(
            &contents,
            &database,
            Some(namespace),
        ))
    }

    fn map_schema_rows(
        &self,
        schema: &str,
        nulls: bool,
        forest: bool,
        namespace: Option<&str>,
    ) -> Result<String> {
        let label = pg_quote_literal(schema);
        let found = self.xml_query_result(&format!(
            "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = {label}"
        ))?;
        if found.rows.is_empty() {
            return Err(SqlError::data_exception_public(
                "3F000",
                format!("schema \"{schema}\" does not exist"),
                None,
            ));
        }
        // Partition roots and sequences are absent from PostgreSQL's schema
        // map. A table-level SELECT grant determines table membership.
        let tables = self.xml_query_result(&format!(
            "SELECT c.relname
               FROM pg_catalog.pg_class AS c
               JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              WHERE n.nspname = {label}
                AND c.relkind IN ('r', 'v', 'm', 'f')
                AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
              ORDER BY c.relname"
        ))?;
        let source_schema = pg_quote_ident(schema);
        let mut contents = String::new();
        for row in tables.rows {
            self.check_cancellation()?;
            let table = row[0].to_cell();
            let source = pg_quote_ident(&table);
            let result =
                self.xml_query_result(&format!("SELECT * FROM {source_schema}.{source}"))?;
            let row_name = if forest { &table } else { "row" };
            contents.push_str(&crate::xml::map_query_rows(
                &result,
                &table,
                row_name,
                nulls,
                forest,
                None,
                || self.check_cancellation(),
            )?);
            contents.push('\n');
        }
        Ok(crate::xml::wrap_xml_mapping(
            &contents,
            schema,
            Some(namespace),
        ))
    }

    fn xml_query_result(&self, query: &str) -> Result<SqlResult> {
        let mut result = None;
        for command in split_sql_statements(query) {
            for query in read_only_xml_queries(&command)? {
                self.authorize_read_relations(&query)?;
                let mut output = self.execute_query(&query)?;
                if let Some(described) = self.query_describe_output_columns(&query) {
                    if described.len() == output.columns.len() {
                        output.columns = described.iter().map(|(name, _)| name.clone()).collect();
                        output.column_types.resize(output.columns.len(), None);
                        for ((_, declared), actual) in
                            described.into_iter().zip(&mut output.column_types)
                        {
                            if actual.is_none() {
                                *actual = declared;
                            }
                        }
                    }
                }
                result = Some(output);
            }
        }
        let mut result = result
            .ok_or_else(|| SqlError::data_exception_public("22000", "invalid query", None))?;
        self.normalize_xml_result_types(&mut result)?;
        Ok(result)
    }

    fn normalize_xml_result_types(&self, result: &mut SqlResult) -> Result<()> {
        for pg_type in result.column_types.iter_mut().flatten() {
            *pg_type = common_type_base_name(self.db_ref(), pg_type)?;
        }
        Ok(())
    }
}

fn read_only_xml_queries(command: &str) -> Result<Vec<Box<Query>>> {
    let statements = parse_statements(command)?;
    if let ControlFlow::Break(operation) = statements.visit(&mut ReadOnlyXmlQuery) {
        let operation = match statements.as_slice() {
            [Statement::Query(query)]
                if matches!(operation.as_str(), "INSERT" | "UPDATE" | "DELETE" | "MERGE")
                    && !matches!(
                        query.body.as_ref(),
                        SetExpr::Insert(_)
                            | SetExpr::Update(_)
                            | SetExpr::Delete(_)
                            | SetExpr::Merge(_)
                    ) =>
            {
                "SELECT"
            }
            _ => operation.as_str(),
        };
        return Err(SqlError::data_exception_public(
            "0A000",
            format!("{operation} is not allowed in a non-volatile function"),
            None,
        )
        .with_context_line(format!("SQL statement \"{command}\"")));
    }
    Ok(statements
        .into_iter()
        .map(|statement| match statement {
            Statement::Query(query) => query,
            _ => unreachable!("checked for read-only statements"),
        })
        .collect())
}
