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
        if !matches!(name, "table_to_xml" | "query_to_xml" | "schema_to_xml") {
            return Ok(None);
        }
        let [source, nulls, forest, namespace] = args else {
            return Err(SqlError::undefined_function(format!(
                "{name} expects four arguments"
            )));
        };
        if args.iter().any(|value| matches!(value, SqlValue::Null)) {
            return Ok(Some(SqlValue::Null));
        }
        let nulls = sql_value_bool(nulls)
            .ok_or_else(|| SqlError::invalid_parameter_value("nulls must be boolean"))?;
        let forest = sql_value_bool(forest)
            .ok_or_else(|| SqlError::invalid_parameter_value("tableforest must be boolean"))?;
        let namespace = namespace.to_cell();
        if name == "schema_to_xml" {
            return self
                .map_schema_rows(&source.to_cell(), nulls, forest, &namespace)
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

    fn map_schema_rows(
        &self,
        schema: &str,
        nulls: bool,
        forest: bool,
        namespace: &str,
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
            let statements = parse_statements(&command)?;
            if let ControlFlow::Break(operation) = statements.visit(&mut ReadOnlyXmlQuery) {
                let operation = match statements.as_slice() {
                    [Statement::Query(query)]
                        if matches!(
                            operation.as_str(),
                            "INSERT" | "UPDATE" | "DELETE" | "MERGE"
                        ) && !matches!(
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
            for statement in statements {
                let Statement::Query(query) = statement else {
                    unreachable!("checked for read-only statements");
                };
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
        for pg_type in result.column_types.iter_mut().flatten() {
            *pg_type = common_type_base_name(self.db_ref(), pg_type)?;
        }
        Ok(result)
    }
}
