//! `INSTEAD OF INSERT | UPDATE | DELETE` row triggers on views.
//!
//! A statement on a view that has an INSTEAD OF trigger for its event does
//! not touch the view's base relation: each affected view row is handed to the
//! trigger functions (in name order) as NEW/OLD, and the trigger does the
//! work. A trigger returning NULL skips the row; it is not counted and later
//! triggers do not see it. RETURNING projects the row the last trigger
//! returned (OLD for DELETE).
use super::*;

const NEW_ROWS: &str = "__bicdb_instead_rows";

impl SqlSession<'_> {
    pub(crate) fn instead_of_triggers(&self, view: &str, op: &str) -> Result<Vec<TriggerSchema>> {
        let all = crate::catalog_memo::triggers_shared(self.db_ref())?;
        let mut triggers = all
            .iter()
            .filter(|trigger| {
                trigger.enabled
                    && trigger.table_name.eq_ignore_ascii_case(view)
                    && trigger.timing.eq_ignore_ascii_case("instead of")
                    && trigger.fires_on(op)
            })
            .cloned()
            .collect::<Vec<_>>();
        triggers.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(triggers)
    }

    /// PostgreSQL's restrictions on triggers attached to a view (or on
    /// INSTEAD OF triggers attached to anything else).
    pub(crate) fn validate_view_trigger(&self, trigger: &TriggerSchema) -> Result<()> {
        let relation = trigger.table_name.as_str();
        let short = relation.rsplit('.').next().unwrap_or(relation);
        let instead = trigger.timing.eq_ignore_ascii_case("instead of");
        let wrong_object = |kind: &str, detail: &str| SqlError::RaisedException {
            sqlstate: "42809".to_string(),
            message: format!("\"{short}\" is a {kind}"),
            detail: Some(detail.to_string()),
        };
        match load_view(self.db_ref(), relation)? {
            Some(view) if view.materialized => {
                return Err(wrong_object(
                    "materialized view",
                    "Triggers on materialized views are not supported.",
                ));
            }
            Some(_) => {
                if !instead {
                    if trigger.for_each.eq_ignore_ascii_case("row") {
                        return Err(wrong_object(
                            "view",
                            "Views cannot have row-level BEFORE or AFTER triggers.",
                        ));
                    }
                    return Err(SqlError::Unsupported(
                        "statement-level triggers on views are not supported".to_string(),
                    ));
                }
            }
            None if instead => {
                return Err(wrong_object(
                    "table",
                    "Tables cannot have INSTEAD OF triggers.",
                ));
            }
            None => return Ok(()),
        }
        if !trigger.for_each.eq_ignore_ascii_case("row") {
            return Err(SqlError::Unsupported(
                "INSTEAD OF triggers must be FOR EACH ROW".to_string(),
            ));
        }
        let upper = trigger.definition.to_ascii_uppercase();
        if upper.contains(" WHEN ") || upper.contains(" WHEN(") {
            return Err(SqlError::Unsupported(
                "INSTEAD OF triggers cannot have WHEN conditions".to_string(),
            ));
        }
        if upper.contains("UPDATE OF") {
            return Err(SqlError::Unsupported(
                "INSTEAD OF triggers cannot have column lists".to_string(),
            ));
        }
        if trigger.is_constraint {
            return Err(SqlError::Unsupported(
                "INSTEAD OF triggers cannot be constraint triggers".to_string(),
            ));
        }
        Ok(())
    }

    fn view_row_value(view: &ViewSchema, row: &[SqlValue]) -> SqlValue {
        let mut object = serde_json::Map::new();
        for (column, value) in view.columns.iter().zip(row) {
            object.insert(column.name.clone(), crate::jsonb::sql_value_to_json(value));
        }
        SqlValue::Json(JsonValue::Object(object).into())
    }

    fn view_row_from_value(view: &ViewSchema, value: &SqlValue) -> Result<Vec<SqlValue>> {
        let SqlValue::Json(json) = value else {
            return Err(SqlError::InvalidSql(
                "INSTEAD OF trigger must return a row or NULL".to_string(),
            ));
        };
        let object = json.as_object();
        view.columns
            .iter()
            .map(|column| {
                let value = object
                    .and_then(|object| object.get(&column.name))
                    .map(json_to_sql_value)
                    .unwrap_or(SqlValue::Null);
                if value == SqlValue::Null {
                    return Ok(value);
                }
                cast_value_to_pg_type(value, &column.pg_type)
            })
            .collect()
    }

    fn instead_vars(
        trigger: &TriggerSchema,
        view: &ViewSchema,
        op: &str,
        old: Option<SqlValue>,
        new: Option<SqlValue>,
    ) -> Vec<(String, SqlValue)> {
        let short = view
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&view.name)
            .to_string();
        let schema = view
            .name
            .rsplit_once('.')
            .map(|(schema, _)| schema.to_string())
            .unwrap_or_else(|| "public".to_string());
        let arguments = array_value_with_lower_bounds(
            trigger
                .arguments
                .iter()
                .cloned()
                .map(SqlValue::String)
                .collect(),
            vec![0],
        );
        vec![
            ("tg_name".into(), SqlValue::String(trigger.name.clone())),
            ("tg_when".into(), SqlValue::String("INSTEAD OF".into())),
            ("tg_level".into(), SqlValue::String("ROW".into())),
            ("tg_op".into(), SqlValue::String(op.to_ascii_uppercase())),
            ("tg_table_name".into(), SqlValue::String(short.clone())),
            ("tg_relname".into(), SqlValue::String(short)),
            ("tg_table_schema".into(), SqlValue::String(schema)),
            (
                "tg_relid".into(),
                SqlValue::Int(named_relation_oid(&view.name)),
            ),
            (
                "tg_nargs".into(),
                SqlValue::Int(i64::try_from(trigger.arguments.len()).unwrap_or(i64::MAX)),
            ),
            ("tg_argv".into(), arguments),
            ("old".into(), old.unwrap_or(SqlValue::Null)),
            ("new".into(), new.unwrap_or(SqlValue::Null)),
        ]
    }

    /// Runs the triggers for one row; the row the last one returned, or
    /// `None` when one returned NULL.
    fn fire_instead_of(
        &mut self,
        triggers: &[TriggerSchema],
        view: &ViewSchema,
        op: &str,
        old: Option<&[SqlValue]>,
        new: Option<Vec<SqlValue>>,
    ) -> Result<Option<Vec<SqlValue>>> {
        let old_value = old.map(|row| Self::view_row_value(view, row));
        let mut current = new;
        let shape = super::row_triggers::TriggerRecordShape::new(&view.columns);
        for trigger in triggers {
            let new_value = current
                .as_deref()
                .map(|row| Self::view_row_value(view, row));
            let vars = Self::instead_vars(trigger, view, op, old_value.clone(), new_value);
            match self.call_trigger_function(
                &trigger.name,
                &trigger.function_name,
                &vars,
                &shape,
            )? {
                None | Some(SqlValue::Null) => return Ok(None),
                Some(returned) => {
                    current = Some(Self::view_row_from_value(view, &returned)?);
                }
            }
        }
        Ok(Some(match current {
            Some(row) => row,
            None => old.map(<[SqlValue]>::to_vec).unwrap_or_default(),
        }))
    }

    /// `SELECT <returning> FROM <alias>` over the rows the triggers produced.
    fn instead_returning(
        &mut self,
        view: &ViewSchema,
        alias: &str,
        items: &[SelectItem],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<SqlResult> {
        let mut query = parse_query(&format!(
            "SELECT 1 FROM {NEW_ROWS} AS \"{}\"",
            alias.replace('"', "\"\"")
        ))?;
        if let SetExpr::Select(select) = query.body.as_mut() {
            select.projection = items.to_vec();
        }
        let mut ctes = BTreeMap::new();
        ctes.insert(
            NEW_ROWS.to_string(),
            CteResult::new(
                NEW_ROWS.to_string(),
                view.columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                rows,
            )
            .with_column_types(
                view.columns
                    .iter()
                    .map(|column| Some(column.pg_type.clone()))
                    .collect(),
            ),
        );
        self.sql_engine_with_ctes(ctes).execute_query(&query)
    }

    fn view_alias(view: &ViewSchema, alias: Option<&Ident>) -> String {
        alias.map(ident_value).unwrap_or_else(|| {
            view.name
                .rsplit('.')
                .next()
                .unwrap_or(&view.name)
                .to_string()
        })
    }

    /// The view's rows a statement affects, with extra expressions per row.
    fn instead_target_rows(
        &mut self,
        view_name: &ObjectName,
        alias: &str,
        extra: Vec<Expr>,
        selection: Option<&Expr>,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        let mut query = parse_query(&format!(
            "SELECT \"{0}\".* FROM {1} AS \"{0}\"",
            alias.replace('"', "\"\""),
            view_name
        ))?;
        if let SetExpr::Select(select) = query.body.as_mut() {
            select
                .projection
                .extend(extra.into_iter().map(SelectItem::UnnamedExpr));
            select.selection = selection.cloned();
        }
        if ctes.is_empty() {
            self.execute_query(&query)
        } else {
            self.sql_engine_with_ctes(ctes).execute_query(&query)
        }
    }

    fn finish_instead(
        &mut self,
        view: &ViewSchema,
        alias: &str,
        returning: Option<&[SelectItem]>,
        rows: Vec<Vec<SqlValue>>,
        verb: &str,
    ) -> Result<SqlResult> {
        match returning {
            Some(items) => self.instead_returning(view, alias, items, rows),
            None => Ok(SqlResult::command(format!("{verb} {}", rows.len()))),
        }
    }

    pub(crate) fn execute_instead_of_insert(
        &mut self,
        insert: &Insert,
        view: ViewSchema,
        triggers: Vec<TriggerSchema>,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        if insert.on.is_some() {
            return Err(SqlError::Unsupported(
                "INSERT ... ON CONFLICT on a view with an INSTEAD OF trigger is not supported"
                    .to_string(),
            ));
        }
        let alias = Self::view_alias(&view, insert.table_alias.as_ref().map(|a| &a.alias));
        let columns: Vec<usize> = if insert.columns.is_empty() {
            (0..view.columns.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|name| {
                    let column = object_name_parts(name).last().cloned().unwrap_or_default();
                    view.columns
                        .iter()
                        .position(|candidate| candidate.name == column)
                        .ok_or_else(|| SqlError::UndefinedColumn {
                            table: view.name.clone(),
                            column,
                        })
                })
                .collect::<Result<_>>()?
        };
        let source_rows = match insert.source.as_deref() {
            None => vec![Vec::new()],
            Some(source) => {
                if ctes.is_empty() {
                    self.execute_query(source)?.rows
                } else {
                    self.sql_engine_with_ctes(ctes).execute_query(source)?.rows
                }
            }
        };
        self.with_statement_transaction(|session| {
            let mut produced = Vec::new();
            for source in source_rows {
                if source.len() > columns.len() {
                    return Err(SqlError::InvalidSql(
                        "INSERT has more expressions than target columns".to_string(),
                    ));
                }
                let mut row = vec![SqlValue::Null; view.columns.len()];
                for (index, value) in columns.iter().zip(source) {
                    row[*index] = if value == SqlValue::Null {
                        value
                    } else {
                        cast_value_to_pg_type(value, &view.columns[*index].pg_type)?
                    };
                }
                if let Some(row) =
                    session.fire_instead_of(&triggers, &view, "insert", None, Some(row))?
                {
                    produced.push(row);
                }
            }
            session.finish_instead(
                &view,
                &alias,
                insert.returning.as_deref(),
                produced,
                "INSERT 0",
            )
        })
    }

    pub(crate) fn execute_instead_of_update(
        &mut self,
        update: &sqlparser::ast::Update,
        view: ViewSchema,
        triggers: Vec<TriggerSchema>,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        if update.from.is_some() || !update.table.joins.is_empty() {
            return Err(SqlError::Unsupported(
                "UPDATE ... FROM on a view with an INSTEAD OF trigger is not supported".to_string(),
            ));
        }
        let TableFactor::Table { name, alias, .. } = &update.table.relation else {
            return Err(SqlError::Unsupported(
                "UPDATE target must be a view".to_string(),
            ));
        };
        let alias = Self::view_alias(&view, alias.as_ref().map(|alias| &alias.name));
        let mut targets = Vec::new();
        let mut values = Vec::new();
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(column) = &assignment.target else {
                return Err(SqlError::Unsupported(
                    "row assignments on a view with an INSTEAD OF trigger are not supported"
                        .to_string(),
                ));
            };
            let column = object_name_parts(column)
                .last()
                .cloned()
                .unwrap_or_default();
            let index = view
                .columns
                .iter()
                .position(|candidate| candidate.name == column)
                .ok_or_else(|| SqlError::UndefinedColumn {
                    table: view.name.clone(),
                    column,
                })?;
            targets.push(index);
            values.push(assignment.value.clone());
        }
        let width = view.columns.len();
        let rows =
            self.instead_target_rows(name, &alias, values, update.selection.as_ref(), ctes)?;
        self.with_statement_transaction(|session| {
            let mut produced = Vec::new();
            for row in rows.rows {
                let old = row[..width].to_vec();
                let mut new = old.clone();
                for (index, value) in targets.iter().zip(row[width..].iter().cloned()) {
                    new[*index] = if value == SqlValue::Null {
                        value
                    } else {
                        cast_value_to_pg_type(value, &view.columns[*index].pg_type)?
                    };
                }
                if let Some(row) = session.fire_instead_of(
                    &triggers,
                    &view,
                    "update",
                    Some(old.as_slice()),
                    Some(new),
                )? {
                    produced.push(row);
                }
            }
            session.finish_instead(
                &view,
                &alias,
                update.returning.as_deref(),
                produced,
                "UPDATE",
            )
        })
    }

    pub(crate) fn execute_instead_of_delete(
        &mut self,
        delete: &Delete,
        view: ViewSchema,
        triggers: Vec<TriggerSchema>,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        if delete.using.is_some() {
            return Err(SqlError::Unsupported(
                "DELETE ... USING on a view with an INSTEAD OF trigger is not supported"
                    .to_string(),
            ));
        }
        let tables = match &delete.from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        let [TableWithJoins { relation, joins }] = tables.as_slice() else {
            return Err(SqlError::Unsupported(
                "DELETE from a view supports one target".into(),
            ));
        };
        let TableFactor::Table { name, alias, .. } = relation else {
            return Err(SqlError::Unsupported(
                "DELETE target must be a view".to_string(),
            ));
        };
        if !joins.is_empty() {
            return Err(SqlError::Unsupported(
                "DELETE from a joined view is not supported".into(),
            ));
        }
        let alias = Self::view_alias(&view, alias.as_ref().map(|alias| &alias.name));
        let rows =
            self.instead_target_rows(name, &alias, Vec::new(), delete.selection.as_ref(), ctes)?;
        self.with_statement_transaction(|session| {
            let mut produced = Vec::new();
            for old in rows.rows {
                if let Some(row) = session.fire_instead_of(
                    &triggers,
                    &view,
                    "delete",
                    Some(old.as_slice()),
                    None,
                )? {
                    produced.push(row);
                }
            }
            session.finish_instead(
                &view,
                &alias,
                delete.returning.as_deref(),
                produced,
                "DELETE",
            )
        })
    }
}
