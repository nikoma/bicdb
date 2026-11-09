//! Versioned diagnostic state for unresolved Oracle routine return declarations.
//! An invalid declaration remains a named object, but cannot compile or execute.
use super::*;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InvalidReturnType {
    pub format: u32,
    pub identifier: String,
    pub declaration: RoutineTypeSchema,
}
impl InvalidReturnType {
    fn diagnostics(&self) -> [String; 2] {
        [
            format!(
                "PLS-00201: identifier '{}' must be declared",
                self.identifier
            ),
            "PL/SQL: Compilation unit analysis terminated".into(),
        ]
    }
}

fn known_return_type(db: &BicDb, routine: &RoutineSchema) -> Result<bool> {
    if pg_type_oid_by_name(&routine.return_type).is_some() {
        return Ok(true);
    }
    // The declaration AST is authoritative for quoted dots and mixed case;
    // flattened metadata strings cannot recover identifier component boundaries.
    if let Ok(statements) = parse_statements(&routine.definition) {
        if let [Statement::CreateFunction(function)] = statements.as_slice() {
            if let Some(
                FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type),
            ) = &function.return_type
            {
                let mut data_type = data_type;
                while let DataType::Array(
                    ArrayElemTypeDef::AngleBracket(element)
                    | ArrayElemTypeDef::SquareBracket(element, _)
                    | ArrayElemTypeDef::Parenthesis(element),
                ) = data_type
                {
                    data_type = element;
                }
                if let DataType::Custom(name, _) = data_type {
                    let parts = object_name_parts(name);
                    let (schemas, name) = match parts.as_slice() {
                        [name] => {
                            let schemas =
                                if preview_routine_language_version(&routine.language).is_some() {
                                    vec![routine.schema.clone()]
                                } else {
                                    crate::search_path::active_path()
                                        .map(|path| path.iter().cloned().collect())
                                        .unwrap_or_else(|| vec!["public".into()])
                                };
                            (schemas, name.clone())
                        }
                        [schema, name] => (vec![schema.clone()], name.clone()),
                        _ => return Ok(false),
                    };
                    for schema in schemas {
                        if load_user_type(db, &schema, &name)?.is_some() {
                            return Ok(true);
                        }
                        let relation = relation_name_from_parts(&[schema, name.clone()])?;
                        if load_schema(db, &relation)?.is_some()
                            || load_view(db, &relation)?.is_some()
                        {
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
            }
        }
    }
    Ok(pg_user_type_oid_by_name(db, &routine.return_type)?.is_some())
}

pub(crate) fn classify(db: &BicDb, routine: &mut RoutineSchema) -> Result<Option<SqlNotice>> {
    if known_return_type(db, routine)? {
        return Ok(None);
    }
    if !preview_routine_language_version(&routine.language).is_some() || !is_preview_database(db)? {
        return Err(SqlError::undefined_type(&routine.return_type));
    }
    let declaration = RoutineTypeSchema {
        pg_type: routine.return_type.clone(),
        type_modifier: routine.return_type_modifier.clone(),
    };
    routine.oracle_invalid = Some(InvalidReturnType {
        format: 1,
        identifier: routine.return_type.clone(),
        declaration,
    });
    // There is no executable return type for an invalid object. The original
    // declaration and diagnostic remain durable; compilation rejects this state
    // before any cached executable or placeholder physical type can be used.
    routine.return_type = "void".into();
    routine.return_type_modifier = None;
    Ok(Some(SqlNotice {
        sqlstate: "01000".into(),
        message: "creation succeeded with compilation errors".into(),
        fields: Vec::new(),
    }))
}

pub(crate) fn validate(db: &BicDb, routine: &RoutineSchema) -> Result<()> {
    if let Some(state) = &routine.oracle_invalid {
        if state.format != 1
            || state.identifier.is_empty()
            || state.identifier != state.declaration.pg_type
            || routine.return_type != "void"
            || routine.return_type_modifier.is_some()
            || preview_routine_language_version(&routine.language).is_none()
            || !is_preview_database(db)?
            || !db
                .format_metadata()?
                .feature_flags
                .contains(bicdb_core::ORACLE_INVALID_ROUTINES_FEATURE_FLAG)
        {
            return Err(unsupported(
                "invalid or unfenced Oracle routine diagnostic metadata",
            ));
        }
    }
    Ok(())
}

pub(crate) fn check_executable(routine: &RoutineSchema) -> Result<()> {
    if routine.oracle_invalid.is_some() {
        return Err(errors::database_error(
            -6575,
            "42000",
            &format!(
                "Package or function {} is in an invalid state",
                routine.name
            ),
        ));
    }
    Ok(())
}

pub(crate) const OBJECTS: &str = "pg_catalog.bicdb_oracle_user_objects";
pub(crate) const ERRORS: &str = "pg_catalog.bicdb_oracle_user_errors";
const OBJECT_COLUMNS: &[(&str, &str)] = &[
    ("OBJECT_NAME", "varchar"),
    ("OBJECT_TYPE", "varchar"),
    ("STATUS", "varchar"),
];
const ERROR_COLUMNS: &[(&str, &str)] = &[
    ("NAME", "varchar"),
    ("TYPE", "varchar"),
    ("SEQUENCE", "numeric"),
    ("ATTRIBUTE", "varchar"),
    ("TEXT", "varchar"),
    ("MESSAGE_NUMBER", "numeric"),
];
pub(crate) fn columns(table: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match table {
        OBJECTS => Some(OBJECT_COLUMNS),
        ERRORS => Some(ERROR_COLUMNS),
        _ => None,
    }
}
pub(crate) fn is_relation(table: &str) -> bool {
    columns(table).is_some()
}

pub(crate) fn rows(db: &BicDb, table: &str, role: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    if !is_preview_database(db)? {
        return Err(unsupported(
            "Oracle routine dictionary requires the preview profile",
        ));
    }
    let mut rows = if table == OBJECTS {
        owned_objects(db, role)?
    } else {
        Vec::new()
    };
    for routine in list_routines(db)? {
        if routine.owner() != role {
            continue;
        }
        validate(db, &routine)?;
        let kind = match routine.kind {
            RoutineKind::Function => "FUNCTION",
            RoutineKind::Procedure => "PROCEDURE",
        };
        if table == OBJECTS {
            rows.push(object_row(
                routine.name.clone(),
                kind,
                if routine.oracle_invalid.is_some() {
                    "INVALID"
                } else {
                    "VALID"
                },
            ));
        } else if table == ERRORS {
            if let Some(state) = &routine.oracle_invalid {
                for (index, text) in state.diagnostics().into_iter().enumerate() {
                    rows.push(BTreeMap::from([
                        ("NAME".into(), SqlValue::String(routine.name.clone())),
                        ("TYPE".into(), SqlValue::String(kind.into())),
                        ("SEQUENCE".into(), SqlValue::Int(index as i64 + 1)),
                        ("ATTRIBUTE".into(), SqlValue::String("ERROR".into())),
                        ("TEXT".into(), SqlValue::String(text)),
                        (
                            "MESSAGE_NUMBER".into(),
                            SqlValue::Int(if index == 0 { 201 } else { 0 }),
                        ),
                    ]));
                }
            }
        } else {
            return Err(unsupported("unknown Oracle routine dictionary relation"));
        }
    }
    Ok(rows)
}

/// Resolve the public routine dictionary only after local relations and CTEs.
pub(super) fn lower<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    schema: &str,
) -> Result<()> {
    struct Resolve<'a> {
        db: &'a BicDb,
        schema: &'a str,
        ctes: Vec<BTreeSet<String>>,
    }
    impl sqlparser::ast::VisitorMut for Resolve<'_> {
        type Break = SqlError;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            self.ctes.push(
                query
                    .with
                    .as_ref()
                    .map(|with| {
                        with.cte_tables
                            .iter()
                            .map(|cte| ident_value(&cte.alias.name))
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<SqlError> {
            self.ctes.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<SqlError> {
            let TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            let parts = object_name_parts(name);
            let (local, table) = match parts.as_slice() {
                [table]
                    if matches!(table.as_str(), "USER_OBJECTS" | "USER_ERRORS")
                        && !self.ctes.iter().rev().any(|scope| scope.contains(table)) =>
                {
                    (vec![self.schema.to_string(), table.clone()], table.clone())
                }
                [owner, table]
                    if owner == "SYS"
                        && matches!(table.as_str(), "USER_OBJECTS" | "USER_ERRORS") =>
                {
                    (parts.clone(), table.clone())
                }
                _ => return ControlFlow::Continue(()),
            };
            let local = match relation_name_from_parts(&local) {
                Ok(local) => local,
                Err(error) => return ControlFlow::Break(error),
            };
            if resolve_relation_name_memoized(self.db, &local).is_some() {
                return ControlFlow::Continue(());
            }
            if alias.is_none() {
                *alias = Some(TableAlias {
                    explicit: true,
                    name: Ident::with_quote('"', &table),
                    columns: Vec::new(),
                    at: None,
                });
            }
            let target = if table == "USER_OBJECTS" {
                "bicdb_oracle_user_objects"
            } else {
                "bicdb_oracle_user_errors"
            };
            *name = ObjectName::from(vec![
                Ident::with_quote('"', "pg_catalog"),
                Ident::with_quote('"', target),
            ]);
            ControlFlow::Continue(())
        }
    }
    match sqlparser::ast::VisitMut::visit(
        node,
        &mut Resolve {
            db,
            schema,
            ctes: Vec::new(),
        },
    ) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

fn object_row(name: String, kind: &str, status: &str) -> BTreeMap<String, SqlValue> {
    BTreeMap::from([
        ("OBJECT_NAME".into(), SqlValue::String(name)),
        ("OBJECT_TYPE".into(), SqlValue::String(kind.into())),
        ("STATUS".into(), SqlValue::String(status.into())),
    ])
}
fn owned_objects(db: &BicDb, role: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = relation_schemas(db)?;
    let views = list_views(db)?
        .into_iter()
        .map(|view| (view.name, view.materialized))
        .collect::<BTreeMap<_, _>>();
    let owners = schemas
        .iter()
        .map(|schema| {
            (
                schema.name.clone(),
                schema.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for schema in &schemas {
        if schema.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME) != role
            || matches!(
                schema.schema_name.as_str(),
                "pg_catalog" | "information_schema"
            )
        {
            continue;
        }
        let kind = match views.get(&schema.name) {
            Some(true) => "MATERIALIZED VIEW",
            Some(false) => "VIEW",
            None => "TABLE",
        };
        rows.push(object_row(
            catalog_display_name(&schema.name),
            kind,
            "VALID",
        ));
        if !schema.has_hidden_primary_key() && schema.primary_key_column().is_some() {
            rows.push(object_row(
                catalog_display_name(&schema.primary_key_constraint_name()),
                "INDEX",
                "VALID",
            ));
        }
    }
    for index in catalog_indexes(&schemas, db) {
        if owners.get(&index.collection).copied() == Some(role) {
            rows.push(object_row(index.name, "INDEX", "VALID"));
        }
    }
    for sequence in list_sequences(db)? {
        if sequence.owner == role {
            rows.push(object_row(
                sequence_catalog_name(&sequence.name),
                "SEQUENCE",
                "VALID",
            ));
        }
    }
    for user_type in list_user_types(db)? {
        if user_type.owner == role {
            rows.push(object_row(user_type.name, "TYPE", "VALID"));
        }
    }
    for trigger in list_triggers(db)? {
        if owners.get(&trigger.table_name).copied() == Some(role) {
            rows.push(object_row(trigger.name, "TRIGGER", "VALID"));
        }
    }
    Ok(rows)
}
