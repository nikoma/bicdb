//! Named Oracle collection metadata, with native array transport carriers.
use super::*;
mod methods;
pub(crate) mod multisets;
pub(crate) use methods::*;

thread_local! { static ROUTINE_CONSTRUCTORS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct RoutineScope(bool);
impl RoutineScope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ROUTINE_CONSTRUCTORS.with(|flag| flag.replace(enabled)))
    }
}
impl Drop for RoutineScope {
    fn drop(&mut self) {
        ROUTINE_CONSTRUCTORS.with(|flag| flag.set(self.0));
    }
}
pub(crate) fn routine_constructors_enabled() -> bool {
    ROUTINE_CONSTRUCTORS.with(std::cell::Cell::get)
}

pub(crate) fn bind_routine_type_name(name: &mut String, schema: &str) {
    if routine_constructors_enabled() && pg_type_oid_by_name(name).is_none() && !name.contains('.')
    {
        *name = format!("{schema}.{name}");
    }
}

pub(crate) fn bind_declaration_type(declaration: &mut RoutineDecl, schema: &str) {
    if let RoutineDecl::Variable {
        pg_type: Some(name),
        ..
    } = declaration
    {
        bind_routine_type_name(name, schema);
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CollectionKind {
    NestedTable,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CollectionSchema {
    pub(crate) kind: CollectionKind,
    pub(crate) element: RoutineTypeSchema,
}

pub(crate) fn prepare_save(db: &mut BicDb, user_type: &UserTypeSchema) -> Result<()> {
    if let UserTypeKind::Domain {
        oracle_collection: Some(collection),
        base_type,
        base_user_type,
        type_modifier,
        ..
    } = &user_type.kind
    {
        if base_user_type.is_some()
            || type_modifier.is_some()
            || *base_type != format!("{}[]", collection.element.pg_type)
            || collection.element.pg_type.ends_with("[]")
            || pg_type_oid_by_name(&collection.element.pg_type).is_none()
        {
            return Err(unsupported("invalid Oracle collection element metadata"));
        }
        enable_metadata_v5(db)?;
    }
    Ok(())
}

/// Parse only the canonical collection declaration produced by the preview
/// lowering. Native PostgreSQL never enters this grammar.
pub(crate) fn declaration(
    db: &BicDb,
    sql: &str,
) -> Result<Option<(sqlparser::ast::CreateDomain, CollectionSchema)>> {
    // Avoid tokenizing ordinary queries, and retain the normal parser bounds
    // before entering this additional grammar.
    if ![b"CREATE".as_slice(), b"TYPE", b"TABLE"]
        .iter()
        .all(|keyword| {
            sql.as_bytes()
                .windows(keyword.len())
                .any(|word| word.eq_ignore_ascii_case(keyword))
        })
    {
        return Ok(None);
    }
    reject_oversized_sql(sql)?;
    reject_deep_parse_nesting(sql)?;
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    let source = tokens(sql)?;
    let source = source.strip_suffix(&[Token::SemiColon]).unwrap_or(&source);
    if source.len() < 7 || !word(&source[0], "CREATE") || !word(&source[1], "TYPE") {
        return Ok(None);
    }
    let Some(boundary) = source
        .windows(3)
        .position(|w| word(&w[0], "AS") && word(&w[1], "TABLE") && word(&w[2], "OF"))
    else {
        return Ok(None);
    };
    let name = join(&source[2..boundary]);
    let element = join(&source[boundary + 3..]);
    let parsed = parse_statements(&format!("CREATE DOMAIN {name} AS {element}"))?;
    let [Statement::CreateDomain(domain)] = parsed.as_slice() else {
        return Err(unsupported("invalid collection declaration"));
    };
    if domain.default.is_some() || domain.collation.is_some() || !domain.constraints.is_empty() {
        return Err(unsupported(
            "collection elements cannot declare domain constraints",
        ));
    }
    let (pg_type, _) = pg_type_from_data_type(&domain.data_type)?;
    if !matches!(pg_type.as_str(), "varchar" | "bpchar" | "numeric") {
        return Err(unsupported("collection element type is not yet supported"));
    }
    let type_modifier = character_columns::modifier(db, &domain.data_type)?
        .or(pg_type_modifier_from_data_type(&domain.data_type)?);
    let collection = CollectionSchema {
        kind: CollectionKind::NestedTable,
        element: RoutineTypeSchema {
            pg_type: pg_type.clone(),
            type_modifier,
        },
    };
    let physical = parse_statements(&format!("CREATE DOMAIN {name} AS {pg_type}[]"))?;
    let [Statement::CreateDomain(domain)] = physical.as_slice() else {
        unreachable!()
    };
    Ok(Some((domain.clone(), collection)))
}

pub(super) fn lower_declaration(
    db: &BicDb,
    source: &[Token],
    namespace: Option<&str>,
) -> Result<Option<String>> {
    if source.len() < 7 || !word(&source[0], "CREATE") || !word(&source[1], "TYPE") {
        return Ok(None);
    }
    let Some(boundary) = source
        .windows(3)
        .position(|w| word(&w[0], "AS") && word(&w[1], "TABLE") && word(&w[2], "OF"))
    else {
        return Ok(None);
    };
    let name = canonical_tokens_with_routine_types(
        db,
        &source[2..boundary],
        true,
        false,
        false,
        namespace,
    )?;
    let mut name = join(&name);
    if !source[2..boundary].contains(&Token::Period) {
        if let Some(namespace) = namespace {
            name = format!("\"{}\".{name}", namespace.replace('"', "\"\""));
        }
    }
    let element = canonical_tokens_with_routine_types(
        db,
        &source[boundary + 3..],
        true,
        false,
        false,
        namespace,
    )?;
    let sql = format!("CREATE TYPE {name} AS TABLE OF {}", join(&element));
    declaration(db, &sql)?.ok_or_else(|| unsupported("invalid collection declaration"))?;
    Ok(Some(sql))
}

/// Only registered Oracle collection types introduce constructor calls.
/// Ordinary PostgreSQL domains keep their existing function resolution.
pub(super) fn constructor_expression(
    db: &BicDb,
    expr: &mut Expr,
    namespace: Option<&str>,
) -> Result<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    let resolved = (|| -> Result<Option<Expr>> {
        let parts = object_name_parts(&function.name);
        let (schema, name) = match parts.as_slice() {
            [name] => (namespace.unwrap_or("public"), name.as_str()),
            [schema, name] => (schema.as_str(), name.as_str()),
            _ => return Ok(None),
        };
        let Some(user_type) = load_user_type(db, schema, name)? else {
            return Ok(None);
        };
        let UserTypeKind::Domain {
            oracle_collection: Some(collection),
            ..
        } = &user_type.kind
        else {
            return Ok(None);
        };
        let FunctionArguments::List(list) = &function.args else {
            return Ok(None);
        };
        if list.duplicate_treatment.is_some()
            || !list.clauses.is_empty()
            || function.filter.is_some()
            || function.over.is_some()
        {
            return Err(unsupported(
                "collection constructors require positional scalar elements",
            ));
        }
        let args = list
            .args
            .iter()
            .map(|arg| {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = arg else {
                    return Err(unsupported(
                        "collection constructors require positional scalar elements",
                    ));
                };
                Ok(if collection.element.pg_type == "numeric" {
                    format!(
                        "CAST({} AS NUMERIC)",
                        date_trunc::exact_numeric_operand_sql(arg)
                    )
                } else {
                    render_sql(arg)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let name = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\"")
        );
        expression(&format!("CAST(ARRAY[{}] AS {name})", args.join(","))).map(Some)
    })()?;
    if let Some(lowered) = resolved {
        *expr = lowered;
    }
    Ok(())
}

pub(super) fn lower_constructors<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    namespace: Option<&str>,
) -> Result<()> {
    let result = visit_expressions_mut(node, |expr| {
        match constructor_expression(db, expr, namespace) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    });
    if let ControlFlow::Break(error) = result {
        return Err(error);
    }
    Ok(())
}

pub(crate) fn coerce(value: SqlValue, collection: &CollectionSchema) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    require_one_dimensional_array(
        &value,
        "2202E",
        "nested tables require one-dimensional elements",
    )?;
    let Some((values, bounds)) = nullable_array_parts(&value, "collection")? else {
        return Ok(SqlValue::Null);
    };
    if bounds.iter().any(|bound| *bound != 1) {
        return Err(unsupported("Oracle collection indices start at one"));
    }
    let values = values
        .into_iter()
        .map(|value| {
            apply_pg_type_modifier_with_context(
                value,
                &collection.element.pg_type,
                collection.element.type_modifier.as_ref(),
                false,
            )
            .map_err(|error| {
                if matches!(
                    collection.element.type_modifier,
                    Some(PgTypeModifier::OracleCharacterV1 { .. })
                ) {
                    errors::database_error(
                        -22814,
                        "22001",
                        "collection element exceeds declared length",
                    )
                } else {
                    error
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(array_value(values))
}

impl SqlSession<'_> {
    pub(crate) fn execute_raw_oracle_collection(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((domain, collection)) = declaration(self.db_ref(), sql)? else {
            return Ok(None);
        };
        self.execute_create_domain_with_collection(&domain, Some(collection))?;
        Ok(Some(SqlResult::command("CREATE TYPE")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn collection_declarations_register_logical_elements_and_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE SCHEMA app")
            .unwrap();
        for source in [
            "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)",
            "CREATE TYPE amounts AS TABLE OF NUMBER(9,2)",
            "CREATE TYPE \"MiXeD\" AS TABLE OF VARCHAR2(4 CHAR)",
        ] {
            let sql = lower_session_sql(
                &db,
                source,
                ORACLE_PREVIEW_PROFILE,
                Some("\"app\""),
                "postgres",
            )
            .unwrap();
            assert!(infer_parameter_types(&db, &sql).unwrap().is_empty());
            assert!(infer_query_result_columns_for_describe(&db, &sql)
                .unwrap()
                .is_none());
            SqlSession::new(&mut db).execute(&sql).unwrap();
            assert!(
                SqlSession::new(&mut db).execute(&sql).is_err(),
                "duplicate must fail"
            );
        }
        let labels = load_user_type(&db, "app", "LABELS").unwrap().unwrap();
        let UserTypeKind::Domain {
            base_type,
            oracle_collection: Some(collection),
            ..
        } = labels.kind
        else {
            panic!("missing collection metadata")
        };
        assert_eq!(base_type, "varchar[]");
        assert_eq!(
            collection.element.type_modifier,
            Some(PgTypeModifier::OracleCharacterV1 {
                fixed: false,
                length: 8,
                unit: character_columns::Unit::Bytes,
            })
        );
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(load_user_type(&db, "app", "MiXeD").unwrap().is_some());
        SqlSession::new(&mut db)
            .execute("DROP TYPE app.\"LABELS\"")
            .unwrap();
        assert!(load_user_type(&db, "app", "LABELS").unwrap().is_none());
    }

    #[test]
    fn constructors_preserve_null_empty_unicode_and_enforce_element_bounds() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE TYPE texts AS TABLE OF VARCHAR2(8 BYTE)",
            "CREATE TYPE chars AS TABLE OF VARCHAR2(3 CHAR)",
            "CREATE TYPE amounts AS TABLE OF NUMBER(9,2)",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for (source, expected) in [
            ("SELECT texts() FROM DUAL", vec![]),
            ("SELECT texts(NULL) FROM DUAL", vec![SqlValue::Null]),
            ("SELECT texts('') FROM DUAL", vec![SqlValue::Null]),
            (
                "SELECT texts('é','😀') FROM DUAL",
                vec![SqlValue::String("é".into()), SqlValue::String("😀".into())],
            ),
            (
                "SELECT chars('😀😀😀') FROM DUAL",
                vec![SqlValue::String("😀😀😀".into())],
            ),
            (
                "SELECT amounts(1.235,-3,NULL) FROM DUAL",
                vec![
                    SqlValue::String("1.24".into()),
                    SqlValue::String("-3.00".into()),
                    SqlValue::Null,
                ],
            ),
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db).execute(&sql).unwrap();
            assert_eq!(
                nullable_array_values(&result.rows[0][0], "test")
                    .unwrap()
                    .unwrap(),
                expected,
                "{source} -> {sql}"
            );
        }
        let sql = lower_profile_sql(
            &db,
            "SELECT CAST(NULL AS texts) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0],
            SqlValue::Null
        );
        let sql = lower_profile_sql(
            &db,
            "SELECT texts('😀😀😀') FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let error = SqlSession::new(&mut db).execute(&sql).unwrap_err();
        assert!(format!("{error:?}").contains("22814"), "{error:?}");
    }

    #[test]
    fn native_domains_remain_untagged_and_collection_grammar_is_preview_only() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let sql = "CREATE TYPE labels AS TABLE OF VARCHAR(8)";
        assert!(declaration(&db, sql).unwrap().is_none());
        assert!(SqlSession::new(&mut db).execute(sql).is_err());
        SqlSession::new(&mut db)
            .execute("CREATE DOMAIN labels AS VARCHAR(8)[]")
            .unwrap();
        let native = load_user_type(&db, "public", "labels").unwrap().unwrap();
        assert!(!serde_json::to_string(&native)
            .unwrap()
            .contains("oracle_collection"));
        assert!(!metadata_v5_enabled(&db).unwrap());
    }

    #[test]
    fn collection_profile_fence_preserves_existing_capabilities_without_downgrade() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        enable_metadata_v5(&mut db).unwrap();
        enable_metadata_v2(&mut db).unwrap();
        enable_metadata_v3(&mut db).unwrap();
        prepare_hierarchy_save(&mut db, "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=2").unwrap();
        assert_eq!(
            db.get(PROFILE_COLLECTION, "profile")
                .unwrap()
                .unwrap()
                .metadata,
            metadata_v5()
        );
        assert!(!matches_profile(&metadata_v5(), POSTGRES_PROFILE));
        assert!(!matches_profile(&metadata_v5(), ORACLE_HR_PROFILE));
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(metadata_v5_enabled(&db).unwrap());
    }
}
