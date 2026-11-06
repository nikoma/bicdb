//! Read-only mapping of native reusable logical page runs to a storage catalog.
//! This is not host filesystem capacity, WAL space, or Oracle file-format data.
use super::*;

pub(crate) const RELATION: &str = "pg_catalog.bicdb_oracle_free_space";
pub(crate) const COLUMNS: &[(&str, &str)] = &[
    ("TABLESPACE_NAME", "varchar"),
    ("FILE_ID", "numeric"),
    ("BLOCK_ID", "numeric"),
    ("BYTES", "numeric"),
    ("BLOCKS", "numeric"),
    ("RELATIVE_FNO", "numeric"),
];

pub(crate) fn is_relation(name: &str) -> bool {
    name == RELATION
}

pub(crate) fn can_select(db: &BicDb, role: &str) -> Result<bool> {
    if role == BOOTSTRAP_ROLE_NAME || load_role_schema(db, role)?.is_some_and(|role| role.superuser)
    {
        return Ok(true);
    }
    let inherited = role_privilege_closure(db, role)?;
    Ok(list_privileges(db)?.iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Table
            && grant.object_name == RELATION
            && grant.column.is_none()
            && grant.privilege.eq_ignore_ascii_case("SELECT")
            && (grant.grantee.eq_ignore_ascii_case("public")
                || inherited.contains(&normalize_role_name(&grant.grantee)))
    }))
}

pub(super) fn native_privilege_objects(objects: &Option<GrantObjects>) -> bool {
    matches!(objects, Some(GrantObjects::Tables(names)) if names.len() == 1 && relation_name(&names[0]).is_ok_and(|name| is_relation(&name)))
}

/// Resolve the public dictionary name only after checking local relations and
/// CTEs. Explicit application-schema references never become system catalogs.
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
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
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
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<Self::Break> {
            self.ctes.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
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
            let local = match parts.as_slice() {
                [table]
                    if table == "DBA_FREE_SPACE"
                        && !self.ctes.iter().rev().any(|ctes| ctes.contains(table)) =>
                {
                    vec![self.schema.to_string(), table.clone()]
                }
                [owner, table] if owner == "SYS" && table == "DBA_FREE_SPACE" => parts.clone(),
                _ => return ControlFlow::Continue(()),
            };
            let local = match relation_name_from_parts(&local) {
                Ok(name) => name,
                Err(error) => return ControlFlow::Break(error),
            };
            if resolve_relation_name_memoized(self.db, &local).is_some() {
                return ControlFlow::Continue(());
            }
            if alias.is_none() {
                *alias = Some(TableAlias {
                    explicit: true,
                    name: Ident::with_quote('"', "DBA_FREE_SPACE"),
                    columns: Vec::new(),
                    at: None,
                });
            }
            *name = ObjectName::from(vec![
                Ident::with_quote('"', "pg_catalog"),
                Ident::with_quote('"', "bicdb_oracle_free_space"),
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

impl SqlEngine<'_> {
    pub(crate) fn storage_free_space_rows(&self) -> Result<Vec<BTreeMap<String, SqlValue>>> {
        let role = current_user_from_gucs(&self.session_gucs);
        if !can_select(self.db_ref(), &role)? {
            return Err(SqlError::BicDb(BicDbError::Authorization(
                "SELECT on storage inventory requires an explicit monitoring grant".into(),
            )));
        }
        self.check_cancellation()?;
        let handle = self
            .db_ref()
            .paged_storage_ops_handle()
            .ok_or_else(|| unsupported("storage extent inventory requires server_paged storage"))?;
        fn limit(
            gucs: &HashMap<String, String>,
            key: &str,
            default: u64,
            maximum: u64,
        ) -> Result<u64> {
            match gucs.get(key) {
                None => Ok(default),
                Some(value) => value
                    .parse::<u64>()
                    .ok()
                    .filter(|v| *v > 0 && *v <= maximum)
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!("{key} must be between 1 and {maximum}"))
                    }),
            }
        }
        let pages = limit(
            &self.session_gucs,
            "bicdb.storage_inventory_max_pages",
            8192,
            65536,
        )?;
        let milliseconds = limit(
            &self.session_gucs,
            "bicdb.storage_inventory_timeout_ms",
            1000,
            10000,
        )?;
        let bytes = limit(
            &self.session_gucs,
            "bicdb.storage_inventory_max_read_bytes",
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        )?;
        let operation = self.cancellation.child_with_deadline(
            std::time::Instant::now() + std::time::Duration::from_millis(milliseconds),
        );
        let inventory =
            handle.free_page_extents_while(pages, bytes, || operation.check().is_ok())?;
        operation.check()?;
        let inventory = inventory.ok_or_else(|| {
            SqlError::ResourceLimit {
                sqlstate: "54000",
                message: format!(
                    "complete storage inventory exceeds configured admission limits ({pages} pages, {bytes} read bytes)"
                ),
            }
        })?;
        let mut rows = Vec::new();
        for extent in inventory.extents {
            self.check_cancellation()?;
            let integer = |value: u64| {
                let value = i64::try_from(value).map_err(|_| {
                    SqlError::numeric_value_out_of_range(
                        "native storage extent exceeds signed file-offset range",
                    )
                })?;
                // NUMBER/numeric uses the exact decimal carrier throughout
                // the engine. An Int carrier would silently select integer
                // division and the wrong aggregate/arithmetic overloads.
                Ok::<_, SqlError>(SqlValue::String(value.to_string()))
            };
            let bytes = extent
                .pages
                .checked_mul(u64::from(inventory.page_size))
                .ok_or_else(|| {
                    SqlError::numeric_value_out_of_range("storage extent byte count overflow")
                })?;
            rows.push(virtual_row([
                ("TABLESPACE_NAME", SqlValue::String("BICDB_PAGED".into())),
                ("FILE_ID", integer(1)?),
                ("BLOCK_ID", integer(extent.start_page)?),
                ("BYTES", integer(bytes)?),
                ("BLOCKS", integer(extent.pages)?),
                ("RELATIVE_FNO", integer(1)?),
            ]));
        }
        Ok(rows)
    }
}
