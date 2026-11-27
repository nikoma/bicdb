//! Foreign-data wrappers, foreign servers and user mappings: their catalog,
//! generic option lists, USAGE privileges, and the catalog rows
//! (`pg_foreign_data_wrapper`, `pg_foreign_server`, `pg_user_mapping`,
//! `pg_user_mappings` and the information schema views) that describe them.
//!
//! BicDB has no foreign-data wrapper handlers, so these objects are catalog
//! entries only: they can be created, altered, granted, commented on and
//! dropped as in PostgreSQL, but nothing reads remote data through them.
use crate::*;

pub(crate) const FOREIGN_DATA_COLLECTION: &str = "__bicdb_pg_foreign_data";
const CATALOG_KEY: &str = "catalog";

pub(crate) const POSTGRESQL_FDW_VALIDATOR: &str = "postgresql_fdw_validator";
pub(crate) const POSTGRESQL_FDW_VALIDATOR_OID: i64 = 2316;

/// `pg_class` OIDs of the catalogs, used as `classoid`/`tableoid`.
pub(crate) const WRAPPER_CATALOG_OID: i64 = 2328;
pub(crate) const SERVER_CATALOG_OID: i64 = 1417;
pub(crate) const USER_MAPPING_CATALOG_OID: i64 = 1418;

/// One `option 'value'` of an OPTIONS list, in declaration order.
pub(crate) type GenericOption = (String, String);

/// One entry of an object's access list: `grantee=U[*]/grantor`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AclItem {
    /// `None` is PUBLIC.
    pub(crate) grantee: Option<String>,
    pub(crate) grantor: String,
    pub(crate) grant_option: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ForeignDataWrapper {
    pub(crate) name: String,
    pub(crate) oid: i64,
    pub(crate) owner: String,
    #[serde(default)]
    pub(crate) validator: Option<String>,
    #[serde(default)]
    pub(crate) options: Vec<GenericOption>,
    /// `None` is the default access list: the owner holds USAGE.
    #[serde(default)]
    pub(crate) acl: Option<Vec<AclItem>>,
    #[serde(default)]
    pub(crate) comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ForeignServer {
    pub(crate) name: String,
    pub(crate) oid: i64,
    pub(crate) owner: String,
    pub(crate) wrapper: String,
    #[serde(default)]
    pub(crate) server_type: Option<String>,
    #[serde(default)]
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) options: Vec<GenericOption>,
    #[serde(default)]
    pub(crate) acl: Option<Vec<AclItem>>,
    #[serde(default)]
    pub(crate) comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UserMapping {
    pub(crate) oid: i64,
    /// `None` is PUBLIC.
    pub(crate) role: Option<String>,
    pub(crate) server: String,
    #[serde(default)]
    pub(crate) options: Vec<GenericOption>,
}

impl UserMapping {
    /// The role as PostgreSQL's messages name it.
    pub(crate) fn role_display(&self) -> &str {
        self.role.as_deref().unwrap_or("public")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ForeignCatalog {
    #[serde(default)]
    pub(crate) wrappers: Vec<ForeignDataWrapper>,
    #[serde(default)]
    pub(crate) servers: Vec<ForeignServer>,
    #[serde(default)]
    pub(crate) mappings: Vec<UserMapping>,
    #[serde(default)]
    pub(crate) next_oid: i64,
}

impl ForeignCatalog {
    pub(crate) fn wrapper(&self, name: &str) -> Option<&ForeignDataWrapper> {
        self.wrappers.iter().find(|wrapper| wrapper.name == name)
    }

    /// The wrapper to update. Like an updated heap row, it moves to the end
    /// of the catalog's scan order.
    pub(crate) fn wrapper_mut(&mut self, name: &str) -> Option<&mut ForeignDataWrapper> {
        let index = self
            .wrappers
            .iter()
            .position(|wrapper| wrapper.name == name)?;
        let wrapper = self.wrappers.remove(index);
        self.wrappers.push(wrapper);
        self.wrappers.last_mut()
    }

    pub(crate) fn server(&self, name: &str) -> Option<&ForeignServer> {
        self.servers.iter().find(|server| server.name == name)
    }

    /// The server to update; it moves to the end of the scan order.
    pub(crate) fn server_mut(&mut self, name: &str) -> Option<&mut ForeignServer> {
        let index = self.servers.iter().position(|server| server.name == name)?;
        let server = self.servers.remove(index);
        self.servers.push(server);
        self.servers.last_mut()
    }

    pub(crate) fn mapping(&self, role: Option<&str>, server: &str) -> Option<&UserMapping> {
        self.mappings
            .iter()
            .find(|mapping| mapping.role.as_deref() == role && mapping.server == server)
    }

    /// A fresh OID for a new wrapper, server or user mapping.
    pub(crate) fn allocate_oid(&mut self, kind: &str) -> i64 {
        loop {
            self.next_oid += 1;
            let oid = named_relation_oid(&format!("foreign-data:{kind}:{}", self.next_oid));
            let taken = self.wrappers.iter().any(|wrapper| wrapper.oid == oid)
                || self.servers.iter().any(|server| server.oid == oid)
                || self.mappings.iter().any(|mapping| mapping.oid == oid);
            if !taken {
                return oid;
            }
        }
    }
}

thread_local! {
    static CATALOG_CACHE: RefCell<Option<(u64, u64, Arc<ForeignCatalog>)>> =
        const { RefCell::new(None) };
}

pub(crate) fn load_foreign_catalog(db: &BicDb) -> Result<Arc<ForeignCatalog>> {
    let database = db.instance_id();
    let generation = db.collection_generation(FOREIGN_DATA_COLLECTION);
    if let Some(catalog) = CATALOG_CACHE.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, catalog)| {
            (*id == database && *cached == generation).then(|| catalog.clone())
        })
    }) {
        return Ok(catalog);
    }
    let records = match db.scan_collection(FOREIGN_DATA_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let catalog = match records.into_iter().find(|record| record.id == CATALOG_KEY) {
        Some(record) => serde_json::from_value(record.metadata)?,
        None => ForeignCatalog::default(),
    };
    let catalog = Arc::new(catalog);
    CATALOG_CACHE.with(|cache| *cache.borrow_mut() = Some((database, generation, catalog.clone())));
    Ok(catalog)
}

pub(crate) fn save_foreign_catalog(db: &mut BicDb, catalog: &ForeignCatalog) -> Result<()> {
    db.create_collection(FOREIGN_DATA_COLLECTION)?;
    db.insert(
        FOREIGN_DATA_COLLECTION,
        Record::new(CATALOG_KEY).with_metadata(serde_json::to_value(catalog)?),
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Generic options
// ---------------------------------------------------------------------------

/// What an OPTIONS list item asks for: plain and ADD add, SET replaces and
/// DROP removes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OptionAction {
    Add,
    Set,
    Drop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OptionChange {
    pub(crate) action: OptionAction,
    pub(crate) name: String,
    pub(crate) value: Option<String>,
}

/// The catalog an option list belongs to, which decides what a validator
/// accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OptionContext {
    Wrapper,
    Server,
    UserMapping,
    ForeignTable,
    Column,
}

/// PostgreSQL's `transformGenericOptions`: applies `changes` to `current`
/// and checks the result with the wrapper's validator.
pub(crate) fn apply_option_changes(
    current: &[GenericOption],
    changes: &[OptionChange],
    context: OptionContext,
    validator: Option<&str>,
) -> Result<Vec<GenericOption>> {
    let mut result = current.to_vec();
    for change in changes {
        if change.name.contains('=') {
            return Err(SqlError::data_exception_public(
                "22023",
                format!(
                    "invalid option name \"{}\": must not contain \"=\"",
                    change.name
                ),
                None,
            ));
        }
        let position = result.iter().position(|(name, _)| *name == change.name);
        match (change.action, position) {
            (OptionAction::Drop, Some(index)) => {
                result.remove(index);
            }
            (OptionAction::Set, Some(index)) => {
                result[index].1 = change.value.clone().unwrap_or_default();
            }
            (OptionAction::Drop | OptionAction::Set, None) => {
                return Err(SqlError::data_exception_public(
                    "42704",
                    format!("option \"{}\" not found", change.name),
                    None,
                ));
            }
            (OptionAction::Add, Some(_)) => {
                return Err(SqlError::data_exception_public(
                    "42710",
                    format!("option \"{}\" provided more than once", change.name),
                    None,
                ));
            }
            (OptionAction::Add, None) => {
                result.push((
                    change.name.clone(),
                    change.value.clone().unwrap_or_default(),
                ));
            }
        }
    }
    if let Some(validator) = validator {
        validate_options(validator, &result, context)?;
    }
    Ok(result)
}

/// The options `postgresql_fdw_validator` accepts, with the catalog each
/// belongs to.
const LIBPQ_OPTIONS: &[(&str, OptionContext)] = &[
    ("authtype", OptionContext::Server),
    ("service", OptionContext::Server),
    ("user", OptionContext::UserMapping),
    ("password", OptionContext::UserMapping),
    ("connect_timeout", OptionContext::Server),
    ("dbname", OptionContext::Server),
    ("host", OptionContext::Server),
    ("hostaddr", OptionContext::Server),
    ("port", OptionContext::Server),
    ("tty", OptionContext::Server),
    ("options", OptionContext::Server),
    ("requiressl", OptionContext::Server),
    ("sslmode", OptionContext::Server),
    ("gsslib", OptionContext::Server),
    ("gssdelegation", OptionContext::Server),
];

fn validate_options(
    validator: &str,
    options: &[GenericOption],
    context: OptionContext,
) -> Result<()> {
    if validator != POSTGRESQL_FDW_VALIDATOR {
        return Err(SqlError::Unsupported(format!(
            "foreign-data wrapper validator {validator} is not supported"
        )));
    }
    for (name, _) in options {
        if LIBPQ_OPTIONS
            .iter()
            .any(|(option, option_context)| option == name && *option_context == context)
        {
            continue;
        }
        let candidates = LIBPQ_OPTIONS
            .iter()
            .filter(|(_, option_context)| *option_context == context)
            .map(|(option, _)| *option)
            .collect::<Vec<_>>();
        let hint = if candidates.is_empty() {
            Some("There are no valid options in this context.".to_string())
        } else {
            closest_match(name, &candidates)
                .map(|candidate| format!("Perhaps you meant the option \"{candidate}\"."))
        };
        return Err(SqlError::coded(
            "42601",
            format!("invalid option \"{name}\""),
            None,
            hint,
        ));
    }
    Ok(())
}

/// PostgreSQL's `getClosestMatch` with a maximum distance of 4: the nearest
/// candidate no further than half the source's length away.
fn closest_match<'a>(source: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(usize, &str)> = None;
    if source.is_empty() {
        return None;
    }
    for candidate in candidates {
        let distance = levenshtein(source.as_bytes(), candidate.as_bytes());
        if distance <= 4
            && distance <= source.len() / 2
            && best.is_none_or(|(best_distance, _)| distance < best_distance)
        {
            best = Some((distance, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}

fn levenshtein(left: &[u8], right: &[u8]) -> usize {
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    for (i, left_byte) in left.iter().enumerate() {
        let mut current = vec![i + 1; right.len() + 1];
        for (j, right_byte) in right.iter().enumerate() {
            let substitution = previous[j] + usize::from(left_byte != right_byte);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        previous = current;
    }
    previous[right.len()]
}

/// `{"name=value",...}`, the text[] spelling of an option list, or NULL.
pub(crate) fn options_value(options: &[GenericOption]) -> SqlValue {
    if options.is_empty() {
        return SqlValue::Null;
    }
    text_array_value(
        options
            .iter()
            .map(|(name, value)| format!("{name}={value}")),
    )
}

pub(crate) fn text_array_value(items: impl IntoIterator<Item = String>) -> SqlValue {
    let elements = items
        .into_iter()
        .map(|item| array_element_literal(&item))
        .collect::<Vec<_>>();
    SqlValue::String(format!("{{{}}}", elements.join(",")))
}

fn array_element_literal(item: &str) -> String {
    let needs_quotes = item.is_empty()
        || item.eq_ignore_ascii_case("null")
        || item
            .chars()
            .any(|ch| matches!(ch, '{' | '}' | '"' | ',' | '\\') || ch.is_whitespace());
    if !needs_quotes {
        return item.to_string();
    }
    let mut quoted = String::with_capacity(item.len() + 2);
    quoted.push('"');
    for ch in item.chars() {
        if matches!(ch, '"' | '\\') {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

pub(crate) const OPTIONS_TO_TABLE_COLUMNS: [&str; 2] = ["option_name", "option_value"];

pub(crate) fn is_options_to_table_function(name: &str) -> bool {
    name.strip_prefix("pg_catalog.")
        .unwrap_or(name)
        .eq_ignore_ascii_case("pg_options_to_table")
}

/// The `option_name`/`option_value` rows of `pg_options_to_table(options)`.
pub(crate) fn options_to_table(value: &SqlValue) -> Result<Vec<(String, Option<String>)>> {
    let items = match value {
        SqlValue::Null => return Ok(Vec::new()),
        SqlValue::Json(json) if json.is_array() => json
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| match item {
                serde_json::Value::String(text) => Some(text.clone()),
                serde_json::Value::Null => None,
                other => Some(other.to_string()),
            })
            .collect::<Vec<_>>(),
        other => {
            let text = other.to_cell();
            parse_array_literal(&text)?
                .into_iter()
                .map(|item| match item {
                    serde_json::Value::String(text) => Some(text),
                    serde_json::Value::Null => None,
                    other => Some(other.to_string()),
                })
                .collect()
        }
    };
    Ok(items
        .into_iter()
        .flatten()
        .map(|item| match item.split_once('=') {
            Some((name, value)) => (name.to_string(), Some(value.to_string())),
            None => (item, None),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Privileges
// ---------------------------------------------------------------------------

/// The access list in effect: the stored one, or the owner's default USAGE.
pub(crate) fn effective_acl(acl: &Option<Vec<AclItem>>, owner: &str) -> Vec<AclItem> {
    acl.clone().unwrap_or_else(|| {
        vec![AclItem {
            grantee: Some(owner.to_string()),
            grantor: owner.to_string(),
            grant_option: false,
        }]
    })
}

fn role_is_superuser(db: &BicDb, role: &str) -> Result<bool> {
    pg_role_is_superuser(db, &normalize_role_name(role))
}

/// Whether `role` holds USAGE through the access list, directly, through a
/// role it inherits, or through PUBLIC. Superusers always do.
pub(crate) fn role_has_usage(
    db: &BicDb,
    role: &str,
    acl: &Option<Vec<AclItem>>,
    owner: &str,
) -> Result<bool> {
    if role_is_superuser(db, role)? {
        return Ok(true);
    }
    let roles = role_privilege_closure(db, role)?;
    Ok(effective_acl(acl, owner)
        .iter()
        .any(|item| match &item.grantee {
            None => true,
            Some(grantee) => roles.contains(&normalize_role_name(grantee)),
        }))
}

/// Whether `role` has the privileges of `owner` (is it, inherits it, or is a
/// superuser): PostgreSQL's ownership test.
pub(crate) fn role_is_owner(db: &BicDb, role: &str, owner: &str) -> Result<bool> {
    if role_is_superuser(db, role)? {
        return Ok(true);
    }
    Ok(role_privilege_closure(db, role)?.contains(&normalize_role_name(owner)))
}

/// PostgreSQL's `select_best_grantor`: the role `user` grants as. The owner
/// (for a superuser or a member of the owning role), else a held role that
/// has the grant option.
pub(crate) fn best_grantor(
    db: &BicDb,
    user: &str,
    acl: &Option<Vec<AclItem>>,
    owner: &str,
) -> Result<Option<String>> {
    if role_is_owner(db, user, owner)? {
        return Ok(Some(owner.to_string()));
    }
    let roles = role_privilege_closure(db, user)?;
    let user = normalize_role_name(user);
    let items = effective_acl(acl, owner);
    let holders = items
        .iter()
        .filter(|item| item.grant_option)
        .filter_map(|item| item.grantee.clone())
        .filter(|grantee| roles.contains(&normalize_role_name(grantee)))
        .collect::<Vec<_>>();
    Ok(holders
        .iter()
        .find(|holder| normalize_role_name(holder) == user)
        .or_else(|| holders.first())
        .cloned())
}

/// Grants USAGE (with or without the grant option) as `grantor`.
pub(crate) fn acl_grant(
    acl: &mut Option<Vec<AclItem>>,
    owner: &str,
    grantor: &str,
    grantees: &[Option<String>],
    grant_option: bool,
) {
    let mut items = effective_acl(acl, owner);
    for grantee in grantees {
        match items
            .iter_mut()
            .find(|item| item.grantee == *grantee && item.grantor == grantor)
        {
            Some(item) => item.grant_option |= grant_option,
            None => items.push(AclItem {
                grantee: grantee.clone(),
                grantor: grantor.to_string(),
                grant_option,
            }),
        }
    }
    *acl = Some(items);
}

/// Revokes USAGE (or only its grant option) that `grantor` gave. Returns
/// `Err` naming nothing when dependent grants exist without CASCADE.
pub(crate) fn acl_revoke(
    acl: &mut Option<Vec<AclItem>>,
    owner: &str,
    grantor: &str,
    grantees: &[Option<String>],
    grant_option_only: bool,
    cascade: bool,
) -> Result<()> {
    let mut items = effective_acl(acl, owner);
    for grantee in grantees {
        let Some(index) = items
            .iter()
            .position(|item| item.grantee == *grantee && item.grantor == grantor)
        else {
            continue;
        };
        let had_grant_option = items[index].grant_option;
        if had_grant_option {
            if let Some(grantee_name) = grantee {
                revoke_dependents(&mut items, grantee_name, cascade)?;
            }
        }
        if grant_option_only {
            items[index].grant_option = false;
        } else {
            items.remove(index);
        }
    }
    *acl = Some(items);
    Ok(())
}

/// Removes what `grantor` granted onward, recursively; without CASCADE any
/// such grant is an error.
fn revoke_dependents(items: &mut Vec<AclItem>, grantor: &str, cascade: bool) -> Result<()> {
    let dependents = items
        .iter()
        .filter(|item| item.grantor == grantor && item.grantee.as_deref() != Some(grantor))
        .cloned()
        .collect::<Vec<_>>();
    if dependents.is_empty() {
        return Ok(());
    }
    if !cascade {
        return Err(SqlError::coded(
            "2BP01",
            "dependent privileges exist",
            None,
            Some("Use CASCADE to revoke them too.".to_string()),
        ));
    }
    items.retain(|item| !(item.grantor == grantor && item.grantee.as_deref() != Some(grantor)));
    for dependent in dependents {
        if dependent.grant_option {
            if let Some(next) = dependent.grantee {
                revoke_dependents(items, &next, true)?;
            }
        }
    }
    Ok(())
}

/// PostgreSQL's `aclnewowner`: the old owner's entries become the new
/// owner's, merged with any the new owner already had.
pub(crate) fn acl_new_owner(acl: &mut Option<Vec<AclItem>>, old_owner: &str, new_owner: &str) {
    let Some(items) = acl.as_mut() else {
        return;
    };
    let mut merged: Vec<AclItem> = Vec::with_capacity(items.len());
    for mut item in items.drain(..) {
        if item.grantee.as_deref() == Some(old_owner) {
            item.grantee = Some(new_owner.to_string());
        }
        if item.grantor == old_owner {
            item.grantor = new_owner.to_string();
        }
        match merged
            .iter_mut()
            .find(|existing| existing.grantee == item.grantee && existing.grantor == item.grantor)
        {
            Some(existing) => existing.grant_option |= item.grant_option,
            None => merged.push(item),
        }
    }
    *items = merged;
}

/// The `aclitem[]` text of an access list, or NULL for the default one.
pub(crate) fn acl_value(acl: &Option<Vec<AclItem>>) -> SqlValue {
    match acl {
        None => SqlValue::Null,
        // An array value, so array functions (psql's `array_length(acl, 1)`)
        // read it as `aclitem[]`.
        Some(items) => SqlValue::Json(Arc::new(serde_json::Value::Array(
            items
                .iter()
                .map(|item| {
                    let grantee = item
                        .grantee
                        .as_deref()
                        .map(pg_quote_ident)
                        .unwrap_or_default();
                    format!(
                        "{grantee}=U{}/{}",
                        if item.grant_option { "*" } else { "" },
                        pg_quote_ident(&item.grantor)
                    )
                })
                .map(serde_json::Value::String)
                .collect(),
        ))),
    }
}

/// Whether `role` is named anywhere in the catalog: as an owner, in an
/// access list, or by a user mapping. Such a role cannot be dropped.
pub(crate) fn role_is_referenced(db: &BicDb, role: &str) -> Result<bool> {
    let catalog = load_foreign_catalog(db)?;
    let role = normalize_role_name(role);
    let named = |name: &str| normalize_role_name(name) == role;
    let in_acl = |acl: &Option<Vec<AclItem>>| {
        acl.as_ref().is_some_and(|items| {
            items
                .iter()
                .any(|item| named(&item.grantor) || item.grantee.as_deref().is_some_and(named))
        })
    };
    Ok(catalog
        .wrappers
        .iter()
        .any(|wrapper| named(&wrapper.owner) || in_acl(&wrapper.acl))
        || catalog
            .servers
            .iter()
            .any(|server| named(&server.owner) || in_acl(&server.acl))
        || catalog
            .mappings
            .iter()
            .any(|mapping| mapping.role.as_deref().is_some_and(named)))
}

/// `has_foreign_data_wrapper_privilege` and `has_server_privilege`.
pub(crate) fn has_privilege_value(
    db: &BicDb,
    function: &str,
    args: &[SqlValue],
    current_user: &str,
) -> Result<SqlValue> {
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let wrapper = function.ends_with("has_foreign_data_wrapper_privilege");
    let (role, object, privilege) = match args {
        [object, privilege] => (current_user.to_string(), object, privilege),
        [role, object, privilege] => (
            role_name_from_value(db, role)?.ok_or_else(|| SqlError::UndefinedRole {
                name: role.to_cell(),
            })?,
            object,
            privilege,
        ),
        _ => {
            return Err(SqlError::undefined_function(format!(
                "function {function} expects two or three arguments"
            )))
        }
    };
    let privilege_text = privilege.to_cell();
    for part in privilege_text.split(',') {
        let part = part.trim();
        let part = part
            .strip_suffix(" WITH GRANT OPTION")
            .or_else(|| part.strip_suffix(" with grant option"))
            .unwrap_or(part)
            .trim();
        if !part.eq_ignore_ascii_case("usage") {
            return Err(SqlError::data_exception_public(
                "22023",
                format!("unrecognized privilege type: \"{part}\""),
                None,
            ));
        }
    }
    let catalog = load_foreign_catalog(db)?;
    let (acl, owner) = if wrapper {
        let found = match object {
            SqlValue::Int(oid) => catalog.wrappers.iter().find(|wrapper| wrapper.oid == *oid),
            other => {
                let name = other.to_cell();
                Some(catalog.wrapper(&name).ok_or_else(|| {
                    SqlError::undefined_object(format!(
                        "foreign-data wrapper \"{name}\" does not exist"
                    ))
                })?)
            }
        };
        let Some(found) = found else {
            return Ok(SqlValue::Null);
        };
        (found.acl.clone(), found.owner.clone())
    } else {
        let found = match object {
            SqlValue::Int(oid) => catalog.servers.iter().find(|server| server.oid == *oid),
            other => {
                let name = other.to_cell();
                Some(catalog.server(&name).ok_or_else(|| {
                    SqlError::undefined_object(format!("server \"{name}\" does not exist"))
                })?)
            }
        };
        let Some(found) = found else {
            return Ok(SqlValue::Null);
        };
        (found.acl.clone(), found.owner.clone())
    };
    Ok(SqlValue::Bool(role_has_usage(db, &role, &acl, &owner)?))
}

// ---------------------------------------------------------------------------
// Catalog rows
// ---------------------------------------------------------------------------

const WRAPPER_COLUMNS: &[(&str, &str)] = &[
    ("oid", "oid"),
    ("fdwname", "name"),
    ("fdwowner", "oid"),
    ("fdwhandler", "oid"),
    ("fdwvalidator", "oid"),
    ("fdwacl", "aclitem[]"),
    ("fdwoptions", "text[]"),
];
const SERVER_COLUMNS: &[(&str, &str)] = &[
    ("oid", "oid"),
    ("srvname", "name"),
    ("srvowner", "oid"),
    ("srvfdw", "oid"),
    ("srvtype", "text"),
    ("srvversion", "text"),
    ("srvacl", "aclitem[]"),
    ("srvoptions", "text[]"),
];
const USER_MAPPING_COLUMNS: &[(&str, &str)] = &[
    ("oid", "oid"),
    ("umuser", "oid"),
    ("umserver", "oid"),
    ("umoptions", "text[]"),
];
const USER_MAPPINGS_COLUMNS: &[(&str, &str)] = &[
    ("umid", "oid"),
    ("srvid", "oid"),
    ("srvname", "name"),
    ("umuser", "oid"),
    ("usename", "name"),
    ("umoptions", "text[]"),
];
const FOREIGN_TABLE_COLUMNS: &[(&str, &str)] = &[
    ("ftrelid", "oid"),
    ("ftserver", "oid"),
    ("ftoptions", "text[]"),
];
const IS_WRAPPERS_COLUMNS: &[(&str, &str)] = &[
    ("foreign_data_wrapper_catalog", "name"),
    ("foreign_data_wrapper_name", "name"),
    ("authorization_identifier", "name"),
    ("library_name", "varchar"),
    ("foreign_data_wrapper_language", "varchar"),
];
const IS_WRAPPER_OPTIONS_COLUMNS: &[(&str, &str)] = &[
    ("foreign_data_wrapper_catalog", "name"),
    ("foreign_data_wrapper_name", "name"),
    ("option_name", "name"),
    ("option_value", "varchar"),
];
const IS_SERVERS_COLUMNS: &[(&str, &str)] = &[
    ("foreign_server_catalog", "name"),
    ("foreign_server_name", "name"),
    ("foreign_data_wrapper_catalog", "name"),
    ("foreign_data_wrapper_name", "name"),
    ("foreign_server_type", "varchar"),
    ("foreign_server_version", "varchar"),
    ("authorization_identifier", "name"),
];
const IS_SERVER_OPTIONS_COLUMNS: &[(&str, &str)] = &[
    ("foreign_server_catalog", "name"),
    ("foreign_server_name", "name"),
    ("option_name", "name"),
    ("option_value", "varchar"),
];
const IS_USER_MAPPINGS_COLUMNS: &[(&str, &str)] = &[
    ("authorization_identifier", "name"),
    ("foreign_server_catalog", "name"),
    ("foreign_server_name", "name"),
];
const IS_USER_MAPPING_OPTIONS_COLUMNS: &[(&str, &str)] = &[
    ("authorization_identifier", "name"),
    ("foreign_server_catalog", "name"),
    ("foreign_server_name", "name"),
    ("option_name", "name"),
    ("option_value", "varchar"),
];

fn catalog_columns_of(table: &str) -> Option<&'static [(&'static str, &'static str)]> {
    Some(match table.strip_prefix("pg_catalog.").unwrap_or(table) {
        "pg_foreign_data_wrapper" => WRAPPER_COLUMNS,
        "pg_foreign_server" => SERVER_COLUMNS,
        "pg_user_mapping" => USER_MAPPING_COLUMNS,
        "pg_user_mappings" => USER_MAPPINGS_COLUMNS,
        "pg_foreign_table" => FOREIGN_TABLE_COLUMNS,
        "information_schema.foreign_data_wrappers" => IS_WRAPPERS_COLUMNS,
        "information_schema.foreign_data_wrapper_options" => IS_WRAPPER_OPTIONS_COLUMNS,
        "information_schema.foreign_servers" => IS_SERVERS_COLUMNS,
        "information_schema.foreign_server_options" => IS_SERVER_OPTIONS_COLUMNS,
        "information_schema.user_mappings" => IS_USER_MAPPINGS_COLUMNS,
        "information_schema.user_mapping_options" => IS_USER_MAPPING_OPTIONS_COLUMNS,
        _ => return None,
    })
}

pub(crate) fn is_catalog_relation(table: &str) -> bool {
    catalog_columns_of(table).is_some()
}

pub(crate) fn catalog_columns(table: &str) -> Option<Vec<String>> {
    catalog_columns_of(table)
        .map(|columns| columns.iter().map(|(name, _)| name.to_string()).collect())
}

pub(crate) fn catalog_column_types(table: &str) -> Option<Vec<(String, Option<String>)>> {
    catalog_columns_of(table).map(|columns| {
        columns
            .iter()
            .map(|(name, kind)| (name.to_string(), Some(kind.to_string())))
            .collect()
    })
}

fn text_or_null(value: &Option<String>) -> SqlValue {
    value
        .as_ref()
        .map_or(SqlValue::Null, |text| SqlValue::String(text.clone()))
}

/// The rows of one of these catalogs as `role` sees them. Rows without a
/// session identity (`role` is `None`) are those of a role holding nothing:
/// the catalogs whose rows depend on the reader fail closed.
pub(crate) fn catalog_rows(
    db: &BicDb,
    table: &str,
    role: Option<&str>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let catalog = load_foreign_catalog(db)?;
    let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
    let superuser = match role {
        Some(role) => role_is_superuser(db, role)?,
        None => false,
    };
    let holds = |owner: &str| -> Result<bool> {
        match role {
            Some(role) => role_is_owner(db, role, owner),
            None => Ok(false),
        }
    };
    let uses = |acl: &Option<Vec<AclItem>>, owner: &str| -> Result<bool> {
        match role {
            Some(role) => role_has_usage(db, role, acl, owner),
            None => Ok(false),
        }
    };
    let wrapper_oid = |name: &str| catalog.wrapper(name).map_or(0, |wrapper| wrapper.oid);
    let server_oid = |name: &str| catalog.server(name).map_or(0, |server| server.oid);
    let mapping_user_oid = |mapping: &UserMapping| mapping.role.as_deref().map_or(0, role_oid);
    let current_catalog = crate::search_path::current_catalog_value;
    let mut rows = Vec::new();
    match table {
        "pg_foreign_data_wrapper" => {
            for wrapper in &catalog.wrappers {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(wrapper.oid)),
                    ("fdwname", SqlValue::String(wrapper.name.clone())),
                    ("fdwowner", SqlValue::Int(role_oid(&wrapper.owner))),
                    ("fdwhandler", SqlValue::Int(0)),
                    (
                        "fdwvalidator",
                        SqlValue::Int(if wrapper.validator.is_some() {
                            POSTGRESQL_FDW_VALIDATOR_OID
                        } else {
                            0
                        }),
                    ),
                    ("fdwacl", acl_value(&wrapper.acl)),
                    ("fdwoptions", options_value(&wrapper.options)),
                ]));
            }
        }
        "pg_foreign_server" => {
            for server in &catalog.servers {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(server.oid)),
                    ("srvname", SqlValue::String(server.name.clone())),
                    ("srvowner", SqlValue::Int(role_oid(&server.owner))),
                    ("srvfdw", SqlValue::Int(wrapper_oid(&server.wrapper))),
                    ("srvtype", text_or_null(&server.server_type)),
                    ("srvversion", text_or_null(&server.version)),
                    ("srvacl", acl_value(&server.acl)),
                    ("srvoptions", options_value(&server.options)),
                ]));
            }
        }
        "pg_user_mapping" => {
            // Holds passwords: superusers only, as PostgreSQL's catalog ACL.
            if !superuser {
                return Err(SqlError::coded(
                    "42501",
                    "permission denied for table pg_user_mapping",
                    None,
                    None,
                ));
            }
            for mapping in &catalog.mappings {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(mapping.oid)),
                    ("umuser", SqlValue::Int(mapping_user_oid(mapping))),
                    ("umserver", SqlValue::Int(server_oid(&mapping.server))),
                    ("umoptions", options_value(&mapping.options)),
                ]));
            }
        }
        "pg_user_mappings" => {
            for mapping in &catalog.mappings {
                let Some(server) = catalog.server(&mapping.server) else {
                    continue;
                };
                let visible = superuser
                    || match (&mapping.role, role) {
                        (Some(mapped), Some(reader)) => {
                            normalize_role_name(mapped) == normalize_role_name(reader)
                                && (holds(&server.owner)? || uses(&server.acl, &server.owner)?)
                        }
                        (None, _) => holds(&server.owner)?,
                        (Some(_), None) => false,
                    };
                rows.push(virtual_row([
                    ("umid", SqlValue::Int(mapping.oid)),
                    ("srvid", SqlValue::Int(server.oid)),
                    ("srvname", SqlValue::String(server.name.clone())),
                    ("umuser", SqlValue::Int(mapping_user_oid(mapping))),
                    (
                        "usename",
                        SqlValue::String(mapping.role_display().to_string()),
                    ),
                    (
                        "umoptions",
                        if visible {
                            options_value(&mapping.options)
                        } else {
                            SqlValue::Null
                        },
                    ),
                ]));
            }
        }
        "pg_foreign_table" => {}
        "information_schema.foreign_data_wrappers"
        | "information_schema.foreign_data_wrapper_options" => {
            for wrapper in &catalog.wrappers {
                if !(holds(&wrapper.owner)? || uses(&wrapper.acl, &wrapper.owner)?) {
                    continue;
                }
                if table == "information_schema.foreign_data_wrappers" {
                    rows.push(virtual_row([
                        ("foreign_data_wrapper_catalog", current_catalog()),
                        (
                            "foreign_data_wrapper_name",
                            SqlValue::String(wrapper.name.clone()),
                        ),
                        (
                            "authorization_identifier",
                            SqlValue::String(wrapper.owner.clone()),
                        ),
                        ("library_name", SqlValue::Null),
                        (
                            "foreign_data_wrapper_language",
                            SqlValue::String("c".to_string()),
                        ),
                    ]));
                    continue;
                }
                for (name, value) in &wrapper.options {
                    rows.push(virtual_row([
                        ("foreign_data_wrapper_catalog", current_catalog()),
                        (
                            "foreign_data_wrapper_name",
                            SqlValue::String(wrapper.name.clone()),
                        ),
                        ("option_name", SqlValue::String(name.clone())),
                        ("option_value", SqlValue::String(value.clone())),
                    ]));
                }
            }
        }
        "information_schema.foreign_servers"
        | "information_schema.foreign_server_options"
        | "information_schema.user_mappings"
        | "information_schema.user_mapping_options" => {
            for server in &catalog.servers {
                if !(holds(&server.owner)? || uses(&server.acl, &server.owner)?) {
                    continue;
                }
                match table {
                    "information_schema.foreign_servers" => rows.push(virtual_row([
                        ("foreign_server_catalog", current_catalog()),
                        ("foreign_server_name", SqlValue::String(server.name.clone())),
                        ("foreign_data_wrapper_catalog", current_catalog()),
                        (
                            "foreign_data_wrapper_name",
                            SqlValue::String(server.wrapper.clone()),
                        ),
                        ("foreign_server_type", text_or_null(&server.server_type)),
                        ("foreign_server_version", text_or_null(&server.version)),
                        (
                            "authorization_identifier",
                            SqlValue::String(server.owner.clone()),
                        ),
                    ])),
                    "information_schema.foreign_server_options" => {
                        for (name, value) in &server.options {
                            rows.push(virtual_row([
                                ("foreign_server_catalog", current_catalog()),
                                ("foreign_server_name", SqlValue::String(server.name.clone())),
                                ("option_name", SqlValue::String(name.clone())),
                                ("option_value", SqlValue::String(value.clone())),
                            ]));
                        }
                    }
                    _ => {
                        for mapping in catalog
                            .mappings
                            .iter()
                            .filter(|mapping| mapping.server == server.name)
                        {
                            let identifier =
                                mapping.role.clone().unwrap_or_else(|| "PUBLIC".to_string());
                            if table == "information_schema.user_mappings" {
                                rows.push(virtual_row([
                                    ("authorization_identifier", SqlValue::String(identifier)),
                                    ("foreign_server_catalog", current_catalog()),
                                    ("foreign_server_name", SqlValue::String(server.name.clone())),
                                ]));
                                continue;
                            }
                            let visible = superuser
                                || match (&mapping.role, role) {
                                    (Some(mapped), Some(reader)) => {
                                        normalize_role_name(mapped) == normalize_role_name(reader)
                                    }
                                    (None, _) => holds(&server.owner)?,
                                    (Some(_), None) => false,
                                };
                            for (name, value) in &mapping.options {
                                rows.push(virtual_row([
                                    (
                                        "authorization_identifier",
                                        SqlValue::String(identifier.clone()),
                                    ),
                                    ("foreign_server_catalog", current_catalog()),
                                    ("foreign_server_name", SqlValue::String(server.name.clone())),
                                    ("option_name", SqlValue::String(name.clone())),
                                    (
                                        "option_value",
                                        if visible {
                                            SqlValue::String(value.clone())
                                        } else {
                                            SqlValue::Null
                                        },
                                    ),
                                ]));
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(rows)
}

/// `pg_description` rows of commented wrappers and servers.
pub(crate) fn description_rows(db: &BicDb) -> Result<Vec<(i64, i64, String)>> {
    let catalog = load_foreign_catalog(db)?;
    let mut rows = Vec::new();
    for wrapper in &catalog.wrappers {
        if let Some(comment) = &wrapper.comment {
            rows.push((wrapper.oid, WRAPPER_CATALOG_OID, comment.clone()));
        }
    }
    for server in &catalog.servers {
        if let Some(comment) = &server.comment {
            rows.push((server.oid, SERVER_CATALOG_OID, comment.clone()));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closest_option_follows_the_reference_distance_rule() {
        assert_eq!(
            closest_match("username", &["user", "password"]),
            Some("user")
        );
        assert_eq!(closest_match("foo", &["host", "port", "dbname"]), None);
    }

    #[test]
    fn option_arrays_quote_like_text_arrays() {
        let options = vec![
            ("test wrapper".to_string(), "true".to_string()),
            ("a".to_string(), "1".to_string()),
        ];
        assert_eq!(
            options_value(&options),
            SqlValue::String("{\"test wrapper=true\",a=1}".to_string())
        );
        assert_eq!(
            options_to_table(&options_value(&options)).unwrap(),
            vec![
                ("test wrapper".to_string(), Some("true".to_string())),
                ("a".to_string(), Some("1".to_string())),
            ]
        );
    }

    #[test]
    fn revoking_a_grant_option_needs_cascade_for_dependent_grants() {
        let mut acl = None;
        acl_grant(&mut acl, "owner", "owner", &[Some("a".into())], true);
        acl_grant(&mut acl, "owner", "a", &[Some("b".into())], false);
        let refused = acl_revoke(
            &mut acl,
            "owner",
            "owner",
            &[Some("a".into())],
            false,
            false,
        );
        assert!(refused.is_err());
        acl_revoke(&mut acl, "owner", "owner", &[Some("a".into())], false, true).unwrap();
        assert_eq!(
            acl,
            Some(vec![AclItem {
                grantee: Some("owner".into()),
                grantor: "owner".into(),
                grant_option: false,
            }])
        );
    }
}
