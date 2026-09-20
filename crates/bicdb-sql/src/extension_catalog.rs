//! Durable extension installations and dynamic route/event declarations.
//!
//! PostgreSQL-compatible `pg_extension` rows remain in the legacy SQL catalog.
//! This module stores BicDB's executable package hash, validated manifest,
//! lifecycle state, REST resources, and durable event bindings separately.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, BicDbError, Record};
pub use bicdb_extension::{
    resolve_extension_order, ActiveWebsiteDeployment, DatabaseOperation, EventBindingDefinition,
    EventSource, ExtensionActivation, ExtensionCapability, ExtensionInstallation,
    ExtensionManifest, ExtensionState, HttpMethod, RestResourceDefinition, RouteAuth,
    WebsiteDefinition, WebsiteRelease,
};
use sha2::{Digest, Sha256};

use crate::{Result, SqlError};

pub const EXTENSION_INSTALLATION_COLLECTION: &str = "__bicdb_extension_installations";
pub const EXTENSION_RESOURCE_COLLECTION: &str = "__bicdb_extension_resources";
pub const EXTENSION_EVENT_BINDING_COLLECTION: &str = "__bicdb_extension_event_bindings";
pub const EXTENSION_WEBSITE_COLLECTION: &str = "__bicdb_extension_websites";
pub const EXTENSION_WEBSITE_RELEASE_COLLECTION: &str = "__bicdb_extension_website_releases";
pub const MAX_WEBSITE_CONTENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub enum ExtensionCatalogDdl {
    Install {
        name: String,
        module_sha256: String,
        manifest: ExtensionManifest,
        if_not_exists: bool,
    },
    ActivateSingleNode {
        name: String,
    },
    UpgradeSingleNode {
        name: String,
        module_sha256: String,
        manifest: ExtensionManifest,
    },
    Disable {
        name: String,
        cascade: bool,
    },
    DropExtension {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    CreateResource {
        definition: RestResourceDefinition,
        if_not_exists: bool,
    },
    DropResource {
        name: String,
        if_exists: bool,
    },
    CreateEventBinding {
        definition: EventBindingDefinition,
        if_not_exists: bool,
    },
    DropEventBinding {
        name: String,
        if_exists: bool,
    },
    CreateWebsite {
        definition: WebsiteDefinition,
        if_not_exists: bool,
    },
    PublishWebsite {
        release: WebsiteRelease,
        activate: bool,
    },
    ActivateWebsite {
        name: String,
        version: String,
    },
    RollbackWebsite {
        name: String,
    },
    DropWebsite {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
}

pub fn parse_extension_catalog_ddl(sql: &str) -> Result<Option<ExtensionCatalogDdl>> {
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("CREATE") && cursor.consume_keyword("EXTENSION") {
        return parse_install_extension(cursor);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("ALTER") && cursor.consume_keyword("EXTENSION") {
        return parse_alter_extension(cursor);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("DROP") && cursor.consume_keyword("EXTENSION") {
        return parse_drop_extension(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("CREATE") && cursor.consume_keyword("RESOURCE") {
        return parse_create_resource(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("DROP") && cursor.consume_keyword("RESOURCE") {
        return parse_drop_resource(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("CREATE")
        && cursor.consume_keyword("EVENT")
        && cursor.consume_keyword("SUBSCRIPTION")
    {
        return parse_create_event_binding(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("DROP")
        && cursor.consume_keyword("EVENT")
        && cursor.consume_keyword("SUBSCRIPTION")
    {
        return parse_drop_event_binding(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("CREATE") && cursor.consume_keyword("WEBSITE") {
        return parse_create_website(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("PUBLISH") && cursor.consume_keyword("WEBSITE") {
        return parse_publish_website(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("ALTER") && cursor.consume_keyword("WEBSITE") {
        return parse_alter_website(cursor).map(Some);
    }
    let mut cursor = SqlCursor::new(sql);
    if cursor.consume_keyword("DROP") && cursor.consume_keyword("WEBSITE") {
        return parse_drop_website(cursor).map(Some);
    }
    Ok(None)
}

fn parse_install_extension(mut cursor: SqlCursor<'_>) -> Result<Option<ExtensionCatalogDdl>> {
    let if_not_exists = cursor.consume_keywords(&["IF", "NOT", "EXISTS"]);
    let name = cursor.identifier()?;
    // Leave ordinary PostgreSQL CREATE EXTENSION statements to the existing
    // compatibility parser.
    if !cursor.consume_keyword("FROM") {
        return Ok(None);
    }
    cursor.expect_keyword("MODULE")?;
    let module_sha256 = cursor.string_literal()?;
    cursor.expect_keyword("MANIFEST")?;
    let manifest_json = cursor.string_literal()?;
    cursor.finish()?;
    let manifest: ExtensionManifest = serde_json::from_str(&manifest_json).map_err(|error| {
        SqlError::InvalidSql(format!("invalid extension manifest JSON: {error}"))
    })?;
    if !manifest.identity.name.eq_ignore_ascii_case(&name) {
        return Err(SqlError::InvalidSql(format!(
            "extension name `{name}` does not match manifest name `{}`",
            manifest.identity.name
        )));
    }
    manifest.validate().map_err(extension_error)?;
    Ok(Some(ExtensionCatalogDdl::Install {
        name: normalize_name(&name),
        module_sha256,
        manifest,
        if_not_exists,
    }))
}

fn parse_alter_extension(mut cursor: SqlCursor<'_>) -> Result<Option<ExtensionCatalogDdl>> {
    let name = cursor.identifier()?;
    if cursor.consume_keyword("UPDATE") {
        cursor.expect_keyword("FROM")?;
        cursor.expect_keyword("MODULE")?;
        let module_sha256 = cursor.string_literal()?;
        cursor.expect_keyword("MANIFEST")?;
        let manifest_json = cursor.string_literal()?;
        cursor.finish()?;
        let manifest: ExtensionManifest =
            serde_json::from_str(&manifest_json).map_err(|error| {
                SqlError::InvalidSql(format!("invalid extension manifest JSON: {error}"))
            })?;
        if !manifest.identity.name.eq_ignore_ascii_case(&name) {
            return Err(SqlError::InvalidSql(format!(
                "extension name `{name}` does not match manifest name `{}`",
                manifest.identity.name
            )));
        }
        manifest.validate().map_err(extension_error)?;
        return Ok(Some(ExtensionCatalogDdl::UpgradeSingleNode {
            name: normalize_name(&name),
            module_sha256,
            manifest,
        }));
    }
    if cursor.consume_keyword("ACTIVATE") {
        if !cursor.consume_keywords(&["SINGLE", "NODE"]) {
            return Err(SqlError::InvalidSql(
                "ALTER EXTENSION ACTIVATE requires SINGLE NODE; clustered activation must be quorum committed through the cluster controller"
                    .to_string(),
            ));
        }
        cursor.finish()?;
        return Ok(Some(ExtensionCatalogDdl::ActivateSingleNode {
            name: normalize_name(&name),
        }));
    }
    if cursor.consume_keyword("DISABLE") {
        let cascade = if cursor.consume_keyword("CASCADE") {
            true
        } else {
            cursor.consume_keyword("RESTRICT");
            false
        };
        cursor.finish()?;
        return Ok(Some(ExtensionCatalogDdl::Disable {
            name: normalize_name(&name),
            cascade,
        }));
    }
    Ok(None)
}

fn parse_drop_extension(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_exists = cursor.consume_keywords(&["IF", "EXISTS"]);
    let name = cursor.identifier()?;
    let cascade = if cursor.consume_keyword("CASCADE") {
        true
    } else {
        cursor.consume_keyword("RESTRICT");
        false
    };
    cursor.finish()?;
    Ok(ExtensionCatalogDdl::DropExtension {
        name: normalize_name(&name),
        if_exists,
        cascade,
    })
}

fn parse_create_resource(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_not_exists = cursor.consume_keywords(&["IF", "NOT", "EXISTS"]);
    let name = cursor.identifier()?;
    cursor.expect_keyword("USING")?;
    cursor.expect_keyword("EXTENSION")?;
    let extension = cursor.identifier()?;
    cursor.expect_keyword("FROM")?;
    cursor.expect_keyword("TABLE")?;
    let relation = cursor.qualified_identifier()?;
    let options = cursor.options()?;
    cursor.finish()?;

    let path = required_option(&options, "path")?.to_string();
    let export = options
        .get("export")
        .cloned()
        .unwrap_or_else(|| "handle_resource".to_string());
    let methods = parse_http_methods(required_option(&options, "methods")?)?;
    let auth = match options.get("auth").map(String::as_str).unwrap_or("rls") {
        "public" => RouteAuth::Public,
        "authenticated" => RouteAuth::Authenticated,
        "rls" | "row_level_security" => RouteAuth::RowLevelSecurity,
        "admin" => RouteAuth::Admin,
        other => {
            return Err(SqlError::InvalidSql(format!(
                "unsupported resource auth `{other}`"
            )))
        }
    };
    let openapi = parse_bool_option(&options, "openapi", true)?;
    let definition = RestResourceDefinition {
        name: normalize_name(&name),
        extension: normalize_name(&extension),
        relation: normalize_qualified_name(&relation),
        path,
        export,
        methods,
        auth,
        openapi,
        enabled: true,
    };
    definition.validate().map_err(extension_error)?;
    Ok(ExtensionCatalogDdl::CreateResource {
        definition,
        if_not_exists,
    })
}

fn parse_drop_resource(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_exists = cursor.consume_keywords(&["IF", "EXISTS"]);
    let name = cursor.identifier()?;
    cursor.finish()?;
    Ok(ExtensionCatalogDdl::DropResource {
        name: normalize_name(&name),
        if_exists,
    })
}

fn parse_create_event_binding(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_not_exists = cursor.consume_keywords(&["IF", "NOT", "EXISTS"]);
    let name = cursor.identifier()?;
    cursor.expect_keyword("USING")?;
    cursor.expect_keyword("EXTENSION")?;
    let extension = cursor.identifier()?;
    cursor.expect_keyword("ON")?;

    let (source, delivery_queue) = if cursor.consume_keyword("TABLE") {
        let relation = cursor.qualified_identifier()?;
        cursor.expect_keyword("EVENTS")?;
        let operations = parse_database_operations(&cursor.parenthesized()?)?;
        cursor.expect_keyword("QUEUE")?;
        let queue = cursor.string_literal()?;
        (
            EventSource::Database {
                relation: normalize_qualified_name(&relation),
                operations,
            },
            Some(queue),
        )
    } else if cursor.consume_keyword("QUEUE") {
        let queue = cursor.string_literal()?;
        cursor.expect_keyword("GROUP")?;
        let group = cursor.string_literal()?;
        (EventSource::Queue { queue, group }, None)
    } else {
        return Err(SqlError::InvalidSql(
            "event subscription source must be TABLE or QUEUE".to_string(),
        ));
    };
    cursor.expect_keyword("EXECUTE")?;
    let export = cursor.string_literal()?;
    let options = if cursor.peek_keyword("WITH") {
        cursor.options()?
    } else {
        BTreeMap::new()
    };
    cursor.finish()?;
    let definition = EventBindingDefinition {
        name: normalize_name(&name),
        extension: normalize_name(&extension),
        source,
        delivery_queue,
        export,
        max_attempts: parse_u32_option(&options, "max_attempts", 5)?,
        visibility_timeout_ms: parse_u64_option(&options, "visibility_timeout_ms", 30_000)?,
        enabled: true,
    };
    definition.validate().map_err(extension_error)?;
    Ok(ExtensionCatalogDdl::CreateEventBinding {
        definition,
        if_not_exists,
    })
}

fn parse_drop_event_binding(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_exists = cursor.consume_keywords(&["IF", "EXISTS"]);
    let name = cursor.identifier()?;
    cursor.finish()?;
    Ok(ExtensionCatalogDdl::DropEventBinding {
        name: normalize_name(&name),
        if_exists,
    })
}

fn parse_create_website(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_not_exists = cursor.consume_keywords(&["IF", "NOT", "EXISTS"]);
    let name = cursor.identifier()?;
    cursor.expect_keyword("USING")?;
    cursor.expect_keyword("EXTENSION")?;
    let extension = cursor.identifier()?;
    let options = cursor.options()?;
    cursor.finish()?;
    let host = options
        .get("host")
        .filter(|host| host.as_str() != "*")
        .map(|host| host.to_ascii_lowercase());
    let definition = WebsiteDefinition {
        name: normalize_name(&name),
        extension: normalize_name(&extension),
        host,
        mount_path: options
            .get("path")
            .cloned()
            .unwrap_or_else(|| "/".to_string()),
        export: options
            .get("export")
            .cloned()
            .unwrap_or_else(|| "render_website".to_string()),
        auth: parse_route_auth(&options, "website")?,
        active_version: None,
        previous_version: None,
        enabled: true,
    };
    definition.validate().map_err(extension_error)?;
    Ok(ExtensionCatalogDdl::CreateWebsite {
        definition,
        if_not_exists,
    })
}

fn parse_publish_website(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let website = normalize_name(&cursor.identifier()?);
    cursor.expect_keyword("VERSION")?;
    let version = cursor.string_literal()?;
    cursor.expect_keyword("CONTENT")?;
    let content_json = cursor.string_literal()?;
    let activate = cursor.consume_keyword("ACTIVATE");
    cursor.finish()?;
    let content = serde_json::from_str(&content_json)
        .map_err(|error| SqlError::InvalidSql(format!("invalid website content JSON: {error}")))?;
    let encoded = serde_json::to_vec(&content)?;
    let content_sha256 = format!("{:x}", Sha256::digest(&encoded));
    let release = WebsiteRelease {
        website,
        version,
        content_sha256,
        content,
        created_at_ms: unix_now_ms(),
    };
    release.validate().map_err(extension_error)?;
    Ok(ExtensionCatalogDdl::PublishWebsite { release, activate })
}

fn parse_alter_website(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let name = normalize_name(&cursor.identifier()?);
    if cursor.consume_keyword("ACTIVATE") {
        cursor.expect_keyword("VERSION")?;
        let version = cursor.string_literal()?;
        cursor.finish()?;
        return Ok(ExtensionCatalogDdl::ActivateWebsite { name, version });
    }
    if cursor.consume_keyword("ROLLBACK") {
        cursor.finish()?;
        return Ok(ExtensionCatalogDdl::RollbackWebsite { name });
    }
    Err(SqlError::InvalidSql(
        "ALTER WEBSITE requires ACTIVATE VERSION or ROLLBACK".to_string(),
    ))
}

fn parse_drop_website(mut cursor: SqlCursor<'_>) -> Result<ExtensionCatalogDdl> {
    let if_exists = cursor.consume_keywords(&["IF", "EXISTS"]);
    let name = normalize_name(&cursor.identifier()?);
    let cascade = if cursor.consume_keyword("CASCADE") {
        true
    } else {
        cursor.consume_keyword("RESTRICT");
        false
    };
    cursor.finish()?;
    Ok(ExtensionCatalogDdl::DropWebsite {
        name,
        if_exists,
        cascade,
    })
}

pub fn install_extension(
    db: &mut BicDb,
    installation: ExtensionInstallation,
    if_not_exists: bool,
) -> Result<bool> {
    installation.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_INSTALLATION_COLLECTION)?;
    let key = normalize_name(&installation.manifest.identity.name);
    if db.get(EXTENSION_INSTALLATION_COLLECTION, &key)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(SqlError::InvalidSql(format!(
            "extension `{key}` is already installed"
        )));
    }
    db.insert(
        EXTENSION_INSTALLATION_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(installation)?),
    )?;
    Ok(true)
}

pub fn load_extension(db: &BicDb, name: &str) -> Result<Option<ExtensionInstallation>> {
    load_record(db, EXTENSION_INSTALLATION_COLLECTION, &normalize_name(name))
}

pub fn list_installed_extensions(db: &BicDb) -> Result<Vec<ExtensionInstallation>> {
    list_records(db, EXTENSION_INSTALLATION_COLLECTION)
}

pub fn save_extension(db: &mut BicDb, installation: &ExtensionInstallation) -> Result<()> {
    installation.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_INSTALLATION_COLLECTION)?;
    db.insert(
        EXTENSION_INSTALLATION_COLLECTION,
        Record::new(normalize_name(&installation.manifest.identity.name))
            .with_metadata(serde_json::to_value(installation)?),
    )?;
    Ok(())
}

pub fn activate_extension(
    db: &mut BicDb,
    name: &str,
    activation: ExtensionActivation,
) -> Result<()> {
    activation.validate().map_err(extension_error)?;
    let mut installation = load_extension(db, name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("extension `{name}` is not installed")))?;
    installation.state = ExtensionState::Active;
    installation.activation = Some(activation);
    installation.last_error = None;
    save_extension(db, &installation)
}

#[cfg(feature = "extension-host")]
pub fn activate_extension_single_node(
    db: &mut BicDb,
    name: &str,
) -> Result<Vec<ExtensionInstallation>> {
    activate_extension_graph(db, name, None)
}

#[cfg(not(feature = "extension-host"))]
pub fn activate_extension_single_node(
    _db: &mut BicDb,
    _name: &str,
) -> Result<Vec<ExtensionInstallation>> {
    Err(SqlError::Unsupported(
        "this BicDB build cannot activate native-hosted WASM extensions".to_string(),
    ))
}

#[cfg(feature = "extension-host")]
pub fn upgrade_extension_single_node(
    db: &mut BicDb,
    name: &str,
    module_sha256: String,
    manifest: ExtensionManifest,
) -> Result<Vec<ExtensionInstallation>> {
    let previous = load_extension(db, name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("extension `{name}` is not installed")))?;
    if previous.state != ExtensionState::Active {
        return Err(SqlError::InvalidSql(format!(
            "extension `{name}` must be active before an atomic upgrade"
        )));
    }
    let candidate = ExtensionInstallation {
        manifest,
        module_sha256,
        state: ExtensionState::Staged,
        installed_at_ms: unix_now_ms(),
        activation: None,
        last_error: None,
    };
    activate_extension_graph(db, name, Some(candidate))
}

#[cfg(not(feature = "extension-host"))]
pub fn upgrade_extension_single_node(
    _db: &mut BicDb,
    _name: &str,
    _module_sha256: String,
    _manifest: ExtensionManifest,
) -> Result<Vec<ExtensionInstallation>> {
    Err(SqlError::Unsupported(
        "this BicDB build cannot upgrade native-hosted WASM extensions".to_string(),
    ))
}

#[cfg(feature = "extension-host")]
fn activate_extension_graph(
    db: &mut BicDb,
    root: &str,
    replacement: Option<ExtensionInstallation>,
) -> Result<Vec<ExtensionInstallation>> {
    let root = normalize_name(root);
    let mut installations = list_installed_extensions(db)?;
    let root_position = installations
        .iter()
        .position(|installation| {
            installation
                .manifest
                .identity
                .name
                .eq_ignore_ascii_case(&root)
        })
        .ok_or_else(|| SqlError::InvalidSql(format!("extension `{root}` is not installed")))?;
    if let Some(replacement) = replacement {
        if !replacement
            .manifest
            .identity
            .name
            .eq_ignore_ascii_case(&root)
        {
            return Err(SqlError::InvalidSql(format!(
                "upgrade package name `{}` does not match extension `{root}`",
                replacement.manifest.identity.name
            )));
        }
        installations[root_position] = replacement;
    }

    let order = resolve_extension_order(&installations, std::slice::from_ref(&root), false)
        .map_err(extension_error)?;
    let base_generation = db
        .collection_generation(EXTENSION_INSTALLATION_COLLECTION)
        .saturating_add(1)
        .max(1);
    let mut changes = Vec::new();
    for (offset, extension_name) in order.iter().enumerate() {
        let installation = installations
            .iter()
            .find(|installation| {
                installation
                    .manifest
                    .identity
                    .name
                    .eq_ignore_ascii_case(extension_name)
            })
            .expect("dependency resolver only returns installed extensions");
        verify_local_package(db, installation)?;
        let current = load_extension(db, extension_name)?.ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "extension `{extension_name}` disappeared during activation"
            ))
        })?;
        let is_replacement =
            extension_name == &root && installation.module_sha256 != current.module_sha256;
        if current.state == ExtensionState::Active && !is_replacement {
            continue;
        }
        let mut active = installation.clone();
        active.state = ExtensionState::Active;
        active.activation = Some(ExtensionActivation {
            catalog_generation: base_generation.saturating_add(offset as u64),
            topology_generation: 0,
            ready_nodes: BTreeSet::from(["local".to_string()]),
            required_nodes: BTreeSet::from(["local".to_string()]),
            quorum_committed: false,
            activated_at_ms: unix_now_ms(),
        });
        active.last_error = None;
        changes.push((current, active));
    }
    save_extension_changes(db, changes)
}

fn save_extension_changes(
    db: &mut BicDb,
    changes: Vec<(ExtensionInstallation, ExtensionInstallation)>,
) -> Result<Vec<ExtensionInstallation>> {
    let mut previous = Vec::with_capacity(changes.len());
    for (before, after) in changes {
        if let Err(error) = save_extension(db, &after) {
            for installation in previous.iter().rev() {
                let _ = save_extension(db, installation);
            }
            return Err(error);
        }
        previous.push(before);
    }
    Ok(previous)
}

#[cfg(feature = "extension-host")]
fn verify_local_package(db: &BicDb, installation: &ExtensionInstallation) -> Result<()> {
    use bicdb_extension::host::{ExtensionPackageStore, WasmExtension, WasmHostConfig};

    let config = WasmHostConfig::default();
    let packages = ExtensionPackageStore::open(db.data_path().join("extensions/packages"))
        .map_err(extension_error)?;
    let bytes = packages
        .read_verified(&installation.module_sha256, config.max_module_bytes)
        .map_err(extension_error)?;
    let module = WasmExtension::load(&bytes, config).map_err(extension_error)?;
    if module.manifest() != &installation.manifest {
        return Err(SqlError::InvalidSql(format!(
            "catalog manifest differs from package manifest for extension `{}`",
            installation.manifest.identity.name
        )));
    }
    Ok(())
}

pub fn disable_extension(
    db: &mut BicDb,
    name: &str,
    cascade: bool,
) -> Result<Vec<ExtensionInstallation>> {
    let name = normalize_name(name);
    let installation = load_extension(db, &name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("extension `{name}` is not installed")))?;
    let installations = list_installed_extensions(db)?;
    let dependents = required_extension_dependents(&installations, &name, true);
    if !cascade && !dependents.is_empty() {
        return Err(SqlError::dependent_objects_still_exist(format!(
            "extension `{name}` is required by active extensions: {}",
            dependents.join(", ")
        )));
    }
    let mut changes = Vec::new();
    for dependent in dependents.iter().rev().chain(std::iter::once(&name)) {
        let Some(current) = installations.iter().find(|candidate| {
            candidate
                .manifest
                .identity
                .name
                .eq_ignore_ascii_case(dependent)
        }) else {
            continue;
        };
        if current.state == ExtensionState::Disabled {
            continue;
        }
        let mut disabled = current.clone();
        disabled.state = ExtensionState::Disabled;
        disabled.activation = None;
        changes.push((current.clone(), disabled));
    }
    if changes.is_empty() && installation.state != ExtensionState::Disabled {
        let mut disabled = installation.clone();
        disabled.state = ExtensionState::Disabled;
        disabled.activation = None;
        changes.push((installation, disabled));
    }
    save_extension_changes(db, changes)
}

pub fn delete_extension(db: &mut BicDb, name: &str, cascade: bool) -> Result<bool> {
    let name = normalize_name(name);
    let installations = list_installed_extensions(db)?;
    let dependents = required_extension_dependents(&installations, &name, false);
    let resources = list_rest_resources(db)?
        .into_iter()
        .filter(|resource| resource.extension == name)
        .collect::<Vec<_>>();
    let bindings = list_event_bindings(db)?
        .into_iter()
        .filter(|binding| binding.extension == name)
        .collect::<Vec<_>>();
    let websites = list_websites(db)?
        .into_iter()
        .filter(|website| website.extension == name)
        .collect::<Vec<_>>();
    if !cascade
        && (!resources.is_empty()
            || !bindings.is_empty()
            || !websites.is_empty()
            || !dependents.is_empty())
    {
        let dependent_summary = if dependents.is_empty() {
            String::new()
        } else {
            format!("; required by extensions {}", dependents.join(", "))
        };
        return Err(SqlError::dependent_objects_still_exist(format!(
            "extension `{name}` has dependent resources, websites, or event subscriptions{dependent_summary}"
        )));
    }
    if cascade {
        let mut changes = Vec::new();
        for dependent in dependents.iter().rev() {
            let current = installations
                .iter()
                .find(|installation| {
                    installation
                        .manifest
                        .identity
                        .name
                        .eq_ignore_ascii_case(dependent)
                })
                .expect("dependent extension came from installed catalog");
            if current.state == ExtensionState::Active {
                let mut disabled = current.clone();
                disabled.state = ExtensionState::Disabled;
                disabled.activation = None;
                changes.push((current.clone(), disabled));
            }
        }
        save_extension_changes(db, changes)?;
        for resource in resources {
            delete_rest_resource(db, &resource.name)?;
        }
        for binding in bindings {
            delete_event_binding(db, &binding.name)?;
        }
        for website in websites {
            delete_website(db, &website.name, true)?;
        }
    }
    delete_record(db, EXTENSION_INSTALLATION_COLLECTION, &name)
}

fn required_extension_dependents(
    installations: &[ExtensionInstallation],
    name: &str,
    active_only: bool,
) -> Vec<String> {
    let mut required = BTreeSet::from([normalize_name(name)]);
    let mut changed = true;
    while changed {
        changed = false;
        for installation in installations {
            if active_only && installation.state != ExtensionState::Active {
                continue;
            }
            let candidate = normalize_name(&installation.manifest.identity.name);
            if required.contains(&candidate) {
                continue;
            }
            if installation.manifest.dependencies.iter().any(|dependency| {
                !dependency.optional && required.contains(&normalize_name(&dependency.name))
            }) {
                changed |= required.insert(candidate);
            }
        }
    }
    required.remove(&normalize_name(name));
    required.into_iter().collect()
}

pub fn save_rest_resource(
    db: &mut BicDb,
    definition: RestResourceDefinition,
    if_not_exists: bool,
) -> Result<bool> {
    definition.validate().map_err(extension_error)?;
    let installation = require_active_extension(db, &definition.extension)?;
    require_capability(&installation, ExtensionCapability::HttpRoutes)?;
    if !installation
        .manifest
        .routes
        .iter()
        .any(|route| route.export == definition.export)
    {
        return Err(SqlError::InvalidSql(format!(
            "extension `{}` did not declare route export `{}`",
            definition.extension, definition.export
        )));
    }
    if !installation
        .manifest
        .permissions
        .read_relations
        .iter()
        .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(&definition.relation))
    {
        return Err(SqlError::InvalidSql(format!(
            "extension `{}` did not request read access to relation `{}`",
            definition.extension, definition.relation
        )));
    }
    db.create_collection(EXTENSION_RESOURCE_COLLECTION)?;
    let key = normalize_name(&definition.name);
    if db.get(EXTENSION_RESOURCE_COLLECTION, &key)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(SqlError::InvalidSql(format!(
            "resource `{key}` already exists"
        )));
    }
    for existing in list_rest_resources(db)? {
        for method in definition.methods.intersection(&existing.methods) {
            if existing.enabled && existing.path == definition.path {
                return Err(SqlError::InvalidSql(format!(
                    "route {method} {} is already registered by resource `{}`",
                    definition.path, existing.name
                )));
            }
        }
    }
    db.insert(
        EXTENSION_RESOURCE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(true)
}

pub fn load_rest_resource(db: &BicDb, name: &str) -> Result<Option<RestResourceDefinition>> {
    load_record(db, EXTENSION_RESOURCE_COLLECTION, &normalize_name(name))
}

pub fn list_rest_resources(db: &BicDb) -> Result<Vec<RestResourceDefinition>> {
    list_records(db, EXTENSION_RESOURCE_COLLECTION)
}

pub fn delete_rest_resource(db: &mut BicDb, name: &str) -> Result<bool> {
    delete_record(db, EXTENSION_RESOURCE_COLLECTION, &normalize_name(name))
}

pub(crate) fn restore_rest_resource(
    db: &mut BicDb,
    definition: RestResourceDefinition,
) -> Result<()> {
    definition.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_RESOURCE_COLLECTION)?;
    let key = normalize_name(&definition.name);
    db.insert(
        EXTENSION_RESOURCE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(())
}

pub fn save_website(
    db: &mut BicDb,
    definition: WebsiteDefinition,
    if_not_exists: bool,
) -> Result<bool> {
    definition.validate().map_err(extension_error)?;
    let installation = require_active_extension(db, &definition.extension)?;
    require_capability(&installation, ExtensionCapability::HttpRoutes)?;
    if !installation
        .manifest
        .routes
        .iter()
        .any(|route| route.export == definition.export)
    {
        return Err(SqlError::InvalidSql(format!(
            "extension `{}` did not declare website route export `{}`",
            definition.extension, definition.export
        )));
    }
    db.create_collection(EXTENSION_WEBSITE_COLLECTION)?;
    let key = normalize_name(&definition.name);
    if db.get(EXTENSION_WEBSITE_COLLECTION, &key)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(SqlError::InvalidSql(format!(
            "website `{key}` already exists"
        )));
    }
    for existing in list_websites(db)?.into_iter().filter(|site| site.enabled) {
        if existing.host.as_deref().map(str::to_ascii_lowercase)
            == definition.host.as_deref().map(str::to_ascii_lowercase)
            && existing.mount_path == definition.mount_path
        {
            return Err(SqlError::InvalidSql(format!(
                "website mount {}{} is already registered by `{}`",
                definition
                    .host
                    .as_deref()
                    .map(|host| format!("{host}:"))
                    .unwrap_or_default(),
                definition.mount_path,
                existing.name
            )));
        }
    }
    db.insert(
        EXTENSION_WEBSITE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(true)
}

pub fn load_website(db: &BicDb, name: &str) -> Result<Option<WebsiteDefinition>> {
    load_record(db, EXTENSION_WEBSITE_COLLECTION, &normalize_name(name))
}

pub fn list_websites(db: &BicDb) -> Result<Vec<WebsiteDefinition>> {
    list_records(db, EXTENSION_WEBSITE_COLLECTION)
}

pub(crate) fn restore_website(db: &mut BicDb, definition: WebsiteDefinition) -> Result<()> {
    definition.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_WEBSITE_COLLECTION)?;
    let key = normalize_name(&definition.name);
    db.insert(
        EXTENSION_WEBSITE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(())
}

pub fn save_website_release(db: &mut BicDb, release: WebsiteRelease) -> Result<()> {
    release.validate().map_err(extension_error)?;
    let encoded = serde_json::to_vec(&release.content)?;
    if encoded.len() > MAX_WEBSITE_CONTENT_BYTES {
        return Err(SqlError::InvalidSql(format!(
            "website content is {} bytes; catalog limit is {MAX_WEBSITE_CONTENT_BYTES}",
            encoded.len()
        )));
    }
    let actual_sha256 = format!("{:x}", Sha256::digest(&encoded));
    if actual_sha256 != release.content_sha256 {
        return Err(SqlError::InvalidSql(
            "website content SHA-256 does not match its JSON bundle".to_string(),
        ));
    }
    let website = load_website(db, &release.website)?.ok_or_else(|| {
        SqlError::InvalidSql(format!("website `{}` does not exist", release.website))
    })?;
    let installation = require_active_extension(db, &website.extension)?;
    let renderer_limit = installation.manifest.limits.max_input_bytes as usize;
    if encoded.len().saturating_add(64 * 1024) > renderer_limit {
        return Err(SqlError::InvalidSql(format!(
            "website content is {} bytes; extension `{}` input limit is {renderer_limit}",
            encoded.len(),
            website.extension
        )));
    }
    db.create_collection(EXTENSION_WEBSITE_RELEASE_COLLECTION)?;
    let key = website_release_key(&release.website, &release.version);
    if db
        .get(EXTENSION_WEBSITE_RELEASE_COLLECTION, &key)?
        .is_some()
    {
        return Err(SqlError::InvalidSql(format!(
            "website `{}` version `{}` is immutable and already exists",
            release.website, release.version
        )));
    }
    db.insert(
        EXTENSION_WEBSITE_RELEASE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(release)?),
    )?;
    Ok(())
}

pub fn load_website_release(
    db: &BicDb,
    website: &str,
    version: &str,
) -> Result<Option<WebsiteRelease>> {
    load_record(
        db,
        EXTENSION_WEBSITE_RELEASE_COLLECTION,
        &website_release_key(website, version),
    )
}

pub fn list_website_releases(db: &BicDb, website: &str) -> Result<Vec<WebsiteRelease>> {
    Ok(list_records(db, EXTENSION_WEBSITE_RELEASE_COLLECTION)?
        .into_iter()
        .filter(|release: &WebsiteRelease| release.website.eq_ignore_ascii_case(website))
        .collect())
}

pub(crate) fn restore_website_release(db: &mut BicDb, release: WebsiteRelease) -> Result<()> {
    release.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_WEBSITE_RELEASE_COLLECTION)?;
    let key = website_release_key(&release.website, &release.version);
    db.insert(
        EXTENSION_WEBSITE_RELEASE_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(release)?),
    )?;
    Ok(())
}

pub fn delete_website_release(db: &mut BicDb, website: &str, version: &str) -> Result<bool> {
    delete_record(
        db,
        EXTENSION_WEBSITE_RELEASE_COLLECTION,
        &website_release_key(website, version),
    )
}

pub fn activate_website_version(
    db: &mut BicDb,
    name: &str,
    version: &str,
) -> Result<WebsiteDefinition> {
    let mut website = load_website(db, name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("website `{name}` does not exist")))?;
    require_active_extension(db, &website.extension)?;
    let release = load_website_release(db, name, version)?.ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "website `{name}` version `{version}` does not exist"
        ))
    })?;
    release.validate().map_err(extension_error)?;
    let previous = website.clone();
    if website.active_version.as_deref() == Some(version) {
        return Ok(previous);
    }
    website.previous_version = website.active_version.take();
    website.active_version = Some(version.to_string());
    save_website_pointer(db, &website)?;
    Ok(previous)
}

pub fn rollback_website(db: &mut BicDb, name: &str) -> Result<WebsiteDefinition> {
    let mut website = load_website(db, name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("website `{name}` does not exist")))?;
    require_active_extension(db, &website.extension)?;
    let rollback_version = website.previous_version.clone().ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "website `{name}` has no previous version to roll back to"
        ))
    })?;
    if load_website_release(db, name, &rollback_version)?.is_none() {
        return Err(SqlError::InvalidSql(format!(
            "website `{name}` previous version `{rollback_version}` is missing"
        )));
    }
    let previous = website.clone();
    std::mem::swap(&mut website.active_version, &mut website.previous_version);
    save_website_pointer(db, &website)?;
    Ok(previous)
}

fn save_website_pointer(db: &mut BicDb, website: &WebsiteDefinition) -> Result<()> {
    website.validate().map_err(extension_error)?;
    db.insert(
        EXTENSION_WEBSITE_COLLECTION,
        Record::new(normalize_name(&website.name)).with_metadata(serde_json::to_value(website)?),
    )?;
    Ok(())
}

pub fn list_active_website_deployments(db: &BicDb) -> Result<Vec<ActiveWebsiteDeployment>> {
    let mut deployments = Vec::new();
    for definition in list_websites(db)?
        .into_iter()
        .filter(|website| website.enabled)
    {
        if !load_extension(db, &definition.extension)?
            .is_some_and(|installation| installation.state == ExtensionState::Active)
        {
            continue;
        }
        let Some(version) = definition.active_version.as_deref() else {
            continue;
        };
        let release = load_website_release(db, &definition.name, version)?.ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "website `{}` active version `{version}` is missing",
                definition.name
            ))
        })?;
        let deployment = ActiveWebsiteDeployment {
            definition,
            release,
        };
        deployment.validate().map_err(extension_error)?;
        deployments.push(deployment);
    }
    Ok(deployments)
}

pub fn delete_website(db: &mut BicDb, name: &str, cascade: bool) -> Result<bool> {
    let releases = list_website_releases(db, name)?;
    if !cascade && !releases.is_empty() {
        return Err(SqlError::dependent_objects_still_exist(format!(
            "website `{name}` has {} immutable releases",
            releases.len()
        )));
    }
    if cascade {
        for release in releases {
            delete_website_release(db, &release.website, &release.version)?;
        }
    }
    delete_record(db, EXTENSION_WEBSITE_COLLECTION, &normalize_name(name))
}

fn website_release_key(website: &str, version: &str) -> String {
    format!("{}@{version}", normalize_name(website))
}

pub fn save_event_binding(
    db: &mut BicDb,
    definition: EventBindingDefinition,
    if_not_exists: bool,
) -> Result<bool> {
    definition.validate().map_err(extension_error)?;
    let installation = require_active_extension(db, &definition.extension)?;
    if !installation
        .manifest
        .subscriptions
        .iter()
        .any(|subscription| subscription.export == definition.export)
    {
        return Err(SqlError::InvalidSql(format!(
            "extension `{}` did not declare event export `{}`",
            definition.extension, definition.export
        )));
    }
    match &definition.source {
        EventSource::Database { .. } => {
            require_capability(&installation, ExtensionCapability::DatabaseEvents)?;
            let EventSource::Database { relation, .. } = &definition.source else {
                unreachable!("matched database event source");
            };
            if !installation
                .manifest
                .permissions
                .read_relations
                .iter()
                .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(relation))
            {
                return Err(SqlError::InvalidSql(format!(
                    "extension `{}` did not request read access to relation `{relation}`",
                    definition.extension
                )));
            }
            let queue = definition
                .delivery_queue
                .as_deref()
                .expect("validated database binding has a queue");
            require_queue_permission(
                &installation.manifest.permissions.publish_queues,
                queue,
                "publish",
            )?;
        }
        EventSource::Queue { queue, .. } => {
            require_capability(&installation, ExtensionCapability::QueueEvents)?;
            require_queue_permission(
                &installation.manifest.permissions.consume_queues,
                queue,
                "consume",
            )?;
        }
    }
    db.create_collection(EXTENSION_EVENT_BINDING_COLLECTION)?;
    let key = normalize_name(&definition.name);
    if db.get(EXTENSION_EVENT_BINDING_COLLECTION, &key)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(SqlError::InvalidSql(format!(
            "event subscription `{key}` already exists"
        )));
    }
    db.insert(
        EXTENSION_EVENT_BINDING_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(true)
}

pub fn load_event_binding(db: &BicDb, name: &str) -> Result<Option<EventBindingDefinition>> {
    load_record(
        db,
        EXTENSION_EVENT_BINDING_COLLECTION,
        &normalize_name(name),
    )
}

pub fn list_event_bindings(db: &BicDb) -> Result<Vec<EventBindingDefinition>> {
    list_records(db, EXTENSION_EVENT_BINDING_COLLECTION)
}

pub fn delete_event_binding(db: &mut BicDb, name: &str) -> Result<bool> {
    delete_record(
        db,
        EXTENSION_EVENT_BINDING_COLLECTION,
        &normalize_name(name),
    )
}

pub(crate) fn restore_event_binding(
    db: &mut BicDb,
    definition: EventBindingDefinition,
) -> Result<()> {
    definition.validate().map_err(extension_error)?;
    db.create_collection(EXTENSION_EVENT_BINDING_COLLECTION)?;
    let key = normalize_name(&definition.name);
    db.insert(
        EXTENSION_EVENT_BINDING_COLLECTION,
        Record::new(&key).with_metadata(serde_json::to_value(definition)?),
    )?;
    Ok(())
}

fn require_active_extension(db: &BicDb, name: &str) -> Result<ExtensionInstallation> {
    let installation = load_extension(db, name)?
        .ok_or_else(|| SqlError::InvalidSql(format!("extension `{name}` is not installed")))?;
    if installation.state != ExtensionState::Active {
        return Err(SqlError::InvalidSql(format!(
            "extension `{name}` is not active"
        )));
    }
    Ok(installation)
}

fn require_capability(
    installation: &ExtensionInstallation,
    capability: ExtensionCapability,
) -> Result<()> {
    if installation.manifest.capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(SqlError::InvalidSql(format!(
            "extension `{}` does not provide capability `{}`",
            installation.manifest.identity.name,
            capability.as_str()
        )))
    }
}

fn require_queue_permission(
    permissions: &BTreeSet<String>,
    queue: &str,
    operation: &str,
) -> Result<()> {
    if permissions
        .iter()
        .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(queue))
    {
        Ok(())
    } else {
        Err(SqlError::InvalidSql(format!(
            "extension did not request {operation} access to queue `{queue}`"
        )))
    }
}

fn load_record<T>(db: &BicDb, collection: &str, key: &str) -> Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    match db.get(collection, key) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn list_records<T>(db: &BicDb, collection: &str) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    let records = match db.scan_collection(collection) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect()
}

fn delete_record(db: &mut BicDb, collection: &str, key: &str) -> Result<bool> {
    match db.delete(collection, key) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn extension_error(error: bicdb_extension::ExtensionError) -> SqlError {
    SqlError::InvalidSql(error.to_string())
}

pub(crate) fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn normalize_name(value: &str) -> String {
    value.trim_matches('"').to_ascii_lowercase()
}

fn normalize_qualified_name(value: &str) -> String {
    value
        .split('.')
        .map(normalize_name)
        .collect::<Vec<_>>()
        .join(".")
}

fn required_option<'a>(options: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| SqlError::InvalidSql(format!("WITH option `{name}` is required")))
}

fn parse_bool_option(
    options: &BTreeMap<String, String>,
    name: &str,
    default: bool,
) -> Result<bool> {
    match options.get(name).map(|value| value.to_ascii_lowercase()) {
        None => Ok(default),
        Some(value) if matches!(value.as_str(), "true" | "on" | "yes" | "1") => Ok(true),
        Some(value) if matches!(value.as_str(), "false" | "off" | "no" | "0") => Ok(false),
        Some(value) => Err(SqlError::InvalidSql(format!(
            "option `{name}` expects a boolean, got `{value}`"
        ))),
    }
}

fn parse_route_auth(options: &BTreeMap<String, String>, kind: &str) -> Result<RouteAuth> {
    match options
        .get("auth")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
        .unwrap_or("public")
    {
        "public" => Ok(RouteAuth::Public),
        "authenticated" => Ok(RouteAuth::Authenticated),
        "rls" | "row_level_security" => Ok(RouteAuth::RowLevelSecurity),
        "admin" => Ok(RouteAuth::Admin),
        other => Err(SqlError::InvalidSql(format!(
            "unsupported {kind} auth `{other}`"
        ))),
    }
}

fn parse_u32_option(options: &BTreeMap<String, String>, name: &str, default: u32) -> Result<u32> {
    match options.get(name) {
        None => Ok(default),
        Some(value) => value.parse().map_err(|_| {
            SqlError::InvalidSql(format!("option `{name}` expects an unsigned integer"))
        }),
    }
}

fn parse_u64_option(options: &BTreeMap<String, String>, name: &str, default: u64) -> Result<u64> {
    match options.get(name) {
        None => Ok(default),
        Some(value) => value.parse().map_err(|_| {
            SqlError::InvalidSql(format!("option `{name}` expects an unsigned integer"))
        }),
    }
}

fn parse_http_methods(value: &str) -> Result<BTreeSet<HttpMethod>> {
    value
        .split(',')
        .map(|method| match method.trim().to_ascii_uppercase().as_str() {
            "GET" => Ok(HttpMethod::Get),
            "POST" => Ok(HttpMethod::Post),
            "PUT" => Ok(HttpMethod::Put),
            "PATCH" => Ok(HttpMethod::Patch),
            "DELETE" => Ok(HttpMethod::Delete),
            "HEAD" => Ok(HttpMethod::Head),
            "OPTIONS" => Ok(HttpMethod::Options),
            other => Err(SqlError::InvalidSql(format!(
                "unsupported HTTP method `{other}`"
            ))),
        })
        .collect()
}

fn parse_database_operations(value: &str) -> Result<BTreeSet<DatabaseOperation>> {
    value
        .split(',')
        .map(
            |operation| match operation.trim().to_ascii_uppercase().as_str() {
                "INSERT" => Ok(DatabaseOperation::Insert),
                "UPDATE" => Ok(DatabaseOperation::Update),
                "DELETE" => Ok(DatabaseOperation::Delete),
                other => Err(SqlError::InvalidSql(format!(
                    "unsupported database event `{other}`"
                ))),
            },
        )
        .collect()
}

struct SqlCursor<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> SqlCursor<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.trim(),
            offset: 0,
        }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }

    fn skip_space(&mut self) {
        while self
            .remaining()
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_space();
        let remaining = self.remaining();
        if remaining.len() < keyword.len()
            || !remaining[..keyword.len()].eq_ignore_ascii_case(keyword)
        {
            return false;
        }
        let boundary = remaining
            .as_bytes()
            .get(keyword.len())
            .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_');
        if !boundary {
            return false;
        }
        self.offset += keyword.len();
        true
    }

    fn consume_keywords(&mut self, keywords: &[&str]) -> bool {
        let saved = self.offset;
        for keyword in keywords {
            if !self.consume_keyword(keyword) {
                self.offset = saved;
                return false;
            }
        }
        true
    }

    fn peek_keyword(&mut self, keyword: &str) -> bool {
        let saved = self.offset;
        let matched = self.consume_keyword(keyword);
        self.offset = saved;
        matched
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            Err(SqlError::InvalidSql(format!(
                "expected keyword {keyword} near `{}`",
                self.remaining().trim()
            )))
        }
    }

    fn identifier(&mut self) -> Result<String> {
        self.skip_space();
        let remaining = self.remaining();
        if remaining.starts_with('"') {
            let mut output = String::new();
            let mut chars = remaining[1..].char_indices().peekable();
            while let Some((index, character)) = chars.next() {
                if character == '"' {
                    if chars.peek().is_some_and(|(_, next)| *next == '"') {
                        chars.next();
                        output.push('"');
                        continue;
                    }
                    self.offset += index + 2;
                    return Ok(output);
                }
                output.push(character);
            }
            return Err(SqlError::InvalidSql(
                "unterminated quoted identifier".to_string(),
            ));
        }
        let len = remaining
            .bytes()
            .take_while(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            .count();
        if len == 0 {
            return Err(SqlError::InvalidSql(format!(
                "expected identifier near `{}`",
                remaining.trim()
            )));
        }
        self.offset += len;
        Ok(remaining[..len].to_string())
    }

    fn qualified_identifier(&mut self) -> Result<String> {
        let mut parts = vec![self.identifier()?];
        loop {
            self.skip_space();
            if !self.remaining().starts_with('.') {
                break;
            }
            self.offset += 1;
            parts.push(self.identifier()?);
        }
        Ok(parts.join("."))
    }

    fn string_literal(&mut self) -> Result<String> {
        self.skip_space();
        if !self.remaining().starts_with('\'') {
            return Err(SqlError::InvalidSql(format!(
                "expected SQL string near `{}`",
                self.remaining().trim()
            )));
        }
        let remaining = &self.remaining()[1..];
        let mut output = String::new();
        let mut chars = remaining.char_indices().peekable();
        while let Some((index, character)) = chars.next() {
            if character == '\'' {
                if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                    chars.next();
                    output.push('\'');
                    continue;
                }
                self.offset += index + 2;
                return Ok(output);
            }
            output.push(character);
        }
        Err(SqlError::InvalidSql(
            "unterminated SQL string literal".to_string(),
        ))
    }

    fn parenthesized(&mut self) -> Result<String> {
        self.skip_space();
        if !self.remaining().starts_with('(') {
            return Err(SqlError::InvalidSql(format!(
                "expected `(` near `{}`",
                self.remaining().trim()
            )));
        }
        let start = self.offset + 1;
        let mut depth = 1usize;
        let mut quote = false;
        let bytes = self.input.as_bytes();
        let mut index = start;
        while index < bytes.len() {
            match bytes[index] {
                b'\'' => {
                    if quote && bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                        continue;
                    }
                    quote = !quote;
                }
                b'(' if !quote => depth += 1,
                b')' if !quote => {
                    depth -= 1;
                    if depth == 0 {
                        let output = self.input[start..index].to_string();
                        self.offset = index + 1;
                        return Ok(output);
                    }
                }
                _ => {}
            }
            index += 1;
        }
        Err(SqlError::InvalidSql(
            "unterminated parenthesized value".to_string(),
        ))
    }

    fn options(&mut self) -> Result<BTreeMap<String, String>> {
        self.expect_keyword("WITH")?;
        let body = self.parenthesized()?;
        let mut options = BTreeMap::new();
        for option in crate::split_top_level_commas_nested(&body) {
            let (name, raw) = option.split_once('=').ok_or_else(|| {
                SqlError::InvalidSql(format!("extension option `{option}` requires `=`"))
            })?;
            let name = normalize_name(name.trim());
            let raw = raw.trim();
            let value = if raw.starts_with('\'') {
                let mut value_cursor = SqlCursor::new(raw);
                let value = value_cursor.string_literal()?;
                value_cursor.finish()?;
                value
            } else {
                raw.to_string()
            };
            if options.insert(name.clone(), value).is_some() {
                return Err(SqlError::InvalidSql(format!(
                    "duplicate extension option `{name}`"
                )));
            }
        }
        Ok(options)
    }

    fn finish(&mut self) -> Result<()> {
        self.skip_space();
        if self.remaining().starts_with(';') {
            self.offset += 1;
            self.skip_space();
        }
        if self.remaining().is_empty() {
            Ok(())
        } else {
            Err(SqlError::InvalidSql(format!(
                "unexpected extension DDL tail `{}`",
                self.remaining().trim()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use bicdb_extension::{
        EventSubscriptionRegistration, ExtensionIdentity, ExtensionLimits, ExtensionPermissions,
        HttpRouteRegistration, EXTENSION_ABI_VERSION,
    };

    use super::*;

    fn manifest() -> ExtensionManifest {
        ExtensionManifest {
            identity: ExtensionIdentity {
                name: "instant_rest".to_string(),
                version: "1.0.0".to_string(),
                abi_version: EXTENSION_ABI_VERSION,
                description: String::new(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::from([
                ExtensionCapability::HttpRoutes,
                ExtensionCapability::DatabaseEvents,
                ExtensionCapability::QueueEvents,
            ]),
            permissions: ExtensionPermissions {
                read_relations: BTreeSet::from(["public.patients".to_string()]),
                publish_queues: BTreeSet::from(["patients.changed".to_string()]),
                consume_queues: BTreeSet::from(["patients.work".to_string()]),
                ..ExtensionPermissions::default()
            },
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![HttpRouteRegistration {
                name: "resource".to_string(),
                method: HttpMethod::Get,
                path: "/resources".to_string(),
                export: "handle_resource".to_string(),
                auth: RouteAuth::RowLevelSecurity,
            }],
            subscriptions: vec![
                EventSubscriptionRegistration {
                    name: "patient_changes".to_string(),
                    source: EventSource::Database {
                        relation: "public.patients".to_string(),
                        operations: BTreeSet::from([DatabaseOperation::Insert]),
                    },
                    export: "on_patient_changed".to_string(),
                    max_attempts: 5,
                    visibility_timeout_ms: 30_000,
                },
                EventSubscriptionRegistration {
                    name: "patient_work".to_string(),
                    source: EventSource::Queue {
                        queue: "patients.work".to_string(),
                        group: "instant-rest".to_string(),
                    },
                    export: "on_patient_work".to_string(),
                    max_attempts: 5,
                    visibility_timeout_ms: 30_000,
                },
            ],
            observability: vec![],
            application: None,
        }
    }

    fn install(db: &mut BicDb) {
        let manifest = manifest();
        let manifest_json = serde_json::to_string(&manifest).unwrap();
        let escaped = manifest_json
            .as_bytes()
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let module = wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 2 1024)
                (data (i32.const 1024) "{escaped}")
                (func (export "bicdb_extension_abi_version") (result i32) i32.const 1)
                (func (export "bicdb_extension_manifest_ptr") (result i32) i32.const 1024)
                (func (export "bicdb_extension_manifest_len") (result i32)
                    i32.const {manifest_len})
                (func (export "bicdb_extension_alloc") (param i32) (result i32)
                    i32.const 32768)
                (func (export "bicdb_extension_dealloc") (param i32 i32))
                (func (export "bicdb_extension_invoke") (param i32 i32) (result i64)
                    i64.const 0))"#,
            manifest_len = manifest_json.len(),
        ))
        .unwrap();
        let store = bicdb_extension::host::ExtensionPackageStore::open(
            db.data_path().join("extensions/packages"),
        )
        .unwrap();
        let hash = store.install(&module, 64 * 1024 * 1024).unwrap();
        install_extension(
            db,
            ExtensionInstallation {
                manifest,
                module_sha256: hash,
                state: ExtensionState::Staged,
                installed_at_ms: 1,
                activation: None,
                last_error: None,
            },
            false,
        )
        .unwrap();
        activate_extension_single_node(db, "instant_rest").unwrap();
    }

    #[test]
    fn parses_executable_extension_install() {
        let manifest_json = serde_json::to_string(&manifest())
            .unwrap()
            .replace('\'', "''");
        let sql = format!(
            "CREATE EXTENSION instant_rest FROM MODULE '{}' MANIFEST '{}'",
            "a".repeat(64),
            manifest_json
        );
        let Some(ExtensionCatalogDdl::Install {
            name,
            module_sha256,
            ..
        }) = parse_extension_catalog_ddl(&sql).unwrap()
        else {
            panic!("expected install");
        };
        assert_eq!(name, "instant_rest");
        assert_eq!(module_sha256, "a".repeat(64));

        let upgrade = format!(
            "ALTER EXTENSION instant_rest UPDATE FROM MODULE '{}' MANIFEST '{}'",
            "b".repeat(64),
            manifest_json
        );
        let Some(ExtensionCatalogDdl::UpgradeSingleNode {
            name,
            module_sha256,
            ..
        }) = parse_extension_catalog_ddl(&upgrade).unwrap()
        else {
            panic!("expected upgrade");
        };
        assert_eq!(name, "instant_rest");
        assert_eq!(module_sha256, "b".repeat(64));
    }

    #[test]
    fn resource_and_event_ddl_parse_to_typed_definitions() {
        let resource = parse_extension_catalog_ddl(
            "CREATE RESOURCE patients USING EXTENSION instant_rest FROM TABLE public.patients \
             WITH (path = '/patients', methods = 'GET,POST', auth = 'rls', openapi = true)",
        )
        .unwrap()
        .unwrap();
        let ExtensionCatalogDdl::CreateResource { definition, .. } = resource else {
            panic!("expected resource");
        };
        assert_eq!(definition.methods.len(), 2);

        let event = parse_extension_catalog_ddl(
            "CREATE EVENT SUBSCRIPTION patient_worker USING EXTENSION instant_rest \
             ON TABLE public.patients EVENTS (INSERT, UPDATE) QUEUE 'patients.changed' \
             EXECUTE 'on_patient_changed' WITH (max_attempts = 7)",
        )
        .unwrap()
        .unwrap();
        let ExtensionCatalogDdl::CreateEventBinding { definition, .. } = event else {
            panic!("expected event");
        };
        assert_eq!(definition.max_attempts, 7);
    }

    #[test]
    fn lifecycle_and_dependencies_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(dir.path()).unwrap();
            install(&mut db);
            save_rest_resource(
                &mut db,
                RestResourceDefinition {
                    name: "patients".to_string(),
                    extension: "instant_rest".to_string(),
                    relation: "public.patients".to_string(),
                    path: "/patients".to_string(),
                    export: "handle_resource".to_string(),
                    methods: BTreeSet::from([HttpMethod::Get]),
                    auth: RouteAuth::RowLevelSecurity,
                    openapi: true,
                    enabled: true,
                },
                false,
            )
            .unwrap();
        }
        {
            let mut db = BicDb::open(dir.path()).unwrap();
            assert_eq!(
                load_extension(&db, "instant_rest").unwrap().unwrap().state,
                ExtensionState::Active
            );
            assert_eq!(list_rest_resources(&db).unwrap().len(), 1);
            assert!(delete_extension(&mut db, "instant_rest", false).is_err());
            assert!(delete_extension(&mut db, "instant_rest", true).unwrap());
            assert!(list_rest_resources(&db).unwrap().is_empty());
        }
    }

    #[test]
    fn cluster_activation_without_quorum_fails_closed() {
        let activation = ExtensionActivation {
            catalog_generation: 1,
            topology_generation: 9,
            ready_nodes: BTreeSet::from(["node-a".to_string()]),
            required_nodes: BTreeSet::from(["node-a".to_string()]),
            quorum_committed: false,
            activated_at_ms: 1,
        };
        assert!(activation.validate().is_err());
    }
}
