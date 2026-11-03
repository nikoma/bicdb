//! Pre-signing expansion of compact package declarations.
//!
//! JSON producers (for example a code generator) write an unsigned package
//! with an optional top-level `tables` list and `raw_sql` entries that carry
//! only `id`, `sql`, and `parameters`. [`prepare_unsigned_package`] rewrites
//! that document into the ordinary signed manifest form before hashing:
//! `tables` become schema-only resource contracts with tenant row-level
//! security, relation permissions, and an additive migration plan, and raw SQL
//! authority (hash, relations, routines, actions) is derived from the parser
//! the runtime itself uses. The runtime never sees `tables`; it verifies the
//! expanded contracts exactly as if they had been written by hand.
use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::{AppRuntimeError, Result};

/// Compact table declaration accepted by [`prepare_unsigned_package`].
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDeclarationV1 {
    /// SQL table name; also the resource contract name.
    pub name: String,
    #[serde(default = "default_primary_key")]
    pub primary_key: String,
    pub columns: Vec<TableColumnV1>,
    /// Tenant column, added as a required string column when absent.
    #[serde(default = "default_tenant_column")]
    pub tenant_column: String,
    /// Optional owner column compared with the caller's user id.
    #[serde(default)]
    pub owner_column: Option<String>,
    /// `write` (default): only the owner inserts, updates, and deletes; the
    /// whole tenant reads. `all`: reads are owner-only too.
    #[serde(default)]
    pub owner_policy: Option<String>,
    /// Roles required for every read and write of the table.
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub indexes: Vec<TableIndexV1>,
    /// Bump only for a structural change that adds no column or index.
    #[serde(default)]
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableColumnV1 {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableIndexV1 {
    pub name: String,
    pub columns: Vec<String>,
}

fn default_primary_key() -> String {
    "id".to_string()
}

fn default_tenant_column() -> String {
    "tenant_id".to_string()
}

fn invalid(message: impl Into<String>) -> AppRuntimeError {
    AppRuntimeError::InvalidPackage(message.into())
}

fn field_type(name: &str) -> Result<&'static str> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "string" | "text" | "varchar" => "string",
        "uuid" => "uuid",
        "int" | "integer" | "bigint" | "int64" | "int8" => "int64",
        "float" | "double" | "float64" | "float8" => "float64",
        "decimal" | "numeric" | "money" => "decimal",
        "bool" | "boolean" => "bool",
        "timestamp" | "timestamptz" | "datetime" => "timestamp",
        "date" => "date",
        "json" | "jsonb" => "json",
        "bytes" | "bytea" => "bytes",
        other => return Err(invalid(format!("unsupported table column type `{other}`"))),
    })
}

fn object<'a>(value: &'a mut Value, key: &str) -> Result<&'a mut Map<String, Value>> {
    let parent = value
        .as_object_mut()
        .ok_or_else(|| invalid(format!("package `{key}` parent must be an object")))?;
    parent
        .entry(key.to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| invalid(format!("package `{key}` must be an object")))
}

fn array<'a>(value: &'a mut Map<String, Value>, key: &str) -> Result<&'a mut Vec<Value>> {
    value
        .entry(key.to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| invalid(format!("manifest `{key}` must be an array")))
}

fn push_unique(values: &mut Vec<Value>, value: &str) {
    if !values.iter().any(|candidate| candidate == value) {
        values.push(Value::String(value.to_string()));
    }
}

/// Expand `tables` and complete raw SQL authority in an unsigned package
/// document. Idempotent for documents without `tables`.
pub fn prepare_unsigned_package(package: &mut Value) -> Result<()> {
    let tables: Vec<TableDeclarationV1> = match package
        .as_object_mut()
        .ok_or_else(|| invalid("package must be a JSON object"))?
        .remove("tables")
    {
        Some(tables) => serde_json::from_value(tables)
            .map_err(|error| invalid(format!("invalid `tables` declaration: {error}")))?,
        None => Vec::new(),
    };
    let manifest = object(package, "manifest")?;
    if !tables.is_empty() {
        let capabilities = array(manifest, "capabilities")?;
        push_unique(capabilities, "database");
        push_unique(capabilities, "transactions");
        let permissions = manifest
            .entry("permissions".to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| invalid("manifest `permissions` must be an object"))?;
        for key in ["read_relations", "write_relations"] {
            let relations = array(permissions, key)?;
            for table in &tables {
                push_unique(relations, &table.name);
            }
        }
    }
    let application = manifest
        .get_mut("application")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("package lacks manifest.application"))?;
    if !tables.is_empty() {
        expand_tables(application, &tables)?;
    }
    complete_raw_sql(application)
}

fn expand_tables(
    application: &mut Map<String, Value>,
    tables: &[TableDeclarationV1],
) -> Result<()> {
    if application
        .get("migrations")
        .and_then(Value::as_array)
        .is_some_and(|migrations| !migrations.is_empty())
    {
        return Err(invalid(
            "`tables` generates the migration plan; remove hand-written `migrations`",
        ));
    }
    let mut names = BTreeSet::new();
    let mut resources = Vec::new();
    for table in tables {
        if !names.insert(table.name.to_ascii_lowercase()) {
            return Err(invalid(format!("duplicate table `{}`", table.name)));
        }
        resources.push(table_resource(table)?);
    }
    let existing = array(application, "resources")?;
    for resource in &resources {
        if existing.iter().any(|candidate| {
            candidate["relation"].as_str().is_some_and(|relation| {
                relation.eq_ignore_ascii_case(resource["relation"].as_str().unwrap())
            }) || candidate["name"] == resource["name"]
        }) {
            return Err(invalid(format!(
                "table `{}` duplicates a declared resource",
                resource["name"].as_str().unwrap_or_default()
            )));
        }
    }
    let generated = resources.clone();
    existing.extend(resources);
    let all_resources = existing.clone();
    let permissions = array(application, "relation_permissions")?;
    for (table, resource) in tables.iter().zip(&generated) {
        if permissions
            .iter()
            .any(|permission| permission["relation"] == table.name.as_str())
        {
            continue;
        }
        let columns = resource["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|field| field["name"].clone())
            .collect::<Vec<_>>();
        permissions.push(json!({
            "relation": table.name,
            "actions": ["select", "insert", "update", "delete"],
            "readable_columns": [],
            "writable_columns": columns,
        }));
    }
    let mut contract_versions = Map::new();
    let mut schema_version = 0_u64;
    let mut steps = Vec::new();
    for resource in &all_resources {
        let name = resource["name"].as_str().unwrap_or_default().to_string();
        let version = resource["schema_version"].as_u64().unwrap_or(1);
        schema_version = schema_version.saturating_add(version);
        contract_versions.insert(name.clone(), json!(version));
        steps.push(json!({"kind": "validate_contract", "resource": name}));
    }
    application.insert(
        "migrations".to_string(),
        json!([{
            "version": 1,
            "schema_version": schema_version,
            "contract_versions": contract_versions,
            "forward": steps,
            "compatibility_checks": ["additive"],
            "activation_boundary": "catalog-swap",
            "rollback": steps,
        }]),
    );
    Ok(())
}

fn table_resource(table: &TableDeclarationV1) -> Result<Value> {
    let mut columns = table.columns.clone();
    for (column, label) in [
        (Some(&table.tenant_column), "tenant"),
        (table.owner_column.as_ref(), "owner"),
    ] {
        let Some(column) = column else { continue };
        match columns.iter().find(|candidate| &candidate.name == column) {
            Some(existing)
                if existing.nullable || field_type(&existing.column_type)? != "string" =>
            {
                return Err(invalid(format!(
                    "table `{}` {label} column `{column}` must be a required string",
                    table.name
                )));
            }
            Some(_) => {}
            None => columns.push(TableColumnV1 {
                name: column.clone(),
                column_type: "string".to_string(),
                nullable: false,
            }),
        }
    }
    let mut seen = BTreeSet::new();
    let mut fields = Vec::new();
    for column in &columns {
        if !seen.insert(column.name.as_str()) {
            return Err(invalid(format!(
                "table `{}` repeats column `{}`",
                table.name, column.name
            )));
        }
        let mut field =
            json!({"name": column.name, "field_type": field_type(&column.column_type)?});
        if column.nullable {
            field["nullable"] = json!(true);
        }
        fields.push(field);
    }
    if !seen.contains(table.primary_key.as_str()) {
        return Err(invalid(format!(
            "table `{}` primary key `{}` is not a column",
            table.name, table.primary_key
        )));
    }
    for index in &table.indexes {
        if index.columns.is_empty()
            || index
                .columns
                .iter()
                .any(|column| !seen.contains(column.as_str()))
        {
            return Err(invalid(format!(
                "table `{}` index `{}` must name existing columns",
                table.name, index.name
            )));
        }
    }
    let tenant = format!(
        "{} = NULLIF(current_setting('bicdb.current_tenant', true), '')",
        table.tenant_column
    );
    let (read, write) = match (&table.owner_column, table.owner_policy.as_deref()) {
        (None, None) => (tenant.clone(), tenant.clone()),
        (None, Some(_)) => {
            return Err(invalid(format!(
                "table `{}` owner_policy requires owner_column",
                table.name
            )));
        }
        (Some(owner), policy) => {
            let owned = format!(
                "{tenant} AND {owner} = NULLIF(current_setting('bicdb.current_user', true), '')"
            );
            match policy.unwrap_or("write") {
                "write" => (tenant.clone(), owned),
                "all" => (owned.clone(), owned),
                other => {
                    return Err(invalid(format!(
                        "table `{}` owner_policy `{other}` must be `write` or `all`",
                        table.name
                    )));
                }
            }
        }
    };
    let version = (columns.len() + table.indexes.len()) as u64 + table.revision;
    let mut resource = json!({
        "version": 1,
        "name": table.name,
        "relation": table.name,
        "schema_version": version,
        "schema_only": true,
        "primary_key": table.primary_key,
        "fields": fields,
        "tenant_field": table.tenant_column,
        "required_roles": table.roles,
        "policy": {"version": 1, "sql": {
            "select_using": read,
            "insert_with_check": write,
            "update_using": write,
            "update_with_check": write,
            "delete_using": write,
        }},
        "list_route": format!("/_tables/{}", table.name),
        "item_route": format!("/_tables/{}/{{{}}}", table.name, table.primary_key),
        "openapi": {},
    });
    if !table.indexes.is_empty() {
        resource["indexes"] = Value::Array(
            table
                .indexes
                .iter()
                .map(|index| json!({"name": index.name, "fields": index.columns}))
                .collect(),
        );
    }
    let digest = Sha256::digest(serde_json::to_vec(&resource)?);
    resource["contract_sha256"] = json!(format!("{digest:x}"));
    Ok(resource)
}

fn complete_raw_sql(application: &mut Map<String, Value>) -> Result<()> {
    let Some(statements) = application.get_mut("raw_sql").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for statement in statements {
        let id = statement["id"].as_str().unwrap_or_default().to_string();
        let sql = statement["sql"]
            .as_str()
            .ok_or_else(|| invalid(format!("raw SQL `{id}` lacks `sql`")))?
            .to_string();
        let analysis = crate::bounded_sql::analyze_bounded_sql(&sql)
            .map_err(|error| invalid(format!("raw SQL `{id}` is not bounded: {error}")))?;
        let object = statement
            .as_object_mut()
            .ok_or_else(|| invalid("raw SQL entries must be objects"))?;
        object
            .entry("sha256")
            .or_insert_with(|| json!(format!("{:x}", Sha256::digest(sql.as_bytes()))));
        object
            .entry("relations")
            .or_insert_with(|| json!(analysis.relations));
        object
            .entry("routines")
            .or_insert_with(|| json!(analysis.routines));
        object
            .entry("actions")
            .or_insert(serde_json::to_value(&analysis.actions)?);
        if !object.contains_key("parameters") {
            if analysis.parameter_count != 0 {
                return Err(invalid(format!(
                    "raw SQL `{id}` has {} placeholders and needs `parameters` types",
                    analysis.parameter_count
                )));
            }
            object.insert("parameters".to_string(), json!([]));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "package_tables_tests.rs"]
mod tests;
