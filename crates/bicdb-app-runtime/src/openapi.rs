//! Deterministic OpenAPI 3.1 generation from signed ABI-v2 route and resource
//! contracts. The host generates this document from the exact active
//! contracts, so documentation cannot silently grant operations the runtime
//! does not enforce.

use std::collections::{BTreeMap, BTreeSet};

use bicdb_extension::abi_v2::ValidationRuleKind;
use bicdb_extension::abi_v2::{
    ApplicationGeometryTypeV1, ApplicationRouteParameterTypeV1, ContractField, FieldType,
    ResourceContractV1, ResourceOperation,
};
use bicdb_extension::{ExtensionManifest, HttpMethod};
use serde_json::{json, Map, Value};

use crate::{AppRuntimeError, Result};

pub fn generate_openapi(manifest: &ExtensionManifest) -> Result<Value> {
    manifest.validate()?;
    let application = manifest.application.as_deref().ok_or_else(|| {
        AppRuntimeError::InvalidPackage("OpenAPI generation requires ABI v2".to_string())
    })?;
    let resources = application
        .resources
        .iter()
        .map(|contract| (contract.name.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let mut paths = Map::new();
    for route in &application.routes {
        let operation = match (
            route
                .resource
                .as_deref()
                .and_then(|name| resources.get(name)),
            route.operation,
        ) {
            (Some(contract), Some(operation)) => {
                resource_operation(route.name.as_str(), contract, operation)
            }
            _ => json!({
                "operationId": route.name,
                "responses": {
                    "200": { "description": "Application response" },
                    "default": { "$ref": "#/components/responses/Problem" }
                },
                "x-bicdb-export": route.export,
            }),
        };
        paths
            .entry(route.template.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("path entry created as object")
            .insert(method_name(route.method).to_string(), operation);
    }
    let schemas = application
        .resources
        .iter()
        .map(|contract| {
            (
                contract.name.clone(),
                Value::Object(resource_schema(contract, None)),
            )
        })
        .collect::<Map<_, _>>();
    Ok(json!({
        "openapi": "3.1.0",
        "info": {
            "title": manifest.identity.name,
            "version": manifest.identity.version,
        },
        "paths": paths,
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            },
            "schemas": schemas,
            "responses": {
                "Problem": {
                    "description": "Structured BicDB application error",
                    "content": {
                        "application/problem+json": {
                            "schema": {
                                "type": "object",
                                "required": ["code", "message"],
                                "properties": {
                                    "code": { "type": "string" },
                                    "message": { "type": "string" },
                                    "trace_id": { "type": "string" },
                                    "retryable": { "type": "boolean" }
                                }
                            }
                        }
                    }
                }
            }
        },
        "security": [{ "bearerAuth": [] }],
        "x-bicdb-abi": application.abi_version,
        "x-bicdb-application-profile": application.application_profile,
        "x-bicdb-package-sha256": application.package.package_sha256,
    }))
}

fn resource_operation(
    operation_id: &str,
    contract: &ResourceContractV1,
    operation: ResourceOperation,
) -> Value {
    let metadata = contract
        .operation_metadata
        .iter()
        .find(|metadata| metadata.operation == operation);
    let operation_id = metadata
        .map(|metadata| metadata.operation_id.as_str())
        .unwrap_or(operation_id);
    let optimistic_version_fields = contract.version_field.iter().cloned();
    let update_fields: BTreeSet<String> = contract
        .update_fields
        .iter()
        .cloned()
        .chain(optimistic_version_fields.clone())
        .collect();
    let upsert_fields = contract
        .create_fields
        .iter()
        .cloned()
        .chain(update_fields.iter().cloned())
        .collect();
    let request_fields = match operation {
        ResourceOperation::Create => Some(&contract.create_fields),
        ResourceOperation::Upsert => Some(&upsert_fields),
        ResourceOperation::Update => Some(&update_fields),
        _ => None,
    };
    let request_body = request_fields.map(|fields| {
        json!({
            "required": true,
            "content": {
                "application/json": {
                    "schema": Value::Object(resource_schema(contract, Some(fields)))
                }
            }
        })
    });
    let mut parameters = Vec::new();
    if matches!(
        operation,
        ResourceOperation::Get
            | ResourceOperation::Upsert
            | ResourceOperation::Update
            | ResourceOperation::Delete
            | ResourceOperation::Restore
    ) {
        parameters.push(json!({
            "name": contract.primary_key,
            "in": "path",
            "required": true,
            "schema": field_schema(
                contract
                    .fields
                    .iter()
                    .find(|field| field.name == contract.primary_key)
                    .expect("validated contract primary key")
            )
        }));
    }
    if operation == ResourceOperation::List {
        parameters.extend([
            json!({"name":"limit","in":"query","schema":{"type":"integer","minimum":1,"maximum":1000}}),
            json!({"name":"offset","in":"query","schema":{"type":"integer","minimum":0}}),
            json!({"name":"sort","in":"query","schema":{"type":"string"}}),
            json!({
                "name":"scope",
                "in":"query",
                "schema":{
                    "type":"string",
                    "enum":["active", "all", "deleted"],
                    "default": contract
                        .list_defaults
                        .as_ref()
                        .map(|defaults| match defaults.scope {
                            bicdb_extension::abi_v2::ResourceRecordScope::Active => "active",
                            bicdb_extension::abi_v2::ResourceRecordScope::All => "all",
                            bicdb_extension::abi_v2::ResourceRecordScope::Deleted => "deleted",
                        })
                        .unwrap_or("active")
                }
            }),
        ]);
        if !contract.search_fields.is_empty() {
            parameters.push(json!({"name":"search","in":"query","schema":{"type":"string"}}));
        }
        if contract.filter_contracts.is_empty() {
            for filter in &contract.filters {
                parameters.push(json!({
                    "name": format!("filter[{filter}]"),
                    "in": "query",
                    "schema": contract
                        .fields
                        .iter()
                        .find(|field| field.name == *filter)
                        .map(field_schema)
                        .unwrap_or_else(|| json!({}))
                }));
            }
        } else {
            for filter in &contract.filter_contracts {
                match &filter.filter {
                    bicdb_extension::abi_v2::ResourceFilterKind::Overlaps {
                        start_field,
                        end_field,
                    } => {
                        for (suffix, field) in [("start", start_field), ("end", end_field)] {
                            parameters.push(json!({
                                "name": format!("{}_{suffix}", filter.query_name),
                                "in": "query",
                                "schema": contract
                                    .fields
                                    .iter()
                                    .find(|candidate| candidate.name == *field)
                                    .map(field_schema)
                                    .unwrap_or_else(|| json!({}))
                            }));
                        }
                    }
                    bicdb_extension::abi_v2::ResourceFilterKind::Exists { .. } => {
                        parameters.push(json!({
                            "name": filter.query_name,
                            "in": "query",
                            "schema": {"type":"boolean"}
                        }));
                    }
                    bicdb_extension::abi_v2::ResourceFilterKind::JsonExists { .. } => {
                        parameters.push(json!({
                            "name": filter.query_name,
                            "in": "query",
                            "schema": {"type":"boolean"}
                        }));
                    }
                    bicdb_extension::abi_v2::ResourceFilterKind::JsonExact {
                        value_type, ..
                    }
                    | bicdb_extension::abi_v2::ResourceFilterKind::JsonContains {
                        value_type,
                        ..
                    }
                    | bicdb_extension::abi_v2::ResourceFilterKind::JsonMinimum {
                        value_type, ..
                    }
                    | bicdb_extension::abi_v2::ResourceFilterKind::RelationExact {
                        value_type,
                        ..
                    }
                    | bicdb_extension::abi_v2::ResourceFilterKind::RelationContains {
                        value_type,
                        ..
                    }
                    | bicdb_extension::abi_v2::ResourceFilterKind::RelationMinimum {
                        value_type,
                        ..
                    } => {
                        parameters.push(json!({
                            "name": filter.query_name,
                            "in": "query",
                            "schema": carrier_filter_schema(value_type)
                        }));
                    }
                    bicdb_extension::abi_v2::ResourceFilterKind::Exact { field }
                    | bicdb_extension::abi_v2::ResourceFilterKind::Contains { field }
                    | bicdb_extension::abi_v2::ResourceFilterKind::Minimum { field } => {
                        parameters.push(json!({
                            "name": filter.query_name,
                            "in": "query",
                            "schema": contract
                                .fields
                                .iter()
                                .find(|candidate| candidate.name == *field)
                                .map(field_schema)
                                .unwrap_or_else(|| json!({}))
                        }));
                    }
                }
            }
        }
    }
    if matches!(
        operation,
        ResourceOperation::Create | ResourceOperation::Action
    ) {
        if let Some(idempotency) = &contract.idempotency {
            parameters.push(json!({
                "name": idempotency.header,
                "in": "header",
                "required": true,
                "schema": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": idempotency.max_key_bytes,
                },
                "description": format!(
                    "Identity-scoped retry key retained for {} seconds.",
                    idempotency.ttl_seconds
                )
            }));
        }
    }
    let success = match operation {
        ResourceOperation::Create => "201",
        ResourceOperation::Delete => "204",
        _ => "200",
    };
    let mut success_response = json!({
        "description": format!("{operation:?} succeeded"),
        "content": {
            "application/json": {
                "schema": if operation == ResourceOperation::List {
                    json!({"type":"array","items":{"$ref":format!("#/components/schemas/{}", contract.name)}})
                } else {
                    json!({"$ref":format!("#/components/schemas/{}", contract.name)})
                }
            }
        }
    });
    if operation == ResourceOperation::Delete {
        success_response
            .as_object_mut()
            .expect("success response is an object")
            .remove("content");
    }
    if matches!(operation, ResourceOperation::List | ResourceOperation::Get) {
        if let Some(cache) = &contract.cache {
            let visibility = if cache.private { "private" } else { "public" };
            success_response["headers"]["Cache-Control"] = json!({
                "description": "Compiler-signed resource cache policy.",
                "schema": {"type": "string"},
                "example": format!("{visibility}, max-age={}", cache.max_age_seconds),
            });
            if !cache.vary.is_empty() {
                success_response["headers"]["Vary"] = json!({
                    "description": "Request headers that partition cache entries.",
                    "schema": {"type": "string"},
                    "example": cache.vary.iter().cloned().collect::<Vec<_>>().join(", "),
                });
            }
        }
    }
    let responses = Value::Object(Map::from_iter([
        (success.to_string(), success_response),
        (
            "default".to_string(),
            json!({ "$ref": "#/components/responses/Problem" }),
        ),
    ]));
    let mut value = json!({
        "operationId": operation_id,
        "tags": metadata
            .map(|metadata| metadata.tags.iter().cloned().collect::<Vec<_>>())
            .filter(|tags| !tags.is_empty())
            .unwrap_or_else(|| vec![contract.name.clone()]),
        "parameters": parameters,
        "responses": responses,
        "x-bicdb-resource": contract.name,
        "x-bicdb-contract-sha256": contract.contract_sha256,
        "x-bicdb-required-roles": contract.required_roles,
        "x-bicdb-required-scopes": contract.required_scopes,
        "x-bicdb-idempotency": contract.idempotency,
        "x-bicdb-cache": contract.cache,
    });
    if let Some(metadata) = metadata.filter(|metadata| !metadata.summary.is_empty()) {
        value["summary"] = Value::String(metadata.summary.clone());
    }
    if let Some(request_body) = request_body {
        value
            .as_object_mut()
            .expect("operation is object")
            .insert("requestBody".to_string(), request_body);
    }
    if contract.version_field.is_some()
        && matches!(
            operation,
            ResourceOperation::Upsert
                | ResourceOperation::Update
                | ResourceOperation::Delete
                | ResourceOperation::Restore
        )
    {
        value["parameters"]
            .as_array_mut()
            .expect("parameters are an array")
            .push(json!({
                "name": "If-Match",
                "in": "header",
                "required": matches!(
                    operation,
                    ResourceOperation::Delete | ResourceOperation::Restore
                ),
                "schema": {"type":"string"},
                "description": "Quoted optimistic resource version"
            }));
    }
    value
}

fn resource_schema(
    contract: &ResourceContractV1,
    fields: Option<&std::collections::BTreeSet<String>>,
) -> Map<String, Value> {
    let selected = contract
        .fields
        .iter()
        .filter(|field| {
            fields
                .map(|fields| fields.contains(&field.name))
                .unwrap_or_else(|| !contract.redacted_fields.contains(&field.name))
        })
        .collect::<Vec<_>>();
    let mut properties = selected
        .iter()
        .map(|field| (field.name.clone(), field_schema(field)))
        .collect::<Map<_, _>>();
    for validation in &contract.validation {
        let Some(schema) = properties
            .get_mut(&validation.field)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        match &validation.rule {
            ValidationRuleKind::MinLength { value } => {
                schema.insert("minLength".to_string(), Value::from(*value));
            }
            ValidationRuleKind::MaxLength { value } => {
                schema.insert("maxLength".to_string(), Value::from(*value));
            }
            ValidationRuleKind::Minimum { value } => {
                if let Ok(value) = serde_json::from_str(value) {
                    schema.insert("minimum".to_string(), value);
                }
            }
            ValidationRuleKind::Maximum { value } => {
                if let Ok(value) = serde_json::from_str(value) {
                    schema.insert("maximum".to_string(), value);
                }
            }
            ValidationRuleKind::Range { minimum, maximum } => {
                if let Ok(value) = serde_json::from_str(minimum) {
                    schema.insert("minimum".to_string(), value);
                }
                if let Ok(value) = serde_json::from_str(maximum) {
                    schema.insert("maximum".to_string(), value);
                }
            }
            ValidationRuleKind::Email => {
                schema.insert("format".to_string(), Value::String("email".to_string()));
            }
            ValidationRuleKind::Pattern { expression } => {
                schema.insert("pattern".to_string(), Value::String(expression.clone()));
            }
            ValidationRuleKind::OneOf { values } => {
                schema.insert("enum".to_string(), Value::Array(values.clone()));
            }
        }
    }
    if fields.is_none() {
        for (field, roles) in &contract.read_roles {
            if let Some(schema) = properties.get_mut(field).and_then(Value::as_object_mut) {
                schema.insert(
                    "x-bicdb-required-roles".to_string(),
                    serde_json::to_value(roles).unwrap_or(Value::Null),
                );
            }
        }
    }
    let required = selected
        .iter()
        .filter(|field| {
            !field.nullable
                && !field.generated
                && field.default_json.is_none()
                && fields.is_none_or(|fields| {
                    contract.required_create_fields.contains(&field.name)
                        || fields != &contract.create_fields
                })
        })
        .map(|field| Value::String(field.name.clone()))
        .collect::<Vec<_>>();
    Map::from_iter([
        ("type".to_string(), Value::String("object".to_string())),
        ("additionalProperties".to_string(), Value::Bool(false)),
        ("properties".to_string(), Value::Object(properties)),
        ("required".to_string(), Value::Array(required)),
    ])
}

fn field_schema(field: &ContractField) -> Value {
    let mut schema = match field.field_type {
        FieldType::Bool => json!({"type":"boolean"}),
        FieldType::Int64 => json!({"type":"integer","format":"int64"}),
        FieldType::Float64 => json!({"type":"number","format":"double"}),
        FieldType::Decimal => json!({"type":"number"}),
        FieldType::String => json!({"type":"string"}),
        FieldType::Bytes => json!({"type":"string","contentEncoding":"base64"}),
        FieldType::Uuid => json!({"type":"string","format":"uuid"}),
        FieldType::Timestamp => json!({"type":"string","format":"date-time"}),
        FieldType::Date => json!({"type":"string","format":"date"}),
        FieldType::Json => json!({}),
        FieldType::Vector { dimensions } => {
            json!({"type":"array","items":{"type":"number"},"minItems":dimensions,"maxItems":dimensions})
        }
        FieldType::Geometry {
            srid,
            geometry_type,
        } => {
            let geometry_type = geometry_type.map(|kind| match kind {
                ApplicationGeometryTypeV1::Point => "Point",
                ApplicationGeometryTypeV1::LineString => "LineString",
                ApplicationGeometryTypeV1::Polygon => "Polygon",
            });
            json!({"type":"object","x-bicdb-geometry-srid":srid,"x-carrier-geometry-type":geometry_type})
        }
    };
    if field.nullable {
        schema["nullable"] = Value::Bool(true);
    }
    if let Some(default) = field
        .default_json
        .as_deref()
        .and_then(|value| serde_json::from_str(value).ok())
    {
        schema["default"] = default;
    }
    if field.generated {
        schema["readOnly"] = Value::Bool(true);
    }
    schema
}

fn carrier_filter_schema(value_type: &ApplicationRouteParameterTypeV1) -> Value {
    match value_type {
        ApplicationRouteParameterTypeV1::String => json!({"type":"string"}),
        ApplicationRouteParameterTypeV1::Int => json!({"type":"integer","format":"int64"}),
        ApplicationRouteParameterTypeV1::Float | ApplicationRouteParameterTypeV1::Decimal => {
            json!({"type":"number"})
        }
        ApplicationRouteParameterTypeV1::Bool => json!({"type":"boolean"}),
        ApplicationRouteParameterTypeV1::Timestamp => {
            json!({"type":"string","format":"date-time"})
        }
        ApplicationRouteParameterTypeV1::Date => json!({"type":"string","format":"date"}),
        ApplicationRouteParameterTypeV1::LocalDateTime => {
            json!({"type":"string","format":"local-date-time"})
        }
        ApplicationRouteParameterTypeV1::TimeZone => json!({"type":"string","format":"time-zone"}),
        ApplicationRouteParameterTypeV1::Uuid => json!({"type":"string","format":"uuid"}),
        ApplicationRouteParameterTypeV1::Enum { values } => {
            json!({"type":"string","enum":values})
        }
        ApplicationRouteParameterTypeV1::Json
        | ApplicationRouteParameterTypeV1::List { .. }
        | ApplicationRouteParameterTypeV1::Set { .. }
        | ApplicationRouteParameterTypeV1::Optional { .. }
        | ApplicationRouteParameterTypeV1::Object { .. }
        | ApplicationRouteParameterTypeV1::Map { .. }
        | ApplicationRouteParameterTypeV1::Vector { .. }
        | ApplicationRouteParameterTypeV1::Point
        | ApplicationRouteParameterTypeV1::LineString
        | ApplicationRouteParameterTypeV1::Polygon
        | ApplicationRouteParameterTypeV1::Null => json!({}),
    }
}

fn method_name(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "get",
        HttpMethod::Post => "post",
        HttpMethod::Put => "put",
        HttpMethod::Patch => "patch",
        HttpMethod::Delete => "delete",
        HttpMethod::Head => "head",
        HttpMethod::Options => "options",
    }
}
