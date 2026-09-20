//! Trusted unary gRPC provider for applications hosted by BicDB.
//!
//! The application signs method paths and protobuf schemas. Operator config
//! supplies only the credential-free origin and host-owned authentication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bicdb_app_runtime::{AppRuntimeError, GrpcProvider, Result};
use bicdb_extension::abi_v2::{ApplicationGrpcClientV1, ApplicationGrpcFieldV1};
use bytes::{Buf, BufMut, Bytes};
use serde_json::{Map, Number, Value};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Status};
use url::Url;

const MAX_MESSAGE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_SCHEMA_DEPTH: usize = 64;

#[derive(Clone)]
pub struct GrpcProviderConfig {
    pub application: String,
    pub provider: String,
    pub endpoint: Url,
    pub bearer_token: Option<String>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub ca_pem: Option<Vec<u8>>,
    pub allowed_methods: BTreeSet<String>,
    pub allow_insecure_http: bool,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for GrpcProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcProviderConfig")
            .field("application", &self.application)
            .field("provider", &self.provider)
            .field("endpoint", &self.endpoint)
            .field("bearer_token_configured", &self.bearer_token.is_some())
            .field(
                "client_identity_configured",
                &self.client_cert_pem.is_some(),
            )
            .field("custom_ca_configured", &self.ca_pem.is_some())
            .field("allowed_methods", &self.allowed_methods)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl GrpcProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("gRPC application", &self.application)?;
        validate_identifier("gRPC provider", &self.provider)?;
        if !matches!(self.endpoint.scheme(), "http" | "https")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
            || !matches!(self.endpoint.path(), "" | "/")
        {
            return provider_error(
                "gRPC endpoint must be a credential-free HTTP(S) origin without path, query, or fragment",
            );
        }
        if self.endpoint.scheme() == "http" && !self.allow_insecure_http {
            return provider_error("cleartext gRPC requires explicit operator policy");
        }
        if self.client_cert_pem.is_some() != self.client_key_pem.is_some() {
            return provider_error("gRPC client certificate and key must be configured together");
        }
        if self.endpoint.scheme() != "https"
            && (self.client_cert_pem.is_some() || self.ca_pem.is_some())
        {
            return provider_error("gRPC TLS identity and CA require an HTTPS endpoint");
        }
        if self.bearer_token.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > 64 * 1024
                || value.chars().any(|character| character.is_control())
        }) {
            return provider_error("gRPC bearer token is invalid");
        }
        if self.max_request_bytes == 0
            || self.max_request_bytes > MAX_MESSAGE_BYTES
            || self.max_response_bytes == 0
            || self.max_response_bytes > MAX_MESSAGE_BYTES
            || self.connect_timeout.is_zero()
            || self.connect_timeout > MAX_TIMEOUT
            || self.request_timeout.is_zero()
            || self.request_timeout > MAX_TIMEOUT
        {
            return provider_error("gRPC size or timeout policy is invalid");
        }
        for method in &self.allowed_methods {
            if !method.starts_with('/') || method.len() > 8 * 1024 || method.contains("..") {
                return provider_error("gRPC allowed-method policy is invalid");
            }
        }
        Ok(())
    }
}

struct Binding {
    config: Arc<GrpcProviderConfig>,
    channel: Channel,
}

pub struct ProductionGrpcProvider {
    runtime: Arc<tokio::runtime::Runtime>,
    bindings: Arc<BTreeMap<(String, String), Binding>>,
}

impl std::fmt::Debug for ProductionGrpcProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionGrpcProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionGrpcProvider {
    pub fn new(configs: Vec<GrpcProviderConfig>) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("bicdb-grpc-provider")
                .build()
                .map_err(|error| AppRuntimeError::Provider(error.to_string()))?,
        );
        let _entered = runtime.enter();
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.contains_key(&key) {
                return provider_error("duplicate application gRPC provider binding");
            }
            let endpoint = configured_endpoint(&config)?;
            bindings.insert(
                key,
                Binding {
                    channel: endpoint.connect_lazy(),
                    config: Arc::new(config),
                },
            );
        }
        drop(_entered);
        Ok(Self {
            runtime,
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        for binding in self.bindings.values() {
            let endpoint = configured_endpoint(&binding.config)?;
            block_on(&self.runtime, async move { endpoint.connect().await })
                .map_err(|_| AppRuntimeError::Provider("gRPC healthcheck failed".to_string()))?;
        }
        Ok(())
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&Binding> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "gRPC provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl GrpcProvider for ProductionGrpcProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        self.binding(application, provider).map(|_| ())
    }

    fn unary(
        &self,
        application: &str,
        provider: &str,
        method: &str,
        contract: &ApplicationGrpcClientV1,
        payload: Value,
        deadline_ms: u64,
        trace_id: &str,
    ) -> Result<Value> {
        let binding = self.binding(application, provider)?;
        let method_contract = contract.methods.get(method).ok_or_else(|| {
            AppRuntimeError::Provider("gRPC method is absent from the signed contract".to_string())
        })?;
        if !binding.config.allowed_methods.is_empty()
            && !binding
                .config
                .allowed_methods
                .contains(&method_contract.path)
        {
            return provider_error("gRPC method is outside operator policy");
        }
        let request_bytes = encode_message(contract, &method_contract.request_type, &payload, 0)?;
        if request_bytes.len() as u64 > contract.max_request_bytes
            || request_bytes.len() as u64 > binding.config.max_request_bytes
        {
            return provider_error("gRPC request exceeds provider bounds");
        }
        let timeout = Duration::from_millis(
            deadline_ms.min(
                binding
                    .config
                    .request_timeout
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
        );
        let operation_deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| AppRuntimeError::Provider("gRPC deadline overflow".to_string()))?;
        let path = method_contract
            .path
            .parse::<tonic::codegen::http::uri::PathAndQuery>()
            .map_err(|_| AppRuntimeError::Provider("gRPC method path is invalid".to_string()))?;
        let attempts = method_contract.retries.saturating_add(1);
        let mut last = None;
        for _ in 0..attempts {
            let remaining = operation_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                last = Some(tonic::Code::DeadlineExceeded);
                break;
            }
            let mut request = Request::new(Bytes::from(request_bytes.clone()));
            request.set_timeout(remaining);
            request.metadata_mut().insert(
                "x-bicdb-trace-id",
                MetadataValue::try_from(trace_id).map_err(|_| {
                    AppRuntimeError::Provider("gRPC trace id is invalid".to_string())
                })?,
            );
            if let Some(token) = &binding.config.bearer_token {
                request.metadata_mut().insert(
                    "authorization",
                    MetadataValue::try_from(format!("Bearer {token}")).map_err(|_| {
                        AppRuntimeError::Provider("gRPC bearer metadata is invalid".to_string())
                    })?,
                );
            }
            let channel = binding.channel.clone();
            let path = path.clone();
            let response = block_on(&self.runtime, async move {
                let mut grpc = tonic::client::Grpc::new(channel)
                    .max_encoding_message_size(
                        usize::try_from(MAX_MESSAGE_BYTES).unwrap_or(usize::MAX),
                    )
                    .max_decoding_message_size(
                        usize::try_from(MAX_MESSAGE_BYTES).unwrap_or(usize::MAX),
                    );
                tokio::time::timeout(remaining, async {
                    grpc.ready()
                        .await
                        .map_err(|error| Status::unavailable(error.to_string()))?;
                    grpc.unary(request, path, RawCodec).await
                })
                .await
                .map_err(|_| Status::deadline_exceeded("provider deadline exceeded"))?
            });
            match response {
                Ok(response) => {
                    let bytes = response.into_inner();
                    if bytes.len() as u64 > contract.max_response_bytes
                        || bytes.len() as u64 > binding.config.max_response_bytes
                    {
                        return provider_error("gRPC response exceeds provider bounds");
                    }
                    return decode_message(contract, &method_contract.response_type, &bytes, 0);
                }
                Err(error) => last = Some(error.code()),
            }
        }
        Err(AppRuntimeError::Provider(format!(
            "gRPC request failed with status {:?}",
            last.expect("at least one gRPC attempt")
        )))
    }
}

fn configured_endpoint(config: &GrpcProviderConfig) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(config.endpoint.as_str().to_string())
        .map_err(|_| AppRuntimeError::Provider("gRPC endpoint is invalid".to_string()))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout);
    if config.endpoint.scheme() == "https" {
        let host = config.endpoint.host_str().expect("validated gRPC host");
        let mut tls = ClientTlsConfig::new().domain_name(host.to_string());
        if let Some(ca) = &config.ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let (Some(cert), Some(key)) = (&config.client_cert_pem, &config.client_key_pem) {
            tls = tls.identity(Identity::from_pem(cert, key));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|_| AppRuntimeError::Provider("gRPC TLS setup failed".to_string()))?;
    }
    Ok(endpoint)
}

fn block_on<F: std::future::Future>(runtime: &tokio::runtime::Runtime, future: F) -> F::Output {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

#[derive(Clone, Copy, Default)]
struct RawCodec;

#[derive(Clone, Copy, Default)]
struct RawEncoder;

#[derive(Clone, Copy, Default)]
struct RawDecoder;

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), Status> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(
        &mut self,
        source: &mut DecodeBuf<'_>,
    ) -> std::result::Result<Option<Self::Item>, Status> {
        Ok(Some(source.copy_to_bytes(source.remaining())))
    }
}

fn encode_message(
    contract: &ApplicationGrpcClientV1,
    message_type: &str,
    value: &Value,
    depth: usize,
) -> Result<Vec<u8>> {
    if depth > MAX_SCHEMA_DEPTH {
        return provider_error("gRPC message nesting exceeds provider bounds");
    }
    let schema = contract.messages.get(message_type).ok_or_else(|| {
        AppRuntimeError::Provider(format!("gRPC message schema `{message_type}` is absent"))
    })?;
    let object = value.as_object().ok_or_else(|| {
        AppRuntimeError::Provider(format!("gRPC message `{message_type}` must be an object"))
    })?;
    let known = schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    if object.keys().any(|name| !known.contains(name.as_str())) {
        return provider_error("gRPC request contains a field outside the signed schema");
    }
    let mut bytes = Vec::new();
    for field in &schema.fields {
        let Some(value) = object.get(&field.name) else {
            if field.optional || field.repeated {
                continue;
            }
            return provider_error(format!("gRPC request is missing field `{}`", field.name));
        };
        if value.is_null() && field.optional {
            continue;
        }
        if field.repeated {
            let values = value.as_array().ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "gRPC repeated field `{}` must be an array",
                    field.name
                ))
            })?;
            for value in values {
                encode_field(contract, field, value, depth, &mut bytes)?;
            }
        } else {
            encode_field(contract, field, value, depth, &mut bytes)?;
        }
    }
    Ok(bytes)
}

fn encode_field(
    contract: &ApplicationGrpcClientV1,
    field: &ApplicationGrpcFieldV1,
    value: &Value,
    depth: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    let wire = field_wire(field)?;
    write_varint((u64::from(field.tag) << 3) | u64::from(wire), output);
    match field.wire_type.as_str() {
        "string" => {
            let value = value
                .as_str()
                .ok_or_else(|| field_type_error(field, "string"))?;
            write_length_delimited(value.as_bytes(), output)?;
        }
        "bool" => write_varint(
            u64::from(
                value
                    .as_bool()
                    .ok_or_else(|| field_type_error(field, "bool"))?,
            ),
            output,
        ),
        "int32" => {
            let value = i32::try_from(
                value
                    .as_i64()
                    .ok_or_else(|| field_type_error(field, "integer"))?,
            )
            .map_err(|_| field_type_error(field, "int32"))?;
            write_varint(value as i64 as u64, output);
        }
        "int64" => write_varint(
            value
                .as_i64()
                .ok_or_else(|| field_type_error(field, "integer"))? as u64,
            output,
        ),
        "uint32" => write_varint(
            u64::from(
                u32::try_from(
                    value
                        .as_u64()
                        .ok_or_else(|| field_type_error(field, "uint32"))?,
                )
                .map_err(|_| field_type_error(field, "uint32"))?,
            ),
            output,
        ),
        "uint64" => write_varint(
            value
                .as_u64()
                .ok_or_else(|| field_type_error(field, "uint64"))?,
            output,
        ),
        "sint32" => {
            let value = i32::try_from(
                value
                    .as_i64()
                    .ok_or_else(|| field_type_error(field, "integer"))?,
            )
            .map_err(|_| field_type_error(field, "sint32"))?;
            write_varint(u64::from(((value << 1) ^ (value >> 31)) as u32), output);
        }
        "sint64" => {
            let value = value
                .as_i64()
                .ok_or_else(|| field_type_error(field, "integer"))?;
            write_varint(((value << 1) ^ (value >> 63)) as u64, output);
        }
        "fixed32" => {
            let value = u32::try_from(
                value
                    .as_u64()
                    .ok_or_else(|| field_type_error(field, "fixed32"))?,
            )
            .map_err(|_| field_type_error(field, "fixed32"))?;
            output.extend_from_slice(&value.to_le_bytes());
        }
        "sfixed32" => {
            let value = i32::try_from(
                value
                    .as_i64()
                    .ok_or_else(|| field_type_error(field, "sfixed32"))?,
            )
            .map_err(|_| field_type_error(field, "sfixed32"))?;
            output.extend_from_slice(&value.to_le_bytes());
        }
        "fixed64" => output.extend_from_slice(
            &value
                .as_u64()
                .ok_or_else(|| field_type_error(field, "fixed64"))?
                .to_le_bytes(),
        ),
        "sfixed64" => output.extend_from_slice(
            &value
                .as_i64()
                .ok_or_else(|| field_type_error(field, "sfixed64"))?
                .to_le_bytes(),
        ),
        "float" => output.extend_from_slice(
            &(value
                .as_f64()
                .ok_or_else(|| field_type_error(field, "number"))? as f32)
                .to_le_bytes(),
        ),
        "double" => output.extend_from_slice(
            &value
                .as_f64()
                .ok_or_else(|| field_type_error(field, "number"))?
                .to_le_bytes(),
        ),
        _ => {
            let message_type = field.message_type.as_deref().ok_or_else(|| {
                AppRuntimeError::Provider("gRPC message field has no schema reference".to_string())
            })?;
            let nested = encode_message(contract, message_type, value, depth + 1)?;
            write_length_delimited(&nested, output)?;
        }
    }
    Ok(())
}

fn decode_message(
    contract: &ApplicationGrpcClientV1,
    message_type: &str,
    bytes: &[u8],
    depth: usize,
) -> Result<Value> {
    if depth > MAX_SCHEMA_DEPTH {
        return provider_error("gRPC message nesting exceeds provider bounds");
    }
    let schema = contract.messages.get(message_type).ok_or_else(|| {
        AppRuntimeError::Provider(format!("gRPC message schema `{message_type}` is absent"))
    })?;
    let fields = schema
        .fields
        .iter()
        .map(|field| (field.tag, field))
        .collect::<BTreeMap<_, _>>();
    let mut result = Map::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let key = read_varint(bytes, &mut offset)?;
        let tag = u32::try_from(key >> 3)
            .map_err(|_| AppRuntimeError::Provider("gRPC field tag overflow".to_string()))?;
        let wire = (key & 7) as u8;
        let Some(field) = fields.get(&tag) else {
            skip_value(bytes, &mut offset, wire)?;
            continue;
        };
        if field.repeated && wire == 2 && field_wire(field)? != 2 {
            let packed = read_length_delimited(bytes, &mut offset)?;
            let mut packed_offset = 0usize;
            while packed_offset < packed.len() {
                let value = decode_field(
                    contract,
                    field,
                    packed,
                    &mut packed_offset,
                    field_wire(field)?,
                    depth,
                )?;
                result
                    .entry(field.name.clone())
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()
                    .expect("repeated gRPC field is an array")
                    .push(value);
            }
            continue;
        }
        let value = decode_field(contract, field, bytes, &mut offset, wire, depth)?;
        if field.repeated {
            result
                .entry(field.name.clone())
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("repeated gRPC field is an array")
                .push(value);
        } else {
            result.insert(field.name.clone(), value);
        }
    }
    for field in &schema.fields {
        if result.contains_key(&field.name) {
            continue;
        }
        let value = if field.repeated {
            Value::Array(Vec::new())
        } else if field.optional || field.message_type.is_some() {
            Value::Null
        } else {
            scalar_default(&field.wire_type)
        };
        result.insert(field.name.clone(), value);
    }
    Ok(Value::Object(result))
}

fn decode_field(
    contract: &ApplicationGrpcClientV1,
    field: &ApplicationGrpcFieldV1,
    bytes: &[u8],
    offset: &mut usize,
    wire: u8,
    depth: usize,
) -> Result<Value> {
    if wire != field_wire(field)? {
        return provider_error(format!(
            "gRPC field `{}` has an invalid wire type",
            field.name
        ));
    }
    Ok(match field.wire_type.as_str() {
        "string" => Value::String(
            std::str::from_utf8(read_length_delimited(bytes, offset)?)
                .map_err(|_| AppRuntimeError::Provider("gRPC string is not UTF-8".to_string()))?
                .to_string(),
        ),
        "bool" => Value::Bool(read_varint(bytes, offset)? != 0),
        "int32" => Value::from(read_varint(bytes, offset)? as u32 as i32 as i64),
        "int64" => Value::from(read_varint(bytes, offset)? as i64),
        "uint32" => Value::from(read_varint(bytes, offset)? as u32 as i64),
        "uint64" => Value::from(i64::try_from(read_varint(bytes, offset)?).map_err(|_| {
            AppRuntimeError::Provider("gRPC uint64 exceeds BicDB application Int".to_string())
        })?),
        "sint32" => {
            let value = read_varint(bytes, offset)? as u32;
            Value::from(((value >> 1) as i32 ^ -((value & 1) as i32)) as i64)
        }
        "sint64" => {
            let value = read_varint(bytes, offset)?;
            Value::from((value >> 1) as i64 ^ -((value & 1) as i64))
        }
        "fixed32" => Value::from(u32::from_le_bytes(read_array(bytes, offset)?) as i64),
        "sfixed32" => Value::from(i32::from_le_bytes(read_array(bytes, offset)?) as i64),
        "fixed64" => Value::from(
            i64::try_from(u64::from_le_bytes(read_array(bytes, offset)?)).map_err(|_| {
                AppRuntimeError::Provider("gRPC fixed64 exceeds BicDB application Int".to_string())
            })?,
        ),
        "sfixed64" => Value::from(i64::from_le_bytes(read_array(bytes, offset)?)),
        "float" => number_value(f32::from_le_bytes(read_array(bytes, offset)?) as f64)?,
        "double" => number_value(f64::from_le_bytes(read_array(bytes, offset)?))?,
        _ => {
            let nested = read_length_delimited(bytes, offset)?;
            decode_message(
                contract,
                field.message_type.as_deref().ok_or_else(|| {
                    AppRuntimeError::Provider(
                        "gRPC message field has no schema reference".to_string(),
                    )
                })?,
                nested,
                depth + 1,
            )?
        }
    })
}

fn field_wire(field: &ApplicationGrpcFieldV1) -> Result<u8> {
    Ok(match field.wire_type.as_str() {
        "string" => 2,
        "bool" | "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64" => 0,
        "fixed32" | "sfixed32" | "float" => 5,
        "fixed64" | "sfixed64" | "double" => 1,
        _ if field.message_type.is_some() => 2,
        _ => return provider_error("unsupported gRPC protobuf field type"),
    })
}

fn scalar_default(wire_type: &str) -> Value {
    match wire_type {
        "string" => Value::String(String::new()),
        "bool" => Value::Bool(false),
        "float" | "double" => Value::from(0.0),
        _ => Value::from(0),
    }
}

fn number_value(value: f64) -> Result<Value> {
    Number::from_f64(value).map(Value::Number).ok_or_else(|| {
        AppRuntimeError::Provider("gRPC response contains a non-finite float".to_string())
    })
}

fn write_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*offset).ok_or_else(|| {
            AppRuntimeError::Provider("truncated gRPC protobuf varint".to_string())
        })?;
        *offset += 1;
        if shift == 63 && byte > 1 {
            return provider_error("gRPC protobuf varint overflow");
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    provider_error("gRPC protobuf varint overflow")
}

fn write_length_delimited(value: &[u8], output: &mut Vec<u8>) -> Result<()> {
    write_varint(
        u64::try_from(value.len())
            .map_err(|_| AppRuntimeError::Provider("gRPC payload length overflow".to_string()))?,
        output,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn read_length_delimited<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let length = usize::try_from(read_varint(bytes, offset)?)
        .map_err(|_| AppRuntimeError::Provider("gRPC payload length overflow".to_string()))?;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| AppRuntimeError::Provider("truncated gRPC protobuf field".to_string()))?;
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| AppRuntimeError::Provider("truncated gRPC protobuf field".to_string()))?;
    let value = bytes[*offset..end]
        .try_into()
        .expect("validated protobuf fixed-width slice");
    *offset = end;
    Ok(value)
}

fn skip_value(bytes: &[u8], offset: &mut usize, wire: u8) -> Result<()> {
    match wire {
        0 => {
            read_varint(bytes, offset)?;
        }
        1 => {
            read_array::<8>(bytes, offset)?;
        }
        2 => {
            read_length_delimited(bytes, offset)?;
        }
        5 => {
            read_array::<4>(bytes, offset)?;
        }
        _ => return provider_error("unsupported gRPC protobuf wire type"),
    }
    Ok(())
}

fn field_type_error(field: &ApplicationGrpcFieldV1, expected: &str) -> AppRuntimeError {
    AppRuntimeError::Provider(format!("gRPC field `{}` requires {expected}", field.name))
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    let mut chars = value.chars();
    if value.len() > 128
        || !chars
            .next()
            .is_some_and(|value| value.is_ascii_alphabetic() || value == '_')
        || !chars.all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-' | '.'))
    {
        return provider_error(format!("invalid {label}"));
    }
    Ok(())
}

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_extension::abi_v2::{
        ApplicationGrpcFieldV1, ApplicationGrpcMessageV1, ApplicationGrpcMethodV1,
    };
    use serde_json::json;

    mod wire {
        tonic::include_proto!("bicdb.example.inventory.v1");
    }

    #[derive(Default)]
    struct Inventory;

    #[tonic::async_trait]
    impl wire::inventory_server::Inventory for Inventory {
        async fn reserve(
            &self,
            request: tonic::Request<wire::ReserveRequest>,
        ) -> std::result::Result<tonic::Response<wire::ReserveResponse>, tonic::Status> {
            if request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer operator-secret")
                || request.metadata().get("x-bicdb-trace-id").is_none()
            {
                return Err(tonic::Status::unauthenticated("missing provider metadata"));
            }
            let request = request.into_inner();
            if let Some(line) = request
                .lines
                .iter()
                .find(|line| line.sku == "sensitive-sku")
            {
                return Err(tonic::Status::invalid_argument(format!(
                    "rejected {}",
                    line.sku
                )));
            }
            let accepted = request
                .lines
                .iter()
                .any(|line| line.sku == "demo" && line.quantity == 3);
            Ok(tonic::Response::new(wire::ReserveResponse {
                accepted,
                reservation_id: request.request_id.unwrap_or_default(),
            }))
        }
    }

    fn contract() -> ApplicationGrpcClientV1 {
        ApplicationGrpcClientV1 {
            provider: "inventory".to_string(),
            service: "Inventory".to_string(),
            methods: BTreeMap::from([(
                "Reserve".to_string(),
                ApplicationGrpcMethodV1 {
                    path: "/demo.Inventory/Reserve".to_string(),
                    request_type: "Request".to_string(),
                    response_type: "Response".to_string(),
                    deadline_ms: 1_000,
                    retries: 0,
                },
            )]),
            messages: BTreeMap::from([
                (
                    "Request".to_string(),
                    ApplicationGrpcMessageV1 {
                        proto_name: "Request".to_string(),
                        fields: vec![
                            ApplicationGrpcFieldV1 {
                                name: "sku".to_string(),
                                proto_name: "sku".to_string(),
                                wire_type: "string".to_string(),
                                message_type: None,
                                tag: 1,
                                repeated: false,
                                optional: false,
                            },
                            ApplicationGrpcFieldV1 {
                                name: "quantity".to_string(),
                                proto_name: "quantity".to_string(),
                                wire_type: "int32".to_string(),
                                message_type: None,
                                tag: 2,
                                repeated: false,
                                optional: false,
                            },
                        ],
                    },
                ),
                (
                    "Response".to_string(),
                    ApplicationGrpcMessageV1 {
                        proto_name: "Response".to_string(),
                        fields: Vec::new(),
                    },
                ),
            ]),
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            emit_evidence: true,
        }
    }

    fn live_contract() -> ApplicationGrpcClientV1 {
        let line = ApplicationGrpcMessageV1 {
            proto_name: "ReservationLine".to_string(),
            fields: vec![
                ApplicationGrpcFieldV1 {
                    name: "sku".to_string(),
                    proto_name: "sku".to_string(),
                    wire_type: "string".to_string(),
                    message_type: None,
                    tag: 1,
                    repeated: false,
                    optional: false,
                },
                ApplicationGrpcFieldV1 {
                    name: "quantity".to_string(),
                    proto_name: "quantity".to_string(),
                    wire_type: "int32".to_string(),
                    message_type: None,
                    tag: 2,
                    repeated: false,
                    optional: false,
                },
            ],
        };
        let request = ApplicationGrpcMessageV1 {
            proto_name: "ReserveRequest".to_string(),
            fields: vec![
                ApplicationGrpcFieldV1 {
                    name: "lines".to_string(),
                    proto_name: "lines".to_string(),
                    wire_type: "ReservationLine".to_string(),
                    message_type: Some("Line".to_string()),
                    tag: 1,
                    repeated: true,
                    optional: false,
                },
                ApplicationGrpcFieldV1 {
                    name: "request_id".to_string(),
                    proto_name: "request_id".to_string(),
                    wire_type: "string".to_string(),
                    message_type: None,
                    tag: 2,
                    repeated: false,
                    optional: true,
                },
            ],
        };
        let response = ApplicationGrpcMessageV1 {
            proto_name: "ReserveResponse".to_string(),
            fields: vec![
                ApplicationGrpcFieldV1 {
                    name: "accepted".to_string(),
                    proto_name: "accepted".to_string(),
                    wire_type: "bool".to_string(),
                    message_type: None,
                    tag: 1,
                    repeated: false,
                    optional: false,
                },
                ApplicationGrpcFieldV1 {
                    name: "reservation_id".to_string(),
                    proto_name: "reservation_id".to_string(),
                    wire_type: "string".to_string(),
                    message_type: None,
                    tag: 2,
                    repeated: false,
                    optional: false,
                },
            ],
        };
        ApplicationGrpcClientV1 {
            provider: "inventory".to_string(),
            service: "Inventory".to_string(),
            methods: BTreeMap::from([(
                "Reserve".to_string(),
                ApplicationGrpcMethodV1 {
                    path: "/bicdb.example.inventory.v1.Inventory/Reserve".to_string(),
                    request_type: "Request".to_string(),
                    response_type: "Response".to_string(),
                    deadline_ms: 2_000,
                    retries: 1,
                },
            )]),
            messages: BTreeMap::from([
                ("Line".to_string(), line),
                ("Request".to_string(), request),
                ("Response".to_string(), response),
            ]),
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            emit_evidence: true,
        }
    }

    #[test]
    fn protobuf_round_trip_uses_signed_tags_and_types() {
        let contract = contract();
        let value = json!({"sku": "demo", "quantity": 3});
        let encoded = encode_message(&contract, "Request", &value, 0).unwrap();
        assert_eq!(encoded, b"\x0a\x04demo\x10\x03");
        assert_eq!(
            decode_message(&contract, "Request", &encoded, 0).unwrap(),
            value
        );
    }

    #[test]
    fn protobuf_encoder_rejects_unsigned_fields() {
        let error = encode_message(
            &contract(),
            "Request",
            &json!({"sku": "demo", "quantity": 3, "admin": true}),
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside the signed schema"));

        let error = encode_message(
            &contract(),
            "Request",
            &json!({"sku": "demo", "quantity": i64::from(i32::MAX) + 1}),
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("int32"));
    }

    #[test]
    fn production_provider_executes_real_http2_protobuf_and_enforces_operator_methods() {
        let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    address_tx.send(listener.local_addr().unwrap()).unwrap();
                    tonic::transport::Server::builder()
                        .add_service(wire::inventory_server::InventoryServer::new(Inventory))
                        .serve_with_incoming_shutdown(
                            tokio_stream::wrappers::TcpListenerStream::new(listener),
                            async {
                                let _ = shutdown_rx.await;
                            },
                        )
                        .await
                        .unwrap();
                });
        });
        let address = address_rx.recv().unwrap();
        let config = GrpcProviderConfig {
            application: "sample-app".to_string(),
            provider: "inventory".to_string(),
            endpoint: format!("http://{address}").parse().unwrap(),
            bearer_token: Some("operator-secret".to_string()),
            client_cert_pem: None,
            client_key_pem: None,
            ca_pem: None,
            allowed_methods: BTreeSet::from([
                "/bicdb.example.inventory.v1.Inventory/Reserve".to_string()
            ]),
            allow_insecure_http: true,
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
        };
        let provider = ProductionGrpcProvider::new(vec![config.clone()]).unwrap();
        let result = provider
            .unary(
                "sample-app",
                "inventory",
                "Reserve",
                &live_contract(),
                json!({
                    "lines": [{"sku": "demo", "quantity": 3}],
                    "request_id": "request-1"
                }),
                2_000,
                "trace-1",
            )
            .unwrap();
        assert_eq!(
            result,
            json!({"accepted": true, "reservation_id": "request-1"})
        );

        let rejected_status = provider
            .unary(
                "sample-app",
                "inventory",
                "Reserve",
                &live_contract(),
                json!({
                    "lines": [{"sku": "sensitive-sku", "quantity": 1}],
                    "request_id": "request-2"
                }),
                2_000,
                "trace-2",
            )
            .unwrap_err();
        assert!(rejected_status.to_string().contains("InvalidArgument"));
        assert!(!rejected_status.to_string().contains("sensitive-sku"));

        let denied = ProductionGrpcProvider::new(vec![GrpcProviderConfig {
            allowed_methods: BTreeSet::from(["/other.Service/Method".to_string()]),
            ..config
        }])
        .unwrap()
        .unary(
            "sample-app",
            "inventory",
            "Reserve",
            &live_contract(),
            json!({"lines": [], "request_id": null}),
            2_000,
            "trace-3",
        )
        .unwrap_err();
        assert!(denied.to_string().contains("outside operator policy"));
        shutdown_tx.send(()).unwrap();
        server.join().unwrap();
    }
}
