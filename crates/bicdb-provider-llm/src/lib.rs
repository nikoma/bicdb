//! Operator-bound structured LLM provider for BicDB application applications in BicDB.
//!
//! Package contracts select the logical provider, methods, schemas, and hard
//! bounds. The endpoint, credential, concrete model, and optional operator
//! system prompt remain host state and never enter signed package artifacts.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use bicdb_app_runtime::{
    AppRuntimeError, LlmProvider, LlmProviderRequest, LlmProviderResponse, LlmProviderToolCall,
    Result,
};
use bicdb_extension::abi_v2::{
    ApplicationLlmClientV1, ApplicationLlmToolV1, ApplicationRouteParameterTypeV1,
};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Map, Value};
use url::Url;

const MAX_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_BODY_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct LlmProviderConfig {
    pub application: String,
    pub provider: String,
    pub endpoint: Url,
    pub api_key: String,
    pub wire_format: String,
    pub model: String,
    pub operator_system_prompt: Option<String>,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub request_timeout: Duration,
    pub allow_insecure_http: bool,
    pub input_microusd_per_million_tokens: u64,
    pub output_microusd_per_million_tokens: u64,
}

impl std::fmt::Debug for LlmProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LlmProviderConfig")
            .field("application", &self.application)
            .field("provider", &self.provider)
            .field("endpoint", &self.endpoint)
            .field("api_key_configured", &!self.api_key.is_empty())
            .field("wire_format", &self.wire_format)
            .field("model", &self.model)
            .field(
                "operator_system_prompt_configured",
                &self.operator_system_prompt.is_some(),
            )
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("request_timeout", &self.request_timeout)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .field(
                "input_microusd_per_million_tokens",
                &self.input_microusd_per_million_tokens,
            )
            .field(
                "output_microusd_per_million_tokens",
                &self.output_microusd_per_million_tokens,
            )
            .finish_non_exhaustive()
    }
}

impl LlmProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("LLM application", &self.application)?;
        validate_identifier("LLM provider", &self.provider)?;
        if !matches!(self.endpoint.scheme(), "http" | "https")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
        {
            return provider_error(
                "LLM endpoint must be a credential-free HTTP(S) URL without a query or fragment",
            );
        }
        if self.endpoint.scheme() == "http" && !self.allow_insecure_http {
            return provider_error("cleartext LLM transport requires explicit operator policy");
        }
        if self.api_key.is_empty()
            || self.api_key.len() > 64 * 1024
            || self.api_key.chars().any(char::is_control)
        {
            return provider_error("LLM API credential is invalid");
        }
        if !matches!(self.wire_format.as_str(), "openai" | "anthropic")
            || self.model.is_empty()
            || self.model.len() > 1_024
            || self.model.chars().any(char::is_control)
        {
            return provider_error("LLM wire format or model is invalid");
        }
        if self.operator_system_prompt.as_ref().is_some_and(|prompt| {
            prompt.trim().is_empty()
                || prompt.len() > 1024 * 1024
                || prompt.chars().any(|character| {
                    character.is_control() && !matches!(character, '\n' | '\r' | '\t')
                })
        }) {
            return provider_error("LLM operator system prompt is invalid");
        }
        if self.max_request_bytes == 0
            || self.max_request_bytes > MAX_BODY_BYTES
            || self.max_response_bytes == 0
            || self.max_response_bytes > MAX_BODY_BYTES
            || self.request_timeout.is_zero()
            || self.request_timeout > MAX_TIMEOUT
        {
            return provider_error("LLM size or timeout policy is invalid");
        }
        if self.input_microusd_per_million_tokens > 1_000_000_000_000
            || self.output_microusd_per_million_tokens > 1_000_000_000_000
        {
            return provider_error("LLM operator pricing is outside supported bounds");
        }
        Ok(())
    }
}

struct Binding {
    config: Arc<LlmProviderConfig>,
    client: Client,
}

pub struct ProductionLlmProvider {
    bindings: Arc<BTreeMap<(String, String), Binding>>,
}

impl std::fmt::Debug for ProductionLlmProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionLlmProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionLlmProvider {
    pub fn new(configs: Vec<LlmProviderConfig>) -> Result<Self> {
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.contains_key(&key) {
                return provider_error("duplicate application LLM provider binding");
            }
            let client = Client::builder()
                .connect_timeout(config.request_timeout)
                .https_only(!config.allow_insecure_http)
                .build()
                .map_err(|_| AppRuntimeError::Provider("failed to build LLM client".to_string()))?;
            bindings.insert(
                key,
                Binding {
                    config: Arc::new(config),
                    client,
                },
            );
        }
        Ok(Self {
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        Ok(())
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&Binding> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "LLM provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl LlmProvider for ProductionLlmProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        self.binding(application, provider).map(|_| ())
    }

    fn complete(
        &self,
        application: &str,
        provider: &str,
        contract: &ApplicationLlmClientV1,
        request: LlmProviderRequest,
    ) -> Result<LlmProviderResponse> {
        let binding = self.binding(application, provider)?;
        validate_contract_binding(&binding.config, contract)?;
        let timeout = Duration::from_millis(request.deadline_ms)
            .min(binding.config.request_timeout)
            .max(Duration::from_millis(1));
        let body = match binding.config.wire_format.as_str() {
            "openai" => openai_body(&binding.config, contract, &request)?,
            "anthropic" => anthropic_body(&binding.config, contract, &request)?,
            _ => unreachable!("validated LLM wire format"),
        };
        let encoded = serde_json::to_vec(&body)?;
        if encoded.len() as u64 > binding.config.max_request_bytes {
            return provider_error("LLM request exceeds operator bounds");
        }
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "x-carrier-trace-id",
            HeaderValue::from_str(&request.trace_id)
                .map_err(|_| AppRuntimeError::Provider("LLM trace id is invalid".to_string()))?,
        );
        match binding.config.wire_format.as_str() {
            "openai" => {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {}", binding.config.api_key)).map_err(
                        |_| AppRuntimeError::Provider("LLM credential is invalid".to_string()),
                    )?,
                );
            }
            "anthropic" => {
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(&binding.config.api_key).map_err(|_| {
                        AppRuntimeError::Provider("LLM credential is invalid".to_string())
                    })?,
                );
                headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
            }
            _ => unreachable!(),
        }
        let response = binding
            .client
            .post(binding.config.endpoint.clone())
            .headers(headers)
            .timeout(timeout)
            .body(encoded)
            .send()
            .map_err(|error| {
                if error.is_timeout() {
                    AppRuntimeError::Timeout("LLM provider deadline exceeded".to_string())
                } else {
                    AppRuntimeError::Provider("LLM provider request failed".to_string())
                }
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            return Err(provider_status_error(status));
        }
        if response
            .content_length()
            .is_some_and(|length| length > binding.config.max_response_bytes)
        {
            return provider_error("LLM response exceeds operator bounds");
        }
        let mut bytes = Vec::new();
        response
            .take(binding.config.max_response_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| AppRuntimeError::Provider("failed to read LLM response".to_string()))?;
        if bytes.len() as u64 > binding.config.max_response_bytes {
            return provider_error("LLM response exceeds operator bounds");
        }
        let response: Value = serde_json::from_slice(&bytes)
            .map_err(|_| AppRuntimeError::Provider("LLM response is not valid JSON".to_string()))?;
        parse_response(&binding.config, request.output_schema.is_some(), response)
    }

    fn stream(
        &self,
        application: &str,
        provider: &str,
        contract: &ApplicationLlmClientV1,
        request: LlmProviderRequest,
        emit: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<LlmProviderResponse> {
        let binding = self.binding(application, provider)?;
        validate_contract_binding(&binding.config, contract)?;
        if binding.config.wire_format != "openai" {
            return provider_error("live LLM response streaming requires openai wire format");
        }
        if request.output_schema.is_some() {
            return provider_error("live LLM response streaming does not accept structured output");
        }
        let timeout = Duration::from_millis(request.deadline_ms)
            .min(binding.config.request_timeout)
            .max(Duration::from_millis(1));
        let mut body = openai_body(&binding.config, contract, &request)?;
        let object = body
            .as_object_mut()
            .expect("OpenAI request body is an object");
        object.insert("stream".to_string(), Value::Bool(true));
        object.insert("stream_options".to_string(), json!({"include_usage": true}));
        let encoded = serde_json::to_vec(&body)?;
        if encoded.len() as u64 > binding.config.max_request_bytes {
            return provider_error("LLM request exceeds operator bounds");
        }
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", binding.config.api_key))
                .map_err(|_| AppRuntimeError::Provider("LLM credential is invalid".to_string()))?,
        );
        headers.insert(
            "x-carrier-trace-id",
            HeaderValue::from_str(&request.trace_id)
                .map_err(|_| AppRuntimeError::Provider("LLM trace id is invalid".to_string()))?,
        );
        let response = binding
            .client
            .post(binding.config.endpoint.clone())
            .headers(headers)
            .timeout(timeout)
            .body(encoded)
            .send()
            .map_err(|error| {
                if error.is_timeout() {
                    AppRuntimeError::Timeout("LLM provider deadline exceeded".to_string())
                } else {
                    AppRuntimeError::Provider("LLM provider stream request failed".to_string())
                }
            })?;
        if !response.status().is_success() {
            return Err(provider_status_error(response.status().as_u16()));
        }
        parse_openai_stream(&binding.config, contract.max_output_tokens, response, emit)
    }

    fn estimate_microusd(
        &self,
        application: &str,
        provider: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<u64> {
        let config = &self.binding(application, provider)?.config;
        let input = token_cost_microusd(input_tokens, config.input_microusd_per_million_tokens)?;
        let output = token_cost_microusd(output_tokens, config.output_microusd_per_million_tokens)?;
        input
            .checked_add(output)
            .ok_or_else(|| AppRuntimeError::Provider("LLM cost estimate overflowed".to_string()))
    }
}

#[derive(Default)]
struct StreamingToolCall {
    id: String,
    name: String,
    arguments: String,
}

fn accumulate_streaming_tool_calls(
    value: &Value,
    calls: &mut Vec<StreamingToolCall>,
) -> Result<bool> {
    let mut found = false;
    let Some(choices) = value.get("choices").and_then(Value::as_array) else {
        return Ok(false);
    };
    for choice in choices {
        let Some(deltas) = choice
            .get("delta")
            .and_then(|delta| delta.get("tool_calls"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for (position, delta) in deltas.iter().enumerate() {
            found = true;
            let index = delta
                .get("index")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(position);
            if index > 127 {
                return provider_error("LLM provider returned too many streaming tool calls");
            }
            if calls.len() <= index {
                calls.resize_with(index + 1, StreamingToolCall::default);
            }
            let call = &mut calls[index];
            if let Some(id) = delta.get("id").and_then(Value::as_str) {
                call.id.push_str(id);
            }
            if let Some(function) = delta.get("function").and_then(Value::as_object) {
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    call.name.push_str(name);
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    call.arguments.push_str(arguments);
                }
            }
        }
    }
    Ok(found)
}

fn parse_openai_stream(
    config: &LlmProviderConfig,
    max_output_tokens: u64,
    mut response: reqwest::blocking::Response,
    emit: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<LlmProviderResponse> {
    let mut pending = Vec::new();
    let mut chunk = [0u8; 8 * 1024];
    let mut total = 0u64;
    let mut text = String::new();
    let mut input_tokens = None;
    let mut output_tokens = None;
    let mut total_tokens = None;
    let mut response_model = None;
    let mut saw_done = false;
    let mut tool_turn = false;
    let mut final_turn = false;
    let mut tool_calls = Vec::new();
    let mut buffered_frames = Vec::<Vec<u8>>::new();
    while !saw_done {
        let read = response
            .read(&mut chunk)
            .map_err(|_| AppRuntimeError::Provider("failed to read LLM stream".to_string()))?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > config.max_response_bytes {
            return provider_error("LLM stream exceeds operator bounds");
        }
        pending.extend_from_slice(&chunk[..read]);
        while let Some((event_end, separator_len)) = sse_event_boundary(&pending) {
            let event = pending[..event_end].to_vec();
            pending.drain(..event_end + separator_len);
            let Some(data) = sse_data(&event)? else {
                continue;
            };
            if data == "[DONE]" {
                saw_done = true;
                break;
            }
            let value: Value = serde_json::from_str(&data).map_err(|_| {
                AppRuntimeError::Provider("LLM stream data is not valid JSON".to_string())
            })?;
            let has_tool_delta = accumulate_streaming_tool_calls(&value, &mut tool_calls)?;
            if has_tool_delta {
                if final_turn {
                    return provider_error(
                        "LLM provider mixed streamed content and tool calls in one turn",
                    );
                }
                tool_turn = true;
                buffered_frames.clear();
            }
            let content = value
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str);
            if let Some(delta) = content {
                text.push_str(delta);
            }
            if let Some(model) = value.get("model").and_then(Value::as_str) {
                if model != config.model {
                    return provider_error("LLM provider returned an unexpected model");
                }
                response_model = Some(model.to_string());
            }
            if let Some(usage) = value.get("usage") {
                input_tokens = usage.get("prompt_tokens").and_then(Value::as_i64);
                output_tokens = usage.get("completion_tokens").and_then(Value::as_i64);
                total_tokens = usage.get("total_tokens").and_then(Value::as_i64);
                if [input_tokens, output_tokens, total_tokens]
                    .into_iter()
                    .flatten()
                    .any(|tokens| tokens < 0)
                    || input_tokens
                        .zip(output_tokens)
                        .zip(total_tokens)
                        .is_some_and(|((input, output), total)| {
                            input.checked_add(output).is_none_or(|sum| total < sum)
                        })
                {
                    return provider_error("LLM provider returned invalid token usage");
                }
                if output_tokens.is_some_and(|tokens| {
                    u64::try_from(tokens).map_or(true, |tokens| tokens > max_output_tokens)
                }) {
                    return provider_error("LLM provider exceeded signed output token bounds");
                }
            }
            let mut frame = Vec::new();
            for line in data.lines() {
                frame.extend_from_slice(b"data: ");
                frame.extend_from_slice(line.as_bytes());
                frame.push(b'\n');
            }
            frame.push(b'\n');
            if tool_turn {
                continue;
            }
            if content.is_some_and(|content| !content.is_empty()) && !final_turn {
                final_turn = true;
                for buffered in buffered_frames.drain(..) {
                    emit(&buffered)?;
                }
            }
            if final_turn {
                emit(&frame)?;
            } else {
                buffered_frames.push(frame);
            }
            if text.len() as u64 > config.max_response_bytes {
                return provider_error("LLM stream text exceeds operator bounds");
            }
        }
        if pending.len() as u64 > config.max_response_bytes {
            return provider_error("LLM stream event exceeds operator bounds");
        }
    }
    if !saw_done {
        return provider_error("LLM provider ended without data: [DONE]");
    }
    let tool_calls = if tool_turn {
        tool_calls
            .into_iter()
            .map(|call| {
                if call.id.is_empty() || call.name.is_empty() {
                    return provider_error(
                        "streaming LLM tool call omitted its id or function name",
                    );
                }
                let arguments = serde_json::from_str(if call.arguments.trim().is_empty() {
                    "{}"
                } else {
                    &call.arguments
                })
                .map_err(|_| {
                    AppRuntimeError::Provider(
                        "streaming LLM tool arguments are not valid JSON".to_string(),
                    )
                })?;
                Ok(LlmProviderToolCall {
                    id: call.id,
                    name: call.name,
                    arguments,
                })
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        for buffered in buffered_frames {
            emit(&buffered)?;
        }
        emit(b"data: [DONE]\n\n")?;
        Vec::new()
    };
    Ok(LlmProviderResponse {
        text,
        provider: config.provider.clone(),
        model: response_model.unwrap_or_else(|| config.model.clone()),
        structured_output: None,
        input_tokens,
        output_tokens,
        total_tokens,
        tool_calls,
    })
}

fn sse_event_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    for index in 0..bytes.len() {
        if bytes.get(index..index + 4) == Some(b"\r\n\r\n") {
            return Some((index, 4));
        }
        if bytes.get(index..index + 2) == Some(b"\n\n")
            || bytes.get(index..index + 2) == Some(b"\r\r")
        {
            return Some((index, 2));
        }
    }
    None
}

fn sse_data(event: &[u8]) -> Result<Option<String>> {
    let event = std::str::from_utf8(event)
        .map_err(|_| AppRuntimeError::Provider("LLM stream is not UTF-8".to_string()))?;
    let mut data = Vec::new();
    for line in event.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if line == "data" {
            data.push("");
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    Ok((!data.is_empty()).then(|| data.join("\n")))
}

fn provider_status_error(status: u16) -> AppRuntimeError {
    match status {
        408 | 504 => AppRuntimeError::Timeout("LLM provider deadline exceeded".to_string()),
        429 => AppRuntimeError::RateLimited("LLM provider rejected the request rate".to_string()),
        _ => AppRuntimeError::Provider(format!("LLM provider returned HTTP status {status}")),
    }
}

fn token_cost_microusd(tokens: u64, rate: u64) -> Result<u64> {
    let numerator = u128::from(tokens)
        .checked_mul(u128::from(rate))
        .and_then(|value| value.checked_add(999_999))
        .ok_or_else(|| AppRuntimeError::Provider("LLM cost estimate overflowed".to_string()))?;
    u64::try_from(numerator / 1_000_000)
        .map_err(|_| AppRuntimeError::Provider("LLM cost estimate overflowed".to_string()))
}

fn validate_contract_binding(
    config: &LlmProviderConfig,
    contract: &ApplicationLlmClientV1,
) -> Result<()> {
    if contract.provider != config.provider
        || contract
            .wire_format
            .as_deref()
            .is_some_and(|wire| wire != config.wire_format)
        || contract
            .model
            .as_deref()
            .is_some_and(|model| model != config.model)
        || config.operator_system_prompt.is_some() && !contract.operator_system_prompt
    {
        return provider_error("LLM binding conflicts with signed package authority");
    }
    Ok(())
}

fn openai_body(
    config: &LlmProviderConfig,
    contract: &ApplicationLlmClientV1,
    request: &LlmProviderRequest,
) -> Result<Value> {
    let mut messages = request.messages.clone();
    if let Some(prompt) = &config.operator_system_prompt {
        messages.insert(0, json!({"role": "system", "content": prompt}));
    }
    let mut body = Map::from_iter([
        ("model".to_string(), Value::String(config.model.clone())),
        ("messages".to_string(), Value::Array(messages)),
    ]);
    if is_modern_openai_chat_model(&config.model) {
        body.insert(
            "max_completion_tokens".to_string(),
            Value::from(contract.max_output_tokens),
        );
    } else {
        body.insert(
            "max_tokens".to_string(),
            Value::from(contract.max_output_tokens),
        );
        body.insert(
            "temperature".to_string(),
            Value::from(f64::from(contract.temperature_millis) / 1_000.0),
        );
    }
    if let (Some(name), Some(schema)) = (&request.output_type, &request.output_schema) {
        body.insert(
            "response_format".to_string(),
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": safe_schema_name(name),
                    "strict": true,
                    "schema": json_schema(schema),
                }
            }),
        );
    }
    if !request.tools.is_empty() {
        body.insert(
            "tools".to_string(),
            Value::Array(
                request
                    .tools
                    .iter()
                    .map(|(name, tool)| {
                        json!({
                            "type": "function",
                            "function": {
                                "name": name,
                                "description": tool.description,
                                "parameters": tool_input_schema(tool),
                                "strict": true,
                            }
                        })
                    })
                    .collect(),
            ),
        );
        body.insert("tool_choice".to_string(), Value::String("auto".to_string()));
    }
    Ok(Value::Object(body))
}

fn is_modern_openai_chat_model(model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    model.starts_with("gpt-5")
        || model
            .strip_prefix('o')
            .and_then(|suffix| suffix.chars().next())
            .is_some_and(|digit| matches!(digit, '1'..='9'))
}

fn anthropic_body(
    config: &LlmProviderConfig,
    contract: &ApplicationLlmClientV1,
    request: &LlmProviderRequest,
) -> Result<Value> {
    let mut system = Vec::new();
    if let Some(prompt) = &config.operator_system_prompt {
        system.push(prompt.clone());
    }
    let mut messages = Vec::new();
    for message in &request.messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "system" {
            system.push(
                message
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
        } else if role == "assistant" && message.get("tool_calls").is_some() {
            let mut content = Vec::new();
            if let Some(text) = message.get("content").and_then(Value::as_str) {
                if !text.is_empty() {
                    content.push(json!({"type": "text", "text": text}));
                }
            }
            for call in message
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                content.push(json!({
                    "type": "tool_use",
                    "id": call.get("id").and_then(Value::as_str).unwrap_or(""),
                    "name": call.pointer("/function/name").and_then(Value::as_str).unwrap_or(""),
                    "input": call.pointer("/function/arguments").and_then(Value::as_str)
                        .and_then(|value| serde_json::from_str::<Value>(value).ok())
                        .unwrap_or_else(|| json!({})),
                }));
            }
            messages.push(json!({"role": "assistant", "content": content}));
        } else if role == "tool" {
            messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": message.get("tool_call_id").and_then(Value::as_str).unwrap_or(""),
                    "content": message.get("content").and_then(Value::as_str).unwrap_or("null"),
                }]
            }));
        } else {
            messages.push(json!({
                "role": role,
                "content": message.get("content").and_then(Value::as_str).unwrap_or("")
            }));
        }
    }
    if let Some(schema) = &request.output_schema {
        system.push(format!(
            "Return only one JSON value matching this JSON Schema: {}",
            serde_json::to_string(&json_schema(schema))?
        ));
    }
    let mut body = json!({
        "model": config.model,
        "max_tokens": contract.max_output_tokens,
        "temperature": f64::from(contract.temperature_millis) / 1_000.0,
        "system": system.join("\n\n"),
        "messages": messages,
    });
    if !request.tools.is_empty() {
        body.as_object_mut()
            .expect("Anthropic body is an object")
            .insert(
                "tools".to_string(),
                Value::Array(
                    request
                        .tools
                        .iter()
                        .map(|(name, tool)| {
                            json!({
                                "name": name,
                                "description": tool.description,
                                "input_schema": tool_input_schema(tool),
                            })
                        })
                        .collect(),
                ),
            );
    }
    Ok(body)
}

fn tool_input_schema(tool: &ApplicationLlmToolV1) -> Value {
    let required = tool
        .parameters
        .iter()
        .filter(|parameter| !parameter.optional)
        .map(|parameter| Value::String(parameter.name.clone()))
        .collect::<Vec<_>>();
    let properties = tool
        .parameters
        .iter()
        .map(|parameter| (parameter.name.clone(), json_schema(&parameter.value_type)))
        .collect::<Map<_, _>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn parse_response(
    config: &LlmProviderConfig,
    structured: bool,
    response: Value,
) -> Result<LlmProviderResponse> {
    let (text, tool_calls, input_tokens, output_tokens, total_tokens) = if config.wire_format
        == "anthropic"
    {
        let content = response
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(|| AppRuntimeError::Provider("LLM response omitted content".to_string()))?;
        let text = content
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("");
        let tool_calls = content
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_use"))
            .map(|part| {
                let id = part
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("LLM tool call omitted id".to_string())
                    })?;
                let name = part
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("LLM tool call omitted name".to_string())
                    })?;
                Ok(LlmProviderToolCall {
                    id: id.to_string(),
                    name: name.to_string(),
                    arguments: part.get("input").cloned().unwrap_or_else(|| json!({})),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if text.is_empty() && tool_calls.is_empty() {
            return provider_error("LLM response omitted text and tool calls");
        }
        let input = response
            .pointer("/usage/input_tokens")
            .and_then(Value::as_i64);
        let output = response
            .pointer("/usage/output_tokens")
            .and_then(Value::as_i64);
        let total = input
            .zip(output)
            .and_then(|(left, right)| left.checked_add(right));
        (text, tool_calls, input, output, total)
    } else {
        let message = response.pointer("/choices/0/message").ok_or_else(|| {
            AppRuntimeError::Provider("LLM response omitted assistant message".to_string())
        })?;
        let text = message
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool_calls = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|call| {
                let id = call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("LLM tool call omitted id".to_string())
                    })?;
                let name = call
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("LLM tool call omitted name".to_string())
                    })?;
                let arguments = call
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("LLM tool call omitted arguments".to_string())
                    })?;
                Ok(LlmProviderToolCall {
                    id: id.to_string(),
                    name: name.to_string(),
                    arguments: serde_json::from_str(arguments).map_err(|_| {
                        AppRuntimeError::Provider("LLM tool arguments are invalid JSON".to_string())
                    })?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if text.is_empty() && tool_calls.is_empty() {
            return provider_error("LLM response omitted text and tool calls");
        }
        (
            text,
            tool_calls,
            response
                .pointer("/usage/prompt_tokens")
                .and_then(Value::as_i64),
            response
                .pointer("/usage/completion_tokens")
                .and_then(Value::as_i64),
            response
                .pointer("/usage/total_tokens")
                .and_then(Value::as_i64),
        )
    };
    let structured_output = if structured && tool_calls.is_empty() {
        Some(serde_json::from_str(text.trim()).map_err(|_| {
            AppRuntimeError::Provider("LLM structured response is invalid".to_string())
        })?)
    } else {
        None
    };
    let response_model = response.get("model").and_then(Value::as_str);
    if response_model.is_some_and(|model| model != config.model) {
        return provider_error("LLM provider returned an unexpected model");
    }
    let model = response_model.unwrap_or(&config.model).to_string();
    Ok(LlmProviderResponse {
        text,
        provider: config.provider.clone(),
        model,
        structured_output,
        input_tokens,
        output_tokens,
        total_tokens,
        tool_calls,
    })
}

fn json_schema(value_type: &ApplicationRouteParameterTypeV1) -> Value {
    use ApplicationRouteParameterTypeV1 as Type;
    match value_type {
        Type::String
        | Type::Timestamp
        | Type::Date
        | Type::LocalDateTime
        | Type::TimeZone
        | Type::Uuid => json!({"type": "string"}),
        Type::Int => json!({"type": "integer"}),
        Type::Float | Type::Decimal => json!({"type": "number"}),
        Type::Bool => json!({"type": "boolean"}),
        Type::Json => json!({}),
        Type::Enum { values } => json!({"type": "string", "enum": values}),
        Type::List { element } | Type::Set { element } => {
            json!({"type": "array", "items": json_schema(element)})
        }
        Type::Optional { value } => json!({"anyOf": [json_schema(value), {"type": "null"}]}),
        Type::Object { fields } => {
            let properties = fields
                .iter()
                .map(|field| (field.name.clone(), json_schema(&field.value_type)))
                .collect::<Map<_, _>>();
            let required = fields
                .iter()
                .filter(|field| !field.optional)
                .map(|field| Value::String(field.name.clone()))
                .collect::<Vec<_>>();
            json!({
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false,
            })
        }
        Type::Map { value, .. } => {
            json!({"type": "object", "additionalProperties": json_schema(value)})
        }
        Type::Vector { dimensions } => json!({
            "type": "array",
            "items": {"type": "number"},
            "minItems": dimensions,
            "maxItems": dimensions,
        }),
        Type::Point | Type::LineString | Type::Polygon => json!({"type": "object"}),
        Type::Null => json!({"type": "null"}),
    }
}

fn safe_schema_name(name: &str) -> String {
    let mut value = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(64)
        .collect::<String>();
    if value.is_empty() {
        value.push_str("carrier_output");
    }
    value
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return provider_error(format!("{label} is invalid"));
    }
    Ok(())
}

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use bicdb_extension::abi_v2::ApplicationRouteParameterV1;

    use super::*;

    fn server(response: Value) -> (Url, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let response = serde_json::to_vec(&response).unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|value| value == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .unwrap_or(0);
                if request.len() >= headers_end + 4 + content_length {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                response.len()
            )
            .unwrap();
            stream.write_all(&response).unwrap();
            request
        });
        (
            Url::parse(&format!("http://{address}/v1/chat/completions")).unwrap(),
            handle,
        )
    }

    #[test]
    fn performs_bounded_structured_completion_and_redacts_operator_secret() {
        let (endpoint, server) = server(json!({
            "model": "operator-model",
            "choices": [{"message": {"content": "{\"title\":\"Ready\"}"}}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7}
        }));
        let config = LlmProviderConfig {
            application: "app".to_string(),
            provider: "Writer".to_string(),
            endpoint,
            api_key: "super-secret".to_string(),
            wire_format: "openai".to_string(),
            model: "operator-model".to_string(),
            operator_system_prompt: None,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            request_timeout: Duration::from_secs(2),
            allow_insecure_http: true,
            input_microusd_per_million_tokens: 1_000_000,
            output_microusd_per_million_tokens: 2_000_000,
        };
        assert!(!format!("{config:?}").contains("super-secret"));
        let provider = ProductionLlmProvider::new(vec![config]).unwrap();
        let output_schema = ApplicationRouteParameterTypeV1::Object {
            fields: vec![ApplicationRouteParameterV1 {
                name: "title".to_string(),
                value_type: ApplicationRouteParameterTypeV1::String,
                optional: false,
                default_json: None,
                validations: Vec::new(),
            }],
        };
        let contract = ApplicationLlmClientV1 {
            provider: "Writer".to_string(),
            tokenizer_provider: "default".to_string(),
            methods: BTreeSet::from(["respond_as".to_string()]),
            wire_format: Some("openai".to_string()),
            model: Some("operator-model".to_string()),
            max_prompt_bytes: 64 * 1024,
            max_history_messages: 8,
            max_output_tokens: 64,
            max_turns: 1,
            max_response_bytes: 64 * 1024,
            temperature_millis: 200,
            system_prompt: None,
            operator_system_prompt: false,
            structured_outputs: BTreeMap::from([("Draft".to_string(), output_schema.clone())]),
            tools: BTreeMap::new(),
            budget: None,
            routing: None,
            stream: None,
            emit_evidence: true,
        };
        let response = provider
            .complete(
                "app",
                "Writer",
                &contract,
                LlmProviderRequest {
                    messages: vec![json!({"role": "user", "content": "draft"})],
                    output_type: Some("Draft".to_string()),
                    output_schema: Some(output_schema),
                    tools: BTreeMap::new(),
                    deadline_ms: 2_000,
                    trace_id: "trace-1".to_string(),
                },
            )
            .unwrap();
        assert_eq!(response.structured_output, Some(json!({"title": "Ready"})));
        assert_eq!(response.total_tokens, Some(7));
        let request = String::from_utf8_lossy(&server.join().unwrap()).to_string();
        assert!(request.contains("authorization: Bearer super-secret"));
        assert!(request.contains("\"type\":\"json_schema\""));
    }

    #[test]
    fn validates_secret_free_endpoints_and_modern_openai_options() {
        assert!(is_modern_openai_chat_model("gpt-5.4-mini"));
        assert!(is_modern_openai_chat_model("o3-mini"));
        assert!(!is_modern_openai_chat_model("gpt-4.1"));

        let config = LlmProviderConfig {
            application: "app".to_string(),
            provider: "Writer".to_string(),
            endpoint: Url::parse("https://example.com/v1/chat?api_key=secret").unwrap(),
            api_key: "secret".to_string(),
            wire_format: "openai".to_string(),
            model: "gpt-5-mini".to_string(),
            operator_system_prompt: Some("line one\nline two".to_string()),
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            request_timeout: Duration::from_secs(2),
            allow_insecure_http: false,
            input_microusd_per_million_tokens: 0,
            output_microusd_per_million_tokens: 0,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn translates_and_parses_exact_tool_contracts_for_both_wire_formats() {
        let tool = ApplicationLlmToolV1 {
            description: Some("Search signed documents".to_string()),
            parameters: vec![ApplicationRouteParameterV1 {
                name: "query".to_string(),
                value_type: ApplicationRouteParameterTypeV1::String,
                optional: false,
                default_json: None,
                validations: Vec::new(),
            }],
            output: ApplicationRouteParameterTypeV1::List {
                element: Box::new(ApplicationRouteParameterTypeV1::String),
            },
            callable: "search_docs".to_string(),
            mutating: false,
        };
        let tools = BTreeMap::from([("search_docs".to_string(), tool)]);
        let contract = ApplicationLlmClientV1 {
            provider: "Writer".to_string(),
            tokenizer_provider: "default".to_string(),
            methods: BTreeSet::from(["respond".to_string()]),
            wire_format: Some("openai".to_string()),
            model: Some("operator-model".to_string()),
            max_prompt_bytes: 64 * 1024,
            max_history_messages: 8,
            max_output_tokens: 64,
            max_turns: 4,
            max_response_bytes: 64 * 1024,
            temperature_millis: 0,
            system_prompt: None,
            operator_system_prompt: false,
            structured_outputs: BTreeMap::new(),
            tools: tools.clone(),
            budget: None,
            routing: None,
            stream: None,
            emit_evidence: true,
        };
        let mut config = LlmProviderConfig {
            application: "app".to_string(),
            provider: "Writer".to_string(),
            endpoint: Url::parse("https://example.com/v1/chat").unwrap(),
            api_key: "secret".to_string(),
            wire_format: "openai".to_string(),
            model: "operator-model".to_string(),
            operator_system_prompt: None,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            request_timeout: Duration::from_secs(2),
            allow_insecure_http: false,
            input_microusd_per_million_tokens: 1_000,
            output_microusd_per_million_tokens: 2_000,
        };
        let request = LlmProviderRequest {
            messages: vec![json!({"role": "user", "content": "find it"})],
            output_type: None,
            output_schema: None,
            tools: tools.clone(),
            deadline_ms: 1_000,
            trace_id: "trace-tools".to_string(),
        };
        let openai = openai_body(&config, &contract, &request).unwrap();
        assert_eq!(openai["tools"][0]["function"]["strict"], true);
        assert_eq!(
            openai["tools"][0]["function"]["parameters"]["additionalProperties"],
            false
        );
        let parsed = parse_response(
            &config,
            false,
            json!({
                "model": "operator-model",
                "choices": [{"message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "search_docs", "arguments": "{\"query\":\"x\"}"}
                    }]
                }}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}
            }),
        )
        .unwrap();
        assert_eq!(parsed.tool_calls[0].name, "search_docs");
        assert_eq!(parsed.tool_calls[0].arguments, json!({"query": "x"}));

        config.wire_format = "anthropic".to_string();
        let mut anthropic_contract = contract;
        anthropic_contract.wire_format = Some("anthropic".to_string());
        let anthropic = anthropic_body(&config, &anthropic_contract, &request).unwrap();
        assert_eq!(anthropic["tools"][0]["name"], "search_docs");
        let parsed = parse_response(
            &config,
            false,
            json!({
                "model": "operator-model",
                "content": [{"type": "tool_use", "id": "call-2", "name": "search_docs", "input": {"query": "y"}}],
                "usage": {"input_tokens": 3, "output_tokens": 2}
            }),
        )
        .unwrap();
        assert_eq!(parsed.tool_calls[0].id, "call-2");
        assert_eq!(parsed.tool_calls[0].arguments, json!({"query": "y"}));
    }

    #[test]
    fn classifies_fallback_failures_and_rounds_operator_costs_up() {
        assert!(matches!(
            provider_status_error(429),
            AppRuntimeError::RateLimited(_)
        ));
        assert!(matches!(
            provider_status_error(504),
            AppRuntimeError::Timeout(_)
        ));
        assert!(matches!(
            provider_status_error(500),
            AppRuntimeError::Provider(_)
        ));
        assert_eq!(token_cost_microusd(1, 1).unwrap(), 1);
        assert_eq!(token_cost_microusd(1_000_000, 7).unwrap(), 7);
    }

    #[test]
    fn forwards_fragmented_openai_sse_without_buffering_the_completion() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|value| value == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .unwrap_or(0);
                if request.len() >= headers_end + 4 + content_length {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n")
                .unwrap();
            stream.write_all(b"data: {\"model\":\"operator-model\",\"choices\":[{\"delta\":{\"content\":\"hel")
                .unwrap();
            stream.write_all(b"lo\"}}]}\r\n\r\n").unwrap();
            stream.write_all(b"data: {\"model\":\"operator-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\ndata: [DONE]\n\n")
                .unwrap();
            request
        });
        let config = LlmProviderConfig {
            application: "app".to_string(),
            provider: "Writer".to_string(),
            endpoint: Url::parse(&format!("http://{address}/v1/chat/completions")).unwrap(),
            api_key: "secret".to_string(),
            wire_format: "openai".to_string(),
            model: "operator-model".to_string(),
            operator_system_prompt: None,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            request_timeout: Duration::from_secs(2),
            allow_insecure_http: true,
            input_microusd_per_million_tokens: 0,
            output_microusd_per_million_tokens: 0,
        };
        let provider = ProductionLlmProvider::new(vec![config]).unwrap();
        let contract = ApplicationLlmClientV1 {
            provider: "Writer".to_string(),
            tokenizer_provider: "default".to_string(),
            methods: BTreeSet::from(["stream_response".to_string()]),
            wire_format: Some("openai".to_string()),
            model: Some("operator-model".to_string()),
            max_prompt_bytes: 64 * 1024,
            max_history_messages: 8,
            max_output_tokens: 64,
            max_turns: 1,
            max_response_bytes: 64 * 1024,
            temperature_millis: 0,
            system_prompt: None,
            operator_system_prompt: false,
            structured_outputs: BTreeMap::new(),
            tools: BTreeMap::new(),
            budget: None,
            routing: None,
            stream: None,
            emit_evidence: true,
        };
        let mut frames = Vec::new();
        let response = provider
            .stream(
                "app",
                "Writer",
                &contract,
                LlmProviderRequest {
                    messages: vec![json!({"role": "user", "content": "hello"})],
                    output_type: None,
                    output_schema: None,
                    tools: BTreeMap::new(),
                    deadline_ms: 2_000,
                    trace_id: "trace-stream".to_string(),
                },
                &mut |frame| {
                    frames.push(frame.to_vec());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(response.text, "hello");
        assert_eq!(response.total_tokens, Some(3));
        assert_eq!(frames.len(), 3);
        assert_eq!(frames.last().unwrap(), b"data: [DONE]\n\n");
        let request = String::from_utf8_lossy(&server.join().unwrap()).to_string();
        assert!(request.contains("\"stream\":true"));
        assert!(request.contains("\"include_usage\":true"));
    }

    #[test]
    fn buffers_streaming_tool_turns_without_exposing_internal_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|value| value == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .unwrap_or(0);
                if request.len() >= headers_end + 4 + content_length {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n")
                .unwrap();
            stream.write_all(b"data: {\"model\":\"operator-model\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"search_docs\",\"arguments\":\"{\\\"query\\\":\\\"hel\"}}]}}]}\n\n")
                .unwrap();
            stream.write_all(b"data: {\"model\":\"operator-model\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"lo\\\"}\"}}]}}]}\n\ndata: [DONE]\n\n")
                .unwrap();
            request
        });
        let config = LlmProviderConfig {
            application: "app".to_string(),
            provider: "Writer".to_string(),
            endpoint: Url::parse(&format!("http://{address}/v1/chat/completions")).unwrap(),
            api_key: "secret".to_string(),
            wire_format: "openai".to_string(),
            model: "operator-model".to_string(),
            operator_system_prompt: None,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            request_timeout: Duration::from_secs(2),
            allow_insecure_http: true,
            input_microusd_per_million_tokens: 0,
            output_microusd_per_million_tokens: 0,
        };
        let provider = ProductionLlmProvider::new(vec![config]).unwrap();
        let tools = BTreeMap::from([(
            "search_docs".to_string(),
            ApplicationLlmToolV1 {
                description: Some("Search documents".to_string()),
                parameters: vec![ApplicationRouteParameterV1 {
                    name: "query".to_string(),
                    value_type: ApplicationRouteParameterTypeV1::String,
                    optional: false,
                    default_json: None,
                    validations: Vec::new(),
                }],
                output: ApplicationRouteParameterTypeV1::String,
                callable: "search_docs".to_string(),
                mutating: false,
            },
        )]);
        let contract = ApplicationLlmClientV1 {
            provider: "Writer".to_string(),
            tokenizer_provider: "default".to_string(),
            methods: BTreeSet::from(["stream_response".to_string()]),
            wire_format: Some("openai".to_string()),
            model: Some("operator-model".to_string()),
            max_prompt_bytes: 64 * 1024,
            max_history_messages: 8,
            max_output_tokens: 64,
            max_turns: 4,
            max_response_bytes: 64 * 1024,
            temperature_millis: 0,
            system_prompt: None,
            operator_system_prompt: false,
            structured_outputs: BTreeMap::new(),
            tools: tools.clone(),
            budget: None,
            routing: None,
            stream: None,
            emit_evidence: true,
        };
        let mut frames = Vec::new();
        let response = provider
            .stream(
                "app",
                "Writer",
                &contract,
                LlmProviderRequest {
                    messages: vec![json!({"role": "user", "content": "hello"})],
                    output_type: None,
                    output_schema: None,
                    tools,
                    deadline_ms: 2_000,
                    trace_id: "trace-stream-tools".to_string(),
                },
                &mut |frame| {
                    frames.push(frame.to_vec());
                    Ok(())
                },
            )
            .unwrap();
        assert!(
            frames.is_empty(),
            "internal tool frames must remain private"
        );
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].id, "call-1");
        assert_eq!(response.tool_calls[0].name, "search_docs");
        assert_eq!(response.tool_calls[0].arguments, json!({"query": "hello"}));
        let request = String::from_utf8_lossy(&server.join().unwrap()).to_string();
        assert!(request.contains("\"tools\""));
        assert!(request.contains("\"tool_choice\":\"auto\""));
    }
}
