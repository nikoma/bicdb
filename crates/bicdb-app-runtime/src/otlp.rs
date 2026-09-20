use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use bicdb_extension::abi_v2::{
    ActorContext, ApplicationObservabilityContractV1, ApplicationTelemetryProtocolV1, LogLevel,
    MetricKind,
};
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer as _,
    TracerProvider as _,
};
use opentelemetry::{Context as OtelContext, KeyValue};
use opentelemetry_otlp::{
    LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig, WithHttpConfig,
    WithTonicConfig,
};
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor as AsyncBatchLogProcessor;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use url::Url;

use crate::providers::{BoundedObservability, HostObservability, ObservabilityEvent};
use crate::{AppRuntimeError, Result};

const MAX_EXPORT_QUEUE: usize = 1_000_000;

/// Operator-owned OTLP exporter settings. Endpoints and authorization values
/// are never copied into a signed application package or exposed to guest
/// memory.
#[derive(Clone, Debug)]
pub struct OtlpObservabilityConfig {
    pub endpoint: String,
    pub protocol: ApplicationTelemetryProtocolV1,
    pub headers: BTreeMap<String, String>,
    pub allow_insecure_http: bool,
    pub timeout: Duration,
    pub queue_capacity: usize,
    pub export_interval: Duration,
}

impl OtlpObservabilityConfig {
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.protocol,
            ApplicationTelemetryProtocolV1::OtlpHttp | ApplicationTelemetryProtocolV1::OtlpGrpc
        ) {
            return Err(AppRuntimeError::Provider(
                "OTLP exporter protocol must be otlp_http or otlp_grpc".to_string(),
            ));
        }
        if self.queue_capacity == 0 || self.queue_capacity > MAX_EXPORT_QUEUE {
            return Err(AppRuntimeError::Provider(format!(
                "OTLP queue capacity must be in 1..={MAX_EXPORT_QUEUE}"
            )));
        }
        if self.timeout.is_zero() || self.timeout > Duration::from_secs(300) {
            return Err(AppRuntimeError::Provider(
                "OTLP timeout must be in 1ms..=300s".to_string(),
            ));
        }
        if self.export_interval.is_zero() || self.export_interval > Duration::from_secs(60) {
            return Err(AppRuntimeError::Provider(
                "OTLP export interval must be in 1ms..=60s".to_string(),
            ));
        }
        let endpoint = Url::parse(&self.endpoint).map_err(|error| {
            AppRuntimeError::Provider(format!("invalid OTLP endpoint: {error}"))
        })?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(AppRuntimeError::Provider(
                "OTLP endpoint must be an HTTP(S) origin/path without credentials, query, or fragment"
                    .to_string(),
            ));
        }
        if endpoint.scheme() == "http" && !self.allow_insecure_http {
            return Err(AppRuntimeError::Provider(
                "cleartext OTLP requires allow_insecure_http=true".to_string(),
            ));
        }
        for (name, value) in &self.headers {
            reqwest::header::HeaderName::from_str(name).map_err(|error| {
                AppRuntimeError::Provider(format!("invalid OTLP header `{name}`: {error}"))
            })?;
            reqwest::header::HeaderValue::from_str(value).map_err(|error| {
                AppRuntimeError::Provider(format!("invalid OTLP header `{name}`: {error}"))
            })?;
            if self.protocol == ApplicationTelemetryProtocolV1::OtlpGrpc {
                MetadataKey::<Ascii>::from_bytes(name.as_bytes()).map_err(|error| {
                    AppRuntimeError::Provider(format!(
                        "invalid OTLP gRPC metadata key `{name}`: {error}"
                    ))
                })?;
                MetadataValue::<Ascii>::try_from(value.as_str()).map_err(|error| {
                    AppRuntimeError::Provider(format!(
                        "invalid OTLP gRPC metadata value for `{name}`: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }
}

/// A non-blocking host sink. Application execution only enqueues an already
/// bounded and redacted event. A dedicated current-thread Tokio runtime owns
/// the OpenTelemetry SDK, network clients, batching, and shutdown flush.
pub struct OtlpObservability {
    config: OtlpObservabilityConfig,
    ring: BoundedObservability,
    sender: Mutex<Option<mpsc::Sender<ObservabilityEvent>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    export_drops: Arc<AtomicU64>,
}

impl OtlpObservability {
    pub fn open(config: OtlpObservabilityConfig) -> Result<Self> {
        config.validate()?;
        let ring = BoundedObservability::new(config.queue_capacity)?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let (ready_sender, ready_receiver) = std_mpsc::sync_channel(1);
        let worker_config = config.clone();
        let export_drops = Arc::new(AtomicU64::new(0));
        let worker_drops = export_drops.clone();
        let worker = std::thread::Builder::new()
            .name("bicdb-otlp-export".to_string())
            .spawn(move || {
                let async_runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("bicdb-otlp-io")
                    .enable_all()
                    .build();
                let Ok(async_runtime) = async_runtime else {
                    let _ = ready_sender.send(Err(AppRuntimeError::Provider(
                        "create OTLP async runtime failed".to_string(),
                    )));
                    return;
                };
                let _ = ready_sender.send(Ok(()));
                async_runtime.block_on(run_exporter(worker_config, receiver, worker_drops));
            })?;
        ready_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| {
                AppRuntimeError::Provider(format!("start OTLP exporter worker failed: {error}"))
            })??;
        Ok(Self {
            config,
            ring,
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            export_drops,
        })
    }
}

impl HostObservability for OtlpObservability {
    fn record(&self, event: ObservabilityEvent) {
        self.ring.record(event.clone());
        let result = self
            .sender
            .lock()
            .expect("OTLP sender poisoned")
            .as_ref()
            .map(|sender| sender.try_send(event));
        if !matches!(result, Some(Ok(()))) {
            self.export_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn available(
        &self,
        _application: &str,
        contract: &ApplicationObservabilityContractV1,
    ) -> Result<()> {
        if (contract.provider == "opentelemetry" && contract.protocol == self.config.protocol)
            || (contract.provider == "bicdb"
                && contract.protocol == ApplicationTelemetryProtocolV1::Host)
        {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "signed observability provider/protocol `{}/{:?}` does not match operator exporter `opentelemetry/{:?}`",
                contract.provider, contract.protocol, self.config.protocol
            )))
        }
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        self.ring.snapshot()
    }

    fn dropped(&self) -> u64 {
        self.ring
            .dropped()
            .saturating_add(self.export_drops.load(Ordering::Relaxed))
    }
}

impl Drop for OtlpObservability {
    fn drop(&mut self) {
        self.sender.lock().expect("OTLP sender poisoned").take();
        if let Some(worker) = self.worker.lock().expect("OTLP worker poisoned").take() {
            let _ = worker.join();
        }
    }
}

async fn run_exporter(
    config: OtlpObservabilityConfig,
    mut receiver: mpsc::Receiver<ObservabilityEvent>,
    export_drops: Arc<AtomicU64>,
) {
    let mut services = BTreeMap::<String, ServiceTelemetry>::new();
    while let Some(event) = receiver.recv().await {
        let service_name = event_service_name(&event);
        if !services.contains_key(&service_name) {
            match ServiceTelemetry::open(&config, &service_name) {
                Ok(service) => {
                    services.insert(service_name.clone(), service);
                }
                Err(_) => {
                    export_drops.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
        }
        if services
            .get_mut(&service_name)
            .expect("service telemetry inserted")
            .record(event)
            .is_err()
        {
            export_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    for service in services.into_values() {
        service.shutdown();
    }
}

struct ServiceTelemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
    tracer: SdkTracer,
    logger: SdkLogger,
    meter: Meter,
    counters: BTreeMap<String, Counter<f64>>,
    gauges: BTreeMap<String, Gauge<f64>>,
    histograms: BTreeMap<String, Histogram<f64>>,
}

impl ServiceTelemetry {
    fn open(config: &OtlpObservabilityConfig, service_name: &str) -> Result<Self> {
        let resource = Resource::builder_empty()
            .with_attributes(vec![KeyValue::new(
                "service.name",
                service_name.to_string(),
            )])
            .build();
        let (span_exporter, metric_exporter, log_exporter) = exporters(config)?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(BatchSpanProcessor::builder(span_exporter, runtime::Tokio).build())
            .with_resource(resource.clone())
            .build();
        let metric_reader = PeriodicReader::builder(metric_exporter, runtime::Tokio)
            .with_interval(config.export_interval)
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(metric_reader)
            .with_resource(resource.clone())
            .build();
        let logger_provider = SdkLoggerProvider::builder()
            .with_log_processor(
                AsyncBatchLogProcessor::builder(log_exporter, runtime::Tokio).build(),
            )
            .with_resource(resource)
            .build();
        let tracer = tracer_provider.tracer("bicdb-application-runtime");
        let meter = meter_provider.meter("bicdb-application-runtime");
        let logger = logger_provider.logger("bicdb-application-runtime");
        Ok(Self {
            tracer_provider,
            meter_provider,
            logger_provider,
            tracer,
            logger,
            meter,
            counters: BTreeMap::new(),
            gauges: BTreeMap::new(),
            histograms: BTreeMap::new(),
        })
    }

    fn record(&mut self, event: ObservabilityEvent) -> Result<()> {
        match event {
            ObservabilityEvent::Log {
                actor,
                level,
                message,
                fields,
            } => self.emit_log("bicdb.application.log", level, message, actor, fields),
            ObservabilityEvent::Trace {
                actor,
                name,
                fields,
            } => {
                let parent = actor_otel_context(&actor, &name);
                let mut span = self.tracer.start_with_context(name, &parent);
                span.set_attributes(actor_attributes(&actor));
                span.set_attributes(map_attributes("bicdb.application.field", &fields));
                span.end();
                Ok(())
            }
            ObservabilityEvent::Metric {
                actor,
                name,
                kind,
                value,
                labels,
            } => {
                if !value.is_finite() {
                    return Err(AppRuntimeError::Provider(
                        "OTLP metric value must be finite".to_string(),
                    ));
                }
                let mut attributes = actor_attributes(&actor);
                attributes.extend(
                    labels
                        .into_iter()
                        .map(|(key, value)| KeyValue::new(key, value)),
                );
                match kind {
                    MetricKind::Counter => self
                        .counters
                        .entry(name.clone())
                        .or_insert_with(|| self.meter.f64_counter(name).build())
                        .add(value, &attributes),
                    MetricKind::Gauge => self
                        .gauges
                        .entry(name.clone())
                        .or_insert_with(|| self.meter.f64_gauge(name).build())
                        .record(value, &attributes),
                    MetricKind::Histogram => self
                        .histograms
                        .entry(name.clone())
                        .or_insert_with(|| self.meter.f64_histogram(name).build())
                        .record(value, &attributes),
                }
                Ok(())
            }
            ObservabilityEvent::Audit {
                actor,
                action,
                subject,
                mut fields,
            } => {
                fields.insert("audit.action".to_string(), Value::String(action.clone()));
                fields.insert("audit.subject".to_string(), Value::String(subject));
                self.emit_log(
                    "bicdb.application.audit",
                    LogLevel::Info,
                    action,
                    actor,
                    fields,
                )
            }
            ObservabilityEvent::Evidence {
                actor,
                control,
                outcome,
                mut fields,
            } => {
                fields.insert(
                    "evidence.control".to_string(),
                    Value::String(control.clone()),
                );
                fields.insert("evidence.outcome".to_string(), Value::String(outcome));
                self.emit_log(
                    "bicdb.application.evidence",
                    LogLevel::Info,
                    control,
                    actor,
                    fields,
                )
            }
        }
    }

    fn emit_log(
        &self,
        event_name: &'static str,
        level: LogLevel,
        message: String,
        actor: ActorContext,
        fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        let mut record = self.logger.create_log_record();
        let severity = log_severity(level);
        record.set_event_name(event_name);
        record.set_target("bicdb-application-runtime");
        record.set_timestamp(std::time::SystemTime::now());
        record.set_observed_timestamp(std::time::SystemTime::now());
        record.set_severity_number(severity);
        record.set_severity_text(severity.name());
        record.set_body(AnyValue::String(message.into()));
        let trace_id = actor_trace_id(&actor.trace_id);
        let span_id = derived_span_id(&actor, event_name);
        let trace_flags = actor_trace_flags(&actor);
        record.set_trace_context(trace_id, span_id, Some(trace_flags));
        for attribute in actor_attributes(&actor) {
            record.add_attribute(attribute.key, attribute.value.to_string());
        }
        for attribute in map_attributes("bicdb.application.field", &fields) {
            record.add_attribute(attribute.key, attribute.value.to_string());
        }
        self.logger.emit(record);
        Ok(())
    }

    fn shutdown(self) {
        let _ = self.logger_provider.shutdown();
        let _ = self.meter_provider.shutdown();
        let _ = self.tracer_provider.shutdown();
    }
}

fn exporters(
    config: &OtlpObservabilityConfig,
) -> Result<(SpanExporter, MetricExporter, LogExporter)> {
    let protocol = match config.protocol {
        ApplicationTelemetryProtocolV1::OtlpHttp => Protocol::HttpBinary,
        ApplicationTelemetryProtocolV1::OtlpGrpc => Protocol::Grpc,
        ApplicationTelemetryProtocolV1::Host => {
            return Err(AppRuntimeError::Provider(
                "host observability is not an OTLP exporter".to_string(),
            ));
        }
    };
    if config.protocol == ApplicationTelemetryProtocolV1::OtlpHttp {
        let headers = config
            .headers
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>();
        let span = SpanExporter::builder()
            .with_http()
            .with_http_client(reqwest::Client::new())
            .with_protocol(protocol)
            .with_endpoint(signal_endpoint(&config.endpoint, "v1/traces"))
            .with_timeout(config.timeout)
            .with_headers(headers.clone())
            .build()
            .map_err(|error| provider_error("build OTLP HTTP span exporter", error))?;
        let metric = MetricExporter::builder()
            .with_http()
            .with_http_client(reqwest::Client::new())
            .with_protocol(protocol)
            .with_endpoint(signal_endpoint(&config.endpoint, "v1/metrics"))
            .with_timeout(config.timeout)
            .with_headers(headers.clone())
            .build()
            .map_err(|error| provider_error("build OTLP HTTP metric exporter", error))?;
        let log = LogExporter::builder()
            .with_http()
            .with_http_client(reqwest::Client::new())
            .with_protocol(protocol)
            .with_endpoint(signal_endpoint(&config.endpoint, "v1/logs"))
            .with_timeout(config.timeout)
            .with_headers(headers)
            .build()
            .map_err(|error| provider_error("build OTLP HTTP log exporter", error))?;
        Ok((span, metric, log))
    } else {
        let metadata = grpc_metadata(&config.headers)?;
        let span = SpanExporter::builder()
            .with_tonic()
            .with_protocol(protocol)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_metadata(metadata.clone())
            .build()
            .map_err(|error| provider_error("build OTLP gRPC span exporter", error))?;
        let metric = MetricExporter::builder()
            .with_tonic()
            .with_protocol(protocol)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_metadata(metadata.clone())
            .build()
            .map_err(|error| provider_error("build OTLP gRPC metric exporter", error))?;
        let log = LogExporter::builder()
            .with_tonic()
            .with_protocol(protocol)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_metadata(metadata)
            .build()
            .map_err(|error| provider_error("build OTLP gRPC log exporter", error))?;
        Ok((span, metric, log))
    }
}

fn provider_error(context: &'static str, error: impl std::fmt::Display) -> AppRuntimeError {
    AppRuntimeError::Provider(format!("{context}: {error}"))
}

fn grpc_metadata(headers: &BTreeMap<String, String>) -> Result<MetadataMap> {
    let mut metadata = MetadataMap::new();
    for (name, value) in headers {
        let key = MetadataKey::<Ascii>::from_bytes(name.as_bytes()).map_err(|error| {
            AppRuntimeError::Provider(format!("invalid OTLP gRPC metadata key `{name}`: {error}"))
        })?;
        let value = MetadataValue::<Ascii>::try_from(value.as_str()).map_err(|error| {
            AppRuntimeError::Provider(format!(
                "invalid OTLP gRPC metadata value for `{name}`: {error}"
            ))
        })?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

fn signal_endpoint(base: &str, suffix: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), suffix)
}

fn event_service_name(event: &ObservabilityEvent) -> String {
    let named = match event {
        ObservabilityEvent::Metric { labels, .. } => labels.get("service.name").map(String::as_str),
        ObservabilityEvent::Log { fields, .. }
        | ObservabilityEvent::Trace { fields, .. }
        | ObservabilityEvent::Audit { fields, .. }
        | ObservabilityEvent::Evidence { fields, .. } => fields
            .get("service.name")
            .and_then(Value::as_str)
            .or_else(|| fields.get("bicdb.application").and_then(Value::as_str)),
    };
    named
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("bicdb")
        .to_string()
}

fn log_severity(level: LogLevel) -> Severity {
    match level {
        LogLevel::Trace => Severity::Trace,
        LogLevel::Debug => Severity::Debug,
        LogLevel::Info => Severity::Info,
        LogLevel::Warn => Severity::Warn,
        LogLevel::Error => Severity::Error,
    }
}

fn actor_attributes(actor: &ActorContext) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("bicdb.trace_id", actor.trace_id.clone()),
        KeyValue::new("bicdb.deadline_unix_ms", actor.deadline_unix_ms),
    ];
    for (name, value) in [
        ("bicdb.user_id", actor.user_id.as_ref()),
        ("bicdb.service_id", actor.service_id.as_ref()),
        ("bicdb.client_id", actor.client_id.as_ref()),
        ("bicdb.tenant_id", actor.tenant_id.as_ref()),
        ("bicdb.workspace_id", actor.workspace_id.as_ref()),
        ("bicdb.organization_id", actor.organization_id.as_ref()),
        ("bicdb.session_id", actor.session_id.as_ref()),
        ("bicdb.correlation_id", actor.correlation_id.as_ref()),
        ("bicdb.causation_id", actor.causation_id.as_ref()),
    ] {
        if let Some(value) = value {
            attributes.push(KeyValue::new(name, value.clone()));
        }
    }
    if let Some(value) = actor.policy_attributes.get("w3c.tracestate") {
        attributes.push(KeyValue::new("w3c.tracestate", value.clone()));
    }
    attributes
}

fn map_attributes(prefix: &str, fields: &BTreeMap<String, Value>) -> Vec<KeyValue> {
    fields
        .iter()
        .map(|(name, value)| {
            let key = format!("{prefix}.{name}");
            match value {
                Value::Bool(value) => KeyValue::new(key, *value),
                Value::Number(value) if value.is_i64() => {
                    KeyValue::new(key, value.as_i64().unwrap_or_default())
                }
                Value::Number(value) if value.is_u64() => KeyValue::new(
                    key,
                    i64::try_from(value.as_u64().unwrap_or_default()).unwrap_or(i64::MAX),
                ),
                Value::Number(value) => KeyValue::new(key, value.as_f64().unwrap_or_default()),
                Value::String(value) => KeyValue::new(key, value.clone()),
                other => KeyValue::new(key, other.to_string()),
            }
        })
        .collect()
}

fn actor_otel_context(actor: &ActorContext, salt: &str) -> OtelContext {
    let parent_span_id = actor
        .policy_attributes
        .get("w3c.parent_span_id")
        .and_then(|value| {
            let mut bytes = [0_u8; 8];
            (value.len() == 16
                && hex::decode_to_slice(value, &mut bytes).is_ok()
                && bytes != [0; 8])
                .then(|| SpanId::from_bytes(bytes))
        })
        .unwrap_or_else(|| derived_span_id(actor, salt));
    let context = SpanContext::new(
        actor_trace_id(&actor.trace_id),
        parent_span_id,
        actor_trace_flags(actor),
        true,
        TraceState::default(),
    );
    OtelContext::new().with_remote_span_context(context)
}

fn actor_trace_flags(actor: &ActorContext) -> TraceFlags {
    let value = actor
        .policy_attributes
        .get("w3c.trace_flags")
        .and_then(|value| u8::from_str_radix(value, 16).ok())
        .unwrap_or(1);
    TraceFlags::new(value)
}

fn actor_trace_id(value: &str) -> TraceId {
    let mut bytes = [0_u8; 16];
    let compact = value
        .chars()
        .filter(|character| *character != '-')
        .collect::<String>();
    if compact.len() == 32 && hex::decode_to_slice(&compact, &mut bytes).is_ok() && bytes != [0; 16]
    {
        return TraceId::from_bytes(bytes);
    }
    let digest = Sha256::digest(value.as_bytes());
    bytes.copy_from_slice(&digest[..16]);
    TraceId::from_bytes(bytes)
}

fn derived_span_id(actor: &ActorContext, salt: &str) -> SpanId {
    let digest = Sha256::digest(
        format!(
            "{}\0{}\0{}\0{}",
            actor.trace_id,
            actor.correlation_id.as_deref().unwrap_or_default(),
            actor.causation_id.as_deref().unwrap_or_default(),
            salt
        )
        .as_bytes(),
    );
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    if bytes == [0; 8] {
        bytes[7] = 1;
    }
    SpanId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bicdb_extension::abi_v2::ApplicationSamplingV1;

    use super::*;

    fn config(protocol: ApplicationTelemetryProtocolV1) -> OtlpObservabilityConfig {
        OtlpObservabilityConfig {
            endpoint: "http://127.0.0.1:4318".to_string(),
            protocol,
            headers: BTreeMap::new(),
            allow_insecure_http: true,
            timeout: Duration::from_secs(1),
            queue_capacity: 16,
            export_interval: Duration::from_millis(25),
        }
    }

    fn contract(protocol: ApplicationTelemetryProtocolV1) -> ApplicationObservabilityContractV1 {
        ApplicationObservabilityContractV1 {
            version: 1,
            provider: "opentelemetry".to_string(),
            protocol,
            service_name: "test-service".to_string(),
            sampling: ApplicationSamplingV1::AlwaysOn,
            helpers: BTreeSet::new(),
            redacted_keys: BTreeSet::new(),
            metric_names: BTreeSet::new(),
            audit_actions: BTreeSet::new(),
            dynamic_metric_names: false,
            dynamic_audit_actions: false,
            max_field_depth: 16,
            max_field_bytes: 65_536,
            durable_audit: true,
            propagate_w3c: true,
        }
    }

    #[test]
    fn operator_exporter_requires_explicit_cleartext_and_exact_protocol() {
        let mut insecure = config(ApplicationTelemetryProtocolV1::OtlpHttp);
        insecure.allow_insecure_http = false;
        assert!(insecure.validate().is_err());

        let exporter =
            OtlpObservability::open(config(ApplicationTelemetryProtocolV1::OtlpHttp)).unwrap();
        assert!(exporter
            .available("test", &contract(ApplicationTelemetryProtocolV1::OtlpHttp))
            .is_ok());
        assert!(exporter
            .available("test", &contract(ApplicationTelemetryProtocolV1::OtlpGrpc))
            .is_err());
    }

    #[test]
    fn grpc_metadata_is_validated_before_readiness() {
        let mut grpc = config(ApplicationTelemetryProtocolV1::OtlpGrpc);
        grpc.headers
            .insert("authorization".to_string(), "Bearer test".to_string());
        grpc.validate().unwrap();
        grpc.headers
            .insert("Bad Header".to_string(), "value".to_string());
        assert!(grpc.validate().is_err());
    }
}
