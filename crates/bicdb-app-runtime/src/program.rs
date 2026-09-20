//! Reusable interpreter for BicDB application's signed, typed behavior program.
//!
//! The compiler has already type-checked this program. The interpreter still
//! validates every dynamic value, bounds execution by a signed step budget,
//! and delegates effects through an explicit host instead of exposing ambient
//! database, network, filesystem, or process authority.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration as StdDuration, Instant};

use base64::Engine;
use bicdb_extension::abi_v2::{
    ApplicationArgumentV1, ApplicationCallKindV1, ApplicationExpressionTypeV1,
    ApplicationExpressionV1, ApplicationModelProjectionV1, ApplicationProgramV1,
    ApplicationProjectionFieldV1, ApplicationStatementV1,
};
use chrono::{
    DateTime, Datelike, Days, Duration as ChronoDuration, LocalResult, NaiveDate, NaiveDateTime,
    Offset, SecondsFormat, TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;
use handlebars::Handlebars;
use hmac::{Hmac, Mac};
use phonenumber::{country, Mode};
use regex::Regex;
use rust_decimal::Decimal;
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{AppRuntimeError, Result};

/// Capability boundary used by effectful BicDB application expressions.
pub trait ApplicationProgramHost {
    fn call(
        &mut self,
        kind: ApplicationCallKindV1,
        target: &str,
        method: Option<&str>,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value>;

    fn begin_transaction(&mut self, isolation: &str) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(format!(
            "BicDB application transaction `{isolation}` has no declared host binding"
        )))
    }

    fn commit_transaction(&mut self) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(
            "BicDB application transaction has no declared host binding".to_string(),
        ))
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(
            "BicDB application transaction has no declared host binding".to_string(),
        ))
    }

    fn emit(&mut self, event: &str, _payload: Value) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(format!(
            "BicDB application event `{event}` has no declared host binding"
        )))
    }

    fn enter_timeout(&mut self, _timeout_ms: u64) -> Result<()> {
        Ok(())
    }

    fn exit_timeout(&mut self) -> Result<()> {
        Ok(())
    }

    fn sleep(&mut self, duration: StdDuration) -> Result<()> {
        std::thread::sleep(duration);
        Ok(())
    }

    /// Called at every interpreter step so pure BicDB application code cannot outlive
    /// the trusted invocation deadline merely by avoiding host effects.
    fn check_deadline(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DenyApplicationProgramHost;

impl ApplicationProgramHost for DenyApplicationProgramHost {
    fn call(
        &mut self,
        kind: ApplicationCallKindV1,
        target: &str,
        method: Option<&str>,
        _arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        Err(AppRuntimeError::CapabilityDenied(format!(
            "BicDB application program call {kind:?} `{target}{}` has no declared host binding",
            method.map(|value| format!(".{value}")).unwrap_or_default()
        )))
    }
}

pub fn execute_application_program(
    program: &ApplicationProgramV1,
    callable: &str,
    globals: BTreeMap<String, Value>,
    arguments: Vec<(Option<String>, Value)>,
    host: &mut dyn ApplicationProgramHost,
) -> Result<Value> {
    let mut evaluator = Evaluator {
        program,
        host,
        globals,
        steps: 0,
        depth: 0,
        max_call_depth: usize::from(program.max_call_depth),
        deadlines: Vec::new(),
    };
    evaluator.call(callable, arguments)
}

/// Evaluate a compiler-signed schema expression without granting it ambient
/// host capabilities. Generated fields and checks share BicDB application's ordinary
/// expression semantics, but any effectful call fails closed at this boundary.
pub(crate) fn evaluate_carrier_expression(
    program: Option<&ApplicationProgramV1>,
    expression: &ApplicationExpressionV1,
    mut globals: BTreeMap<String, Value>,
) -> Result<Value> {
    let fallback = ApplicationProgramV1 {
        version: 1,
        max_steps: 100_000,
        max_call_depth: 16,
        blob: None,
        redis: None,
        email: None,
        grpc: None,
        tokenizer: None,
        embeddings: None,
        llm: None,
        rag: None,
        agents: None,
        evaluations: None,
        tests: None,
        observability: None,
        callables: BTreeMap::new(),
        service_bindings: BTreeMap::new(),
        client_bindings: BTreeMap::new(),
        flags: BTreeMap::new(),
        job_bindings: BTreeMap::new(),
        secret_bindings: BTreeMap::new(),
        security: None,
        event_bindings: BTreeMap::new(),
        realtime_bindings: BTreeMap::new(),
        mutation_bindings: Vec::new(),
        workflow_bindings: BTreeMap::new(),
    };
    let program = program.unwrap_or(&fallback);
    let mut host = DenyApplicationProgramHost;
    let mut evaluator = Evaluator {
        program,
        host: &mut host,
        globals: globals.clone(),
        steps: 0,
        depth: 0,
        max_call_depth: usize::from(program.max_call_depth),
        deadlines: Vec::new(),
    };
    evaluator.expression(expression, &mut globals)
}

struct Evaluator<'a> {
    program: &'a ApplicationProgramV1,
    host: &'a mut dyn ApplicationProgramHost,
    globals: BTreeMap<String, Value>,
    steps: u64,
    depth: usize,
    max_call_depth: usize,
    deadlines: Vec<Instant>,
}

#[derive(Debug)]
enum Flow {
    Next,
    Return(Value),
    Break,
    Continue,
}

#[derive(Clone, Copy, Debug)]
enum CircuitStatus {
    Closed,
    Open { opened_at: Instant },
    HalfOpen,
}

#[derive(Clone, Copy, Debug)]
struct CircuitState {
    consecutive_failures: i64,
    status: CircuitStatus,
}

static CIRCUITS: OnceLock<Mutex<BTreeMap<String, CircuitState>>> = OnceLock::new();

fn circuits() -> &'static Mutex<BTreeMap<String, CircuitState>> {
    CIRCUITS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn circuit_enter(name: &str, reset_timeout_ms: u64) -> Result<()> {
    let mut circuits = circuits().lock().map_err(|_| {
        AppRuntimeError::Provider(
            "BicDB application circuit breaker state is unavailable".to_string(),
        )
    })?;
    let state = circuits.entry(name.to_string()).or_insert(CircuitState {
        consecutive_failures: 0,
        status: CircuitStatus::Closed,
    });
    match state.status {
        CircuitStatus::Closed => Ok(()),
        CircuitStatus::HalfOpen => Err(AppRuntimeError::CircuitOpen(format!(
            "circuit breaker `{name}` is half-open"
        ))),
        CircuitStatus::Open { opened_at } => {
            if opened_at.elapsed() >= StdDuration::from_millis(reset_timeout_ms) {
                state.status = CircuitStatus::HalfOpen;
                Ok(())
            } else {
                Err(AppRuntimeError::CircuitOpen(format!(
                    "circuit breaker `{name}` is open"
                )))
            }
        }
    }
}

fn circuit_success(name: &str) -> Result<()> {
    let mut circuits = circuits().lock().map_err(|_| {
        AppRuntimeError::Provider(
            "BicDB application circuit breaker state is unavailable".to_string(),
        )
    })?;
    let state = circuits.entry(name.to_string()).or_insert(CircuitState {
        consecutive_failures: 0,
        status: CircuitStatus::Closed,
    });
    state.consecutive_failures = 0;
    state.status = CircuitStatus::Closed;
    Ok(())
}

fn circuit_failure(name: &str, failure_threshold: i64) -> Result<()> {
    let mut circuits = circuits().lock().map_err(|_| {
        AppRuntimeError::Provider(
            "BicDB application circuit breaker state is unavailable".to_string(),
        )
    })?;
    let state = circuits.entry(name.to_string()).or_insert(CircuitState {
        consecutive_failures: 0,
        status: CircuitStatus::Closed,
    });
    match state.status {
        CircuitStatus::HalfOpen => {
            state.consecutive_failures = failure_threshold.max(1);
            state.status = CircuitStatus::Open {
                opened_at: Instant::now(),
            };
        }
        CircuitStatus::Closed | CircuitStatus::Open { .. } => {
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            if state.consecutive_failures >= failure_threshold.max(1) {
                state.status = CircuitStatus::Open {
                    opened_at: Instant::now(),
                };
            }
        }
    }
    Ok(())
}

fn positive_integer(value: Value, context: &str) -> Result<i64> {
    let value = integer(value, context)?;
    if value <= 0 {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "{context} must be a positive Int"
        )));
    }
    Ok(value)
}

fn non_negative_integer(value: Value, context: &str) -> Result<i64> {
    let value = integer(value, context)?;
    if value < 0 {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "{context} must be zero or greater"
        )));
    }
    Ok(value)
}

fn retry_delay(backoff_ms: i64, max_backoff_ms: i64, attempt_index: i64) -> u64 {
    let base = backoff_ms.max(0) as u64;
    let maximum = max_backoff_ms.max(backoff_ms).max(0) as u64;
    let shift = attempt_index.clamp(0, 62) as u32;
    base.saturating_mul(1_u64 << shift).min(maximum)
}

impl Evaluator<'_> {
    fn call(&mut self, name: &str, arguments: Vec<(Option<String>, Value)>) -> Result<Value> {
        if self.depth >= self.max_call_depth {
            return Err(AppRuntimeError::ResourceExhausted(
                "BicDB application program call depth exceeded".to_string(),
            ));
        }
        let callable = self.program.callables.get(name).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application program references absent callable `{name}`"
            ))
        })?;
        let mut environment = self.globals.clone();
        let mut positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value.clone());
        for parameter in &callable.parameters {
            let value = arguments
                .iter()
                .find(|(name, _)| name.as_deref() == Some(parameter))
                .map(|(_, value)| value.clone())
                .or_else(|| positional.next())
                .ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(format!(
                        "BicDB application callable `{name}` is missing argument `{parameter}`"
                    ))
                })?;
            environment.insert(parameter.clone(), value);
        }
        self.depth += 1;
        let flow = self.block(&callable.body, &mut environment);
        self.depth -= 1;
        let result = match flow? {
            Flow::Return(value) => Ok(value),
            Flow::Next => Ok(Value::Null),
            Flow::Break | Flow::Continue => Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application callable `{name}` has loop control outside a loop"
            ))),
        };
        result
    }

    fn step(&mut self) -> Result<()> {
        self.host.check_deadline()?;
        if self
            .deadlines
            .last()
            .is_some_and(|deadline| Instant::now() >= *deadline)
        {
            return Err(AppRuntimeError::ResilienceTimeout(
                "BicDB application resilience block deadline expired".to_string(),
            ));
        }
        self.steps = self.steps.saturating_add(1);
        if self.steps > self.program.max_steps {
            return Err(AppRuntimeError::ResourceExhausted(
                "BicDB application program step budget exceeded".to_string(),
            ));
        }
        Ok(())
    }

    fn block(
        &mut self,
        statements: &[ApplicationStatementV1],
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Flow> {
        for statement in statements {
            self.step()?;
            let flow = self.statement(statement, environment)?;
            if !matches!(flow, Flow::Next) {
                return Ok(flow);
            }
        }
        Ok(Flow::Next)
    }

    fn statement(
        &mut self,
        statement: &ApplicationStatementV1,
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Flow> {
        match statement {
            ApplicationStatementV1::Let { name, value } => {
                let value = self.expression(value, environment)?;
                environment.insert(name.clone(), value);
                Ok(Flow::Next)
            }
            ApplicationStatementV1::Assign { name, value } => {
                if !environment.contains_key(name) {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application assignment references absent variable `{name}`"
                    )));
                }
                let value = self.expression(value, environment)?;
                environment.insert(name.clone(), value);
                Ok(Flow::Next)
            }
            ApplicationStatementV1::Return { value } => {
                Ok(Flow::Return(self.expression(value, environment)?))
            }
            ApplicationStatementV1::If {
                condition,
                then_branch,
                else_branch,
            } => {
                if boolean(self.expression(condition, environment)?, "if condition")? {
                    self.block(then_branch, environment)
                } else {
                    self.block(else_branch, environment)
                }
            }
            ApplicationStatementV1::For {
                name,
                iterable,
                body,
            } => {
                let values = self.expression(iterable, environment)?;
                // A paged model call that reaches a loop unprojected iterates
                // its rows, matching every other BicDB application backend.
                let values = if values.is_object()
                    && values.get("items").is_some_and(Value::is_array)
                    && values.get("page_info").is_some_and(Value::is_object)
                {
                    values
                        .get("items")
                        .and_then(Value::as_array)
                        .cloned()
                        .expect("items checked above")
                } else {
                    values.as_array().cloned().ok_or_else(|| {
                        let iterable = serde_json::to_string(iterable)
                            .unwrap_or_else(|_| "<unserializable signed expression>".to_string());
                        AppRuntimeError::InvalidRequest(format!(
                            "BicDB application for-loop iterable must be an array; got {} while evaluating {iterable}",
                            match &values {
                                Value::Object(map) => format!(
                                    "an object with keys [{}]",
                                    map.keys().cloned().collect::<Vec<_>>().join(", ")
                                ),
                                other => other.to_string(),
                            }
                        ))
                    })?
                };
                for value in values {
                    self.step()?;
                    environment.insert(name.clone(), value);
                    match self.block(body, environment)? {
                        Flow::Next | Flow::Continue => {}
                        Flow::Break => break,
                        result @ Flow::Return(_) => return Ok(result),
                    }
                }
                Ok(Flow::Next)
            }
            ApplicationStatementV1::While { condition, body } => {
                loop {
                    self.step()?;
                    let condition = self.expression(condition, environment)?;
                    if !boolean(condition, "while condition")? {
                        break;
                    }
                    match self.block(body, environment)? {
                        Flow::Next | Flow::Continue => {}
                        Flow::Break => break,
                        result @ Flow::Return(_) => return Ok(result),
                    }
                }
                Ok(Flow::Next)
            }
            ApplicationStatementV1::Break => Ok(Flow::Break),
            ApplicationStatementV1::Continue => Ok(Flow::Continue),
            ApplicationStatementV1::Fail { code, message } => {
                let code = string(self.expression(code, environment)?, "failure code")?;
                let message = string(self.expression(message, environment)?, "failure message")?;
                Err(AppRuntimeError::ApplicationFailure {
                    code,
                    message,
                    retryable: false,
                })
            }
            ApplicationStatementV1::Expr { value } => {
                self.expression(value, environment)?;
                Ok(Flow::Next)
            }
            ApplicationStatementV1::Transaction { isolation, body } => {
                self.host.begin_transaction(isolation)?;
                match self.block(body, environment) {
                    Ok(flow) => {
                        if let Err(error) = self.host.commit_transaction() {
                            let _ = self.host.rollback_transaction();
                            Err(error)
                        } else {
                            Ok(flow)
                        }
                    }
                    Err(error) => {
                        let rollback = self.host.rollback_transaction();
                        match rollback {
                            Ok(()) => Err(error),
                            Err(rollback_error) => Err(AppRuntimeError::Invocation(format!(
                                "BicDB application transaction failed: {error}; rollback failed: {rollback_error}"
                            ))),
                        }
                    }
                }
            }
            ApplicationStatementV1::Emit { event, value } => {
                let payload = self.expression(value, environment)?;
                self.host.emit(event, payload)?;
                Ok(Flow::Next)
            }
            ApplicationStatementV1::WithTimeout { timeout_ms, body } => {
                let timeout_ms = positive_integer(
                    self.expression(timeout_ms, environment)?,
                    "with_timeout timeout_ms",
                )? as u64;
                if !self.deadlines.is_empty() {
                    return Err(AppRuntimeError::InvalidPackage(
                        "nested with_timeout blocks are not supported".to_string(),
                    ));
                }
                let deadline = Instant::now()
                    .checked_add(StdDuration::from_millis(timeout_ms))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "with_timeout timeout_ms exceeds the supported range".to_string(),
                        )
                    })?;
                self.host.enter_timeout(timeout_ms)?;
                self.deadlines.push(deadline);
                let result = self.block(body, environment);
                let expired = Instant::now() >= deadline;
                self.deadlines.pop();
                let exit = self.host.exit_timeout();
                if let Err(error) = exit {
                    return Err(error);
                }
                if expired {
                    Err(AppRuntimeError::ResilienceTimeout(format!(
                        "operation timed out after {timeout_ms}ms"
                    )))
                } else {
                    result
                }
            }
            ApplicationStatementV1::WithRetry {
                attempts,
                backoff_ms,
                max_backoff_ms,
                body,
            } => {
                let attempts = positive_integer(
                    self.expression(attempts, environment)?,
                    "with_retry attempts",
                )?;
                let backoff_ms = non_negative_integer(
                    self.expression(backoff_ms, environment)?,
                    "with_retry backoff_ms",
                )?;
                let max_backoff_ms = match max_backoff_ms {
                    Some(value) => non_negative_integer(
                        self.expression(value, environment)?,
                        "with_retry max_backoff_ms",
                    )?,
                    None => backoff_ms,
                };
                if max_backoff_ms < backoff_ms {
                    return Err(AppRuntimeError::InvalidRequest(
                        "with_retry max_backoff_ms must be greater than or equal to backoff_ms"
                            .to_string(),
                    ));
                }
                let mut attempt = 0_i64;
                loop {
                    match self.block(body, environment) {
                        Ok(flow) => break Ok(flow),
                        Err(error) => {
                            attempt += 1;
                            if attempt >= attempts {
                                break Err(error);
                            }
                            let delay = retry_delay(backoff_ms, max_backoff_ms, attempt - 1);
                            self.host.sleep(StdDuration::from_millis(delay))?;
                            self.step()?;
                        }
                    }
                }
            }
            ApplicationStatementV1::CircuitBreaker {
                name,
                failure_threshold,
                reset_timeout_ms,
                body,
            } => {
                let name = string(self.expression(name, environment)?, "circuit_breaker name")?;
                let failure_threshold = positive_integer(
                    self.expression(failure_threshold, environment)?,
                    "circuit_breaker failure_threshold",
                )?;
                let reset_timeout_ms = positive_integer(
                    self.expression(reset_timeout_ms, environment)?,
                    "circuit_breaker reset_timeout_ms",
                )? as u64;
                circuit_enter(&name, reset_timeout_ms)?;
                match self.block(body, environment) {
                    Ok(flow) => {
                        circuit_success(&name)?;
                        Ok(flow)
                    }
                    Err(error) => {
                        circuit_failure(&name, failure_threshold)?;
                        Err(error)
                    }
                }
            }
        }
    }

    fn expression(
        &mut self,
        expression: &ApplicationExpressionV1,
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Value> {
        self.step()?;
        match expression {
            ApplicationExpressionV1::Variable { name } => {
                environment.get(name).cloned().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application expression references absent variable `{name}`"
                    ))
                })
            }
            ApplicationExpressionV1::Literal { value } => Ok(value.clone()),
            ApplicationExpressionV1::Object { fields } => fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), self.expression(value, environment)?)))
                .collect::<Result<Map<String, Value>>>()
                .map(Value::Object),
            ApplicationExpressionV1::Array { items } => items
                .iter()
                .map(|item| self.expression(item, environment))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            ApplicationExpressionV1::Field { target, field } => {
                let target = self.expression(target, environment)?;
                Ok(target
                    .as_object()
                    .and_then(|object| object.get(field))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            ApplicationExpressionV1::Index { target, index } => {
                let target = self.expression(target, environment)?;
                let index = self.expression(index, environment)?;
                match (target, index) {
                    (Value::Array(values), Value::Number(index)) => Ok(index
                        .as_u64()
                        .and_then(|index| values.get(index as usize))
                        .cloned()
                        .unwrap_or(Value::Null)),
                    (Value::Object(mut envelope), Value::Number(index))
                        if envelope.get("page_info").is_some_and(Value::is_object)
                            && envelope.get("items").is_some_and(Value::is_array) =>
                    {
                        Ok(index
                            .as_u64()
                            .and_then(|index| {
                                envelope
                                    .remove("items")?
                                    .as_array()?
                                    .get(index as usize)
                                    .cloned()
                            })
                            .unwrap_or(Value::Null))
                    }
                    (Value::Object(values), Value::String(index)) => {
                        Ok(values.get(&index).cloned().unwrap_or(Value::Null))
                    }
                    (Value::Object(values), Value::Number(index))
                        if values.get("items").is_some_and(Value::is_array)
                            && values.get("page_info").is_some_and(Value::is_object) =>
                    {
                        Ok(index
                            .as_u64()
                            .and_then(|index| values["items"].as_array()?.get(index as usize))
                            .cloned()
                            .unwrap_or(Value::Null))
                    }
                    _ => Err(AppRuntimeError::InvalidRequest(
                        "BicDB application index requires an array/integer or object/string pair"
                            .to_string(),
                    )),
                }
            }
            ApplicationExpressionV1::Unary {
                operator,
                value,
                value_type,
                operand_type,
            } => {
                let value = self.expression(value, environment)?;
                match operator.as_str() {
                    "not" => Ok(Value::Bool(!boolean(value, "not operand")?)),
                    "negate"
                        if operand_type.or(*value_type)
                            == Some(ApplicationExpressionTypeV1::Int) =>
                    {
                        let value = integer(value, "negate operand")?;
                        value
                            .checked_neg()
                            .map(|value| Value::Number(Number::from(value)))
                            .ok_or_else(int_overflow)
                    }
                    "negate"
                        if operand_type.or(*value_type)
                            == Some(ApplicationExpressionTypeV1::Decimal) =>
                    {
                        let value = decimal(value, "negate operand")?;
                        Decimal::ZERO
                            .checked_sub(value)
                            .map(decimal_value)
                            .ok_or_else(decimal_overflow)
                    }
                    "negate" => number(-numeric(value, "negate operand")?),
                    _ => Err(AppRuntimeError::InvalidPackage(format!(
                        "unknown BicDB application unary operator `{operator}`"
                    ))),
                }
            }
            ApplicationExpressionV1::Binary {
                operator,
                left,
                right,
                value_type,
                left_type,
                right_type,
            } => self.binary(
                operator,
                *value_type,
                *left_type,
                *right_type,
                left,
                right,
                environment,
            ),
            ApplicationExpressionV1::Match { value, arms } => {
                let value = self.expression(value, environment)?;
                let variant = value.as_str();
                let arm = arms
                    .iter()
                    .find(|arm| arm.variant.as_deref() == variant)
                    .or_else(|| arms.iter().find(|arm| arm.variant.is_none()))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application match expression has no matching arm".to_string(),
                        )
                    })?;
                self.expression(&arm.value, environment)
            }
            ApplicationExpressionV1::Call {
                kind,
                target,
                method,
                result_type,
                argument_types,
                argument_item_types,
                result_projection,
                arguments,
            } => {
                if *kind == ApplicationCallKindV1::Builtin && target == "array.push" {
                    return self.array_push(arguments, environment);
                }
                if *kind == ApplicationCallKindV1::Builtin && target == "memoize.call" {
                    return self.memoize_call(arguments, environment);
                }
                let arguments = self.arguments(arguments, environment)?;
                let value = match kind {
                    ApplicationCallKindV1::Function => self.call(target, arguments),
                    ApplicationCallKindV1::Action
                        if self.program.callables.contains_key(target) =>
                    {
                        self.call(target, arguments)
                    }
                    ApplicationCallKindV1::Action => {
                        self.host.call(*kind, target, method.as_deref(), arguments)
                    }
                    ApplicationCallKindV1::Builtin => self.builtin(
                        target,
                        arguments,
                        *result_type,
                        argument_types,
                        argument_item_types,
                    ),
                    _ => self.host.call(*kind, target, method.as_deref(), arguments),
                }?;
                match result_projection {
                    Some(projection) if *kind == ApplicationCallKindV1::Model => {
                        self.project_model_result(value, projection)
                    }
                    Some(projection) if *kind == ApplicationCallKindV1::Builtin => {
                        self.project_declared_sql_result(target, value, projection)
                    }
                    Some(_) => Err(AppRuntimeError::InvalidPackage(
                        "BicDB application result projections are valid only on model or typed declared SQL calls"
                            .to_string(),
                    )),
                    None => Ok(value),
                }
            }
            ApplicationExpressionV1::Exists { .. } | ApplicationExpressionV1::Aggregate { .. } => {
                Err(AppRuntimeError::InvalidPackage(
                    "invariant-only expression escaped into the BicDB application behavior program"
                        .to_string(),
                ))
            }
        }
    }

    fn project_model_result(
        &mut self,
        value: Value,
        projection: &ApplicationModelProjectionV1,
    ) -> Result<Value> {
        let mut envelope = value.as_object().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "paged BicDB application model call did not return an object envelope".to_string(),
            )
        })?;
        let rows = envelope
            .remove("items")
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "paged BicDB application model call did not return an items array".to_string(),
                )
            })?;
        if !envelope.get("page_info").is_some_and(Value::is_object) {
            return Err(AppRuntimeError::InvalidPackage(
                "paged BicDB application model call did not return page_info".to_string(),
            ));
        }
        let mut relation_cache = BTreeMap::<(String, String), Value>::new();
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(self.project_result_row(row, projection, &mut relation_cache)?);
        }
        envelope.insert("items".to_string(), Value::Array(items));
        Ok(Value::Object(envelope))
    }

    fn project_declared_sql_result(
        &mut self,
        target: &str,
        value: Value,
        projection: &ApplicationModelProjectionV1,
    ) -> Result<Value> {
        let mut relation_cache = BTreeMap::<(String, String), Value>::new();
        match target {
            "sql.one_as" | "db.call_as" | "db.fn_one_as" | "db.graph_one_as" => {
                self.project_result_row(value, projection, &mut relation_cache)
            }
            "sql.list_as" | "db.graph_list_as" => {
                let rows = value.as_array().cloned().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application typed declared SQL `{target}` returned a non-list result"
                    ))
                })?;
                rows.into_iter()
                    .map(|row| self.project_result_row(row, projection, &mut relation_cache))
                    .collect::<Result<Vec<_>>>()
                    .map(Value::Array)
            }
            _ => Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application result projection is not valid on builtin `{target}`"
            ))),
        }
    }

    fn project_result_row(
        &mut self,
        row: Value,
        projection: &ApplicationModelProjectionV1,
        relation_cache: &mut BTreeMap<(String, String), Value>,
    ) -> Result<Value> {
        let source = row.as_object().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application projected result contains a non-object item".to_string(),
            )
        })?;
        let mut projected = Map::new();
        for field in &projection.fields {
            let (name, value) = match field {
                ApplicationProjectionFieldV1::Field { name, field } => {
                    let value = source.get(field).cloned().ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "BicDB application projection source field `{field}` is absent"
                        ))
                    })?;
                    (name, value)
                }
                ApplicationProjectionFieldV1::Relation {
                    name,
                    source_field,
                    target,
                    target_key,
                    target_field,
                } => {
                    let key = source.get(source_field).cloned().ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "BicDB application relation projection source field `{source_field}` is absent"
                        ))
                    })?;
                    if key.is_null() {
                        (name, Value::Null)
                    } else {
                        let cache_key = (target.clone(), serde_json::to_string(&key)?);
                        let target_row = if let Some(value) = relation_cache.get(&cache_key) {
                            value.clone()
                        } else {
                            let value = self.host.call(
                                ApplicationCallKindV1::Model,
                                target,
                                Some("get"),
                                vec![(Some(target_key.clone()), key)],
                            )?;
                            relation_cache.insert(cache_key, value.clone());
                            value
                        };
                        let value = target_row.get(target_field).cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(format!(
                                "BicDB application relation projection target field `{target}.{target_field}` is absent"
                            ))
                        })?;
                        (name, value)
                    }
                }
                ApplicationProjectionFieldV1::Computed { name, expression } => {
                    let mut environment = BTreeMap::from([("source".to_string(), row.clone())]);
                    (name, self.expression(expression, &mut environment)?)
                }
            };
            projected.insert(name.clone(), value);
        }
        Ok(Value::Object(projected))
    }

    fn array_push(
        &mut self,
        arguments: &[ApplicationArgumentV1],
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Value> {
        if arguments.len() != 2 || arguments.iter().any(|argument| argument.name.is_some()) {
            return Err(AppRuntimeError::InvalidPackage(
                "array.push requires a list variable and one positional value".to_string(),
            ));
        }
        let ApplicationExpressionV1::Variable { name } = &arguments[0].value else {
            return Err(AppRuntimeError::InvalidPackage(
                "array.push first argument must be a list variable".to_string(),
            ));
        };
        let value = self.expression(&arguments[1].value, environment)?;
        let items = environment
            .get_mut(name)
            .and_then(|value| match value {
                Value::Array(items) => Some(items),
                Value::Object(envelope)
                    if envelope.get("page_info").is_some_and(Value::is_object) =>
                {
                    envelope.get_mut("items").and_then(Value::as_array_mut)
                }
                _ => None,
            })
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "array.push references absent or non-list variable `{name}`"
                ))
            })?;
        items.push(value);
        Ok(Value::Null)
    }

    fn memoize_call(
        &mut self,
        arguments: &[ApplicationArgumentV1],
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Value> {
        let arguments = self.arguments(arguments, environment)?;
        if arguments.len() < 2
            || arguments.first().is_some_and(|(name, _)| name.is_some())
            || arguments
                .last()
                .is_none_or(|(name, _)| name.as_deref() != Some("ttl_seconds"))
            || arguments[1..arguments.len() - 1]
                .iter()
                .any(|(name, _)| name.is_some())
        {
            return Err(AppRuntimeError::InvalidPackage(
                "memoize.call requires a callback, positional arguments, and named ttl_seconds"
                    .to_string(),
            ));
        }
        let function = string(arguments[0].1.clone(), "memoize.call callback")?;
        if !self.program.callables.contains_key(&function) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "memoize.call references absent callable `{function}`"
            )));
        }
        let ttl_seconds = integer(
            arguments
                .last()
                .expect("memoize arguments were validated")
                .1
                .clone(),
            "memoize.call ttl_seconds",
        )?;
        if ttl_seconds <= 0 {
            return Err(AppRuntimeError::InvalidRequest(
                "memoize ttl_seconds must be greater than 0".to_string(),
            ));
        }
        let callback_arguments = arguments[1..arguments.len() - 1].to_vec();
        let cache_key = format!(
            "memoize:{function}:{}",
            serde_json::to_string(&Value::Array(
                callback_arguments
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect(),
            ))?
        );
        match self.host.call(
            ApplicationCallKindV1::Builtin,
            "cache.get_as",
            None,
            vec![
                (None, Value::String("Json".to_string())),
                (None, Value::String(cache_key.clone())),
            ],
        ) {
            Ok(value) => return Ok(value),
            Err(AppRuntimeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let value = self.call(&function, callback_arguments)?;
        self.host.call(
            ApplicationCallKindV1::Builtin,
            "cache.set",
            None,
            vec![
                (None, Value::String(cache_key)),
                (None, value.clone()),
                (Some("ttl_seconds".to_string()), Value::from(ttl_seconds)),
            ],
        )?;
        Ok(value)
    }

    fn arguments(
        &mut self,
        arguments: &[ApplicationArgumentV1],
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Vec<(Option<String>, Value)>> {
        arguments
            .iter()
            .map(|argument| {
                Ok((
                    argument.name.clone(),
                    self.expression(&argument.value, environment)?,
                ))
            })
            .collect()
    }

    fn binary(
        &mut self,
        operator: &str,
        value_type: Option<ApplicationExpressionTypeV1>,
        left_type: Option<ApplicationExpressionTypeV1>,
        right_type: Option<ApplicationExpressionTypeV1>,
        left: &ApplicationExpressionV1,
        right: &ApplicationExpressionV1,
        environment: &mut BTreeMap<String, Value>,
    ) -> Result<Value> {
        let left = self.expression(left, environment)?;
        match operator {
            "and" if !boolean(left.clone(), "and operand")? => {
                return Ok(Value::Bool(false));
            }
            "or" if boolean(left.clone(), "or operand")? => return Ok(Value::Bool(true)),
            "implies" if !boolean(left.clone(), "implies operand")? => {
                return Ok(Value::Bool(true));
            }
            _ => {}
        }
        let right = self.expression(right, environment)?;
        if value_type == Some(ApplicationExpressionTypeV1::Int) {
            return checked_integer_binary(operator, left, right);
        }
        if value_type == Some(ApplicationExpressionTypeV1::Decimal) {
            return checked_decimal_binary(operator, left, right);
        }
        let decimal_operands = left_type == Some(ApplicationExpressionTypeV1::Decimal)
            || right_type == Some(ApplicationExpressionTypeV1::Decimal);
        match operator {
            "add" if value_type == Some(ApplicationExpressionTypeV1::String) => {
                Ok(Value::String(format!(
                    "{}{}",
                    string(left, "add operand")?,
                    string(right, "add operand")?
                )))
            }
            "add" => match (&left, &right) {
                (Value::String(_), _) | (_, Value::String(_)) => Ok(Value::String(format!(
                    "{}{}",
                    display(&left),
                    display(&right)
                ))),
                _ => number(numeric(left, "add operand")? + numeric(right, "add operand")?),
            },
            "subtract" => {
                number(numeric(left, "subtract operand")? - numeric(right, "subtract operand")?)
            }
            "multiply" => {
                number(numeric(left, "multiply operand")? * numeric(right, "multiply operand")?)
            }
            "divide" => {
                let divisor = numeric(right, "divide operand")?;
                if divisor == 0.0 {
                    return Err(AppRuntimeError::InvalidRequest(
                        "BicDB application division by zero".to_string(),
                    ));
                }
                number(numeric(left, "divide operand")? / divisor)
            }
            "and" | "or" | "implies" => Ok(Value::Bool(boolean(right, "boolean operand")?)),
            // Null-aware equality FIRST: `line.rate != null` is how BicDB application
            // behavior code tests an optional Decimal, and forcing null
            // through the ordered decimal parser rejected every optional
            // field check. Equality against null is an is-null test; ordered
            // comparisons on null stay errors.
            "equal" if left.is_null() || right.is_null() => {
                Ok(Value::Bool(left.is_null() && right.is_null()))
            }
            "not_equal" if left.is_null() || right.is_null() => {
                Ok(Value::Bool(left.is_null() != right.is_null()))
            }
            "equal" if decimal_operands => {
                decimal_compare(left, right, |ordering| ordering.is_eq())
            }
            "not_equal" if decimal_operands => {
                decimal_compare(left, right, |ordering| ordering.is_ne())
            }
            "greater" if decimal_operands => {
                decimal_compare(left, right, |ordering| ordering.is_gt())
            }
            "greater_equal" if decimal_operands => {
                decimal_compare(left, right, |ordering| ordering.is_ge())
            }
            "less" if decimal_operands => decimal_compare(left, right, |ordering| ordering.is_lt()),
            "less_equal" if decimal_operands => {
                decimal_compare(left, right, |ordering| ordering.is_le())
            }
            "equal" => Ok(Value::Bool(left == right)),
            "not_equal" => Ok(Value::Bool(left != right)),
            "greater" => compare(left, right, |ordering| ordering.is_gt()),
            "greater_equal" => compare(left, right, |ordering| ordering.is_ge()),
            "less" => compare(left, right, |ordering| ordering.is_lt()),
            "less_equal" => compare(left, right, |ordering| ordering.is_le()),
            "contains" => Ok(Value::Bool(match left {
                Value::String(value) => value.contains(&string(right, "contains value")?),
                Value::Array(values) => values.contains(&right),
                _ => false,
            })),
            _ => Err(AppRuntimeError::InvalidPackage(format!(
                "unknown BicDB application binary operator `{operator}`"
            ))),
        }
    }

    fn builtin(
        &mut self,
        target: &str,
        arguments: Vec<(Option<String>, Value)>,
        result_type: Option<ApplicationExpressionTypeV1>,
        argument_types: &[ApplicationExpressionTypeV1],
        argument_item_types: &[Option<ApplicationExpressionTypeV1>],
    ) -> Result<Value> {
        let host_arguments = arguments.clone();
        let values = arguments
            .iter()
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        let first = || {
            values.first().cloned().ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires an argument"))
            })
        };
        match target {
            "string.lower" => Ok(Value::String(string(first()?, target)?.to_lowercase())),
            "string.upper" => Ok(Value::String(string(first()?, target)?.to_uppercase())),
            "string.trim" => Ok(Value::String(string(first()?, target)?.trim().to_string())),
            "string.starts_with" => Ok(Value::Bool(
                string(first()?, target)?
                    .starts_with(&string(argument(&values, 1, target)?, target)?),
            )),
            "string.ends_with" => Ok(Value::Bool(
                string(first()?, target)?
                    .ends_with(&string(argument(&values, 1, target)?, target)?),
            )),
            "string.replace" => Ok(Value::String(string(first()?, target)?.replace(
                &string(argument(&values, 1, target)?, target)?,
                &string(argument(&values, 2, target)?, target)?,
            ))),
            "string.slug" => Ok(Value::String(slug(&string(first()?, target)?))),
            "string.split" => {
                let value = string(first()?, target)?;
                let separator = string(argument(&values, 1, target)?, target)?;
                Ok(Value::Array(if separator.is_empty() {
                    value
                        .chars()
                        .map(|value| Value::String(value.to_string()))
                        .collect()
                } else {
                    value
                        .split(&separator)
                        .map(|value| Value::String(value.to_string()))
                        .collect()
                }))
            }
            "string.truncate" => {
                let value = string(first()?, target)?;
                let maximum = integer(argument(&values, 1, target)?, target)?.max(0) as usize;
                Ok(Value::String(value.chars().take(maximum).collect()))
            }
            "string.normalize" => Ok(Value::String(normalize_string(&string(first()?, target)?))),
            "string.is_empty" => Ok(Value::Bool(string(first()?, target)?.is_empty())),
            "string.is_blank" => Ok(Value::Bool(string(first()?, target)?.trim().is_empty())),
            "string.length" => Ok(Value::Number(Number::from(match first()? {
                Value::String(value) => value.chars().count() as u64,
                _ => {
                    return Err(AppRuntimeError::InvalidRequest(format!(
                        "builtin `{target}` requires a string"
                    )))
                }
            }))),
            "array.length" => Ok(Value::Number(Number::from(
                array(first()?, target)?.len() as u64
            ))),
            "string.contains" => Ok(Value::Bool(string(first()?, target)?.contains(&string(
                values.get(1).cloned().unwrap_or(Value::Null),
                target,
            )?))),
            "array.first" => Ok(array(first()?, target)?
                .first()
                .cloned()
                .unwrap_or(Value::Null)),
            "array.last" => Ok(array(first()?, target)?
                .last()
                .cloned()
                .unwrap_or(Value::Null)),
            "array.slice" => {
                let start = integer(argument(&values, 1, target)?, target)?.max(0) as usize;
                let length = integer(argument(&values, 2, target)?, target)?.max(0) as usize;
                let input = array(first()?, target)?;
                Ok(Value::Array(
                    input.into_iter().skip(start).take(length).collect(),
                ))
            }
            "array.join" => Ok(Value::String(
                array(first()?, target)?
                    .iter()
                    .map(display)
                    .collect::<Vec<_>>()
                    .join(&string(argument(&values, 1, target)?, target)?),
            )),
            "array.distinct" | "set.from" => {
                let mut output = Vec::new();
                for value in array(first()?, target)? {
                    if !output.contains(&value) {
                        output.push(value);
                    }
                }
                Ok(Value::Array(output))
            }
            "array.sort" => {
                let mut output = array(first()?, target)?;
                sort_carrier_array(&mut output, argument_item_types.first().copied().flatten())?;
                Ok(Value::Array(output))
            }
            "array.contains" | "set.contains" => Ok(Value::Bool(
                array(first()?, target)?.contains(&argument(&values, 1, target)?),
            )),
            "array.push" => Err(AppRuntimeError::InvalidPackage(
                "array.push must execute against its signed list variable".to_string(),
            )),
            "array.partition" => {
                let items = array(first()?, target)?;
                let callback = string(argument(&values, 1, target)?, target)?;
                let mut matched = Vec::new();
                let mut unmatched = Vec::new();
                for item in items {
                    let selected = boolean(
                        self.call(&callback, vec![(None, item.clone())])?,
                        "array.partition callback",
                    )?;
                    if selected {
                        matched.push(item);
                    } else {
                        unmatched.push(item);
                    }
                }
                Ok(serde_json::json!({
                    "matched": matched,
                    "unmatched": unmatched,
                }))
            }
            "array.group_by" | "array.index_by" => {
                let items = array(first()?, target)?;
                let callback = string(argument(&values, 1, target)?, target)?;
                let mut entries = Vec::<Value>::new();
                for item in items {
                    let key = self.call(&callback, vec![(None, item.clone())])?;
                    if let Some(existing) = entries
                        .iter_mut()
                        .find(|entry| entry.get("key") == Some(&key))
                    {
                        if target == "array.group_by" {
                            let group = existing
                                .get_mut("value")
                                .and_then(Value::as_array_mut)
                                .ok_or_else(|| {
                                    AppRuntimeError::InvalidPackage(
                                        "array.group_by produced an invalid internal entry"
                                            .to_string(),
                                    )
                                })?;
                            group.push(item);
                        } else {
                            existing["value"] = item;
                        }
                    } else {
                        entries.push(if target == "array.group_by" {
                            serde_json::json!({"key": key, "value": [item]})
                        } else {
                            serde_json::json!({"key": key, "value": item})
                        });
                    }
                }
                Ok(Value::Array(entries))
            }
            "array.chunk" => {
                let input = array(first()?, target)?;
                let size = integer(argument(&values, 1, target)?, target)?.max(0) as usize;
                if size == 0 {
                    return Ok(Value::Array(Vec::new()));
                }
                Ok(Value::Array(
                    input
                        .chunks(size)
                        .map(|chunk| Value::Array(chunk.to_vec()))
                        .collect(),
                ))
            }
            "map.from" => {
                let mut output: Vec<Value> = Vec::new();
                for entry in array(first()?, target)? {
                    let entry = object(entry, target)?;
                    let key = entry.get("key").cloned().ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "map.from entries must contain `key`".to_string(),
                        )
                    })?;
                    let value = entry.get("value").cloned().ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "map.from entries must contain `value`".to_string(),
                        )
                    })?;
                    let normalized = serde_json::json!({"key": key, "value": value});
                    if let Some(existing) = output
                        .iter_mut()
                        .find(|existing| existing.get("key") == normalized.get("key"))
                    {
                        *existing = normalized;
                    } else {
                        output.push(normalized);
                    }
                }
                Ok(Value::Array(output))
            }
            "map.get" => {
                let entries = carrier_map(first()?, target)?;
                let key = argument(&values, 1, target)?;
                Ok(entries
                    .iter()
                    .find(|entry| entry.get("key") == Some(&key))
                    .and_then(|entry| entry.get("value"))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            "map.contains" => {
                let entries = carrier_map(first()?, target)?;
                let key = argument(&values, 1, target)?;
                Ok(Value::Bool(
                    entries.iter().any(|entry| entry.get("key") == Some(&key)),
                ))
            }
            "json.parse" => serde_json::from_str(&string(first()?, target)?).map_err(Into::into),
            "json.stringify" => Ok(Value::String(serde_json::to_string(&first()?)?)),
            "json.get" => Ok(object(first()?, target)?
                .get(&string(argument(&values, 1, target)?, target)?)
                .cloned()
                .unwrap_or(Value::Null)),
            "json.path" => Ok(Value::Array(json_path(
                &first()?,
                &string(argument(&values, 1, target)?, target)?,
            )?)),
            "json.path_first" => Ok(json_path(
                &first()?,
                &string(argument(&values, 1, target)?, target)?,
            )?
            .into_iter()
            .next()
            .unwrap_or(Value::Null)),
            "json.exists" => Ok(Value::Bool(
                !json_path(&first()?, &string(argument(&values, 1, target)?, target)?)?.is_empty(),
            )),
            "base64.encode" => Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(string(first()?, target)?),
            )),
            "base64.decode" => Ok(Value::String(
                String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(string(first()?, target)?)
                        .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?,
                )
                .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?,
            )),
            "base64url.encode" => Ok(Value::String(
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(string(first()?, target)?),
            )),
            "base64url.decode" => Ok(Value::String(
                String::from_utf8(carrier_base64url_decode(&string(first()?, target)?)?)
                    .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?,
            )),
            "hex.encode" => Ok(Value::String(hex_encode(
                string(first()?, target)?.as_bytes(),
            ))),
            "hex.decode" => Ok(Value::String(
                String::from_utf8(hex_decode(&string(first()?, target)?)?)
                    .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?,
            )),
            "url.parse" => carrier_url_parse(&string(first()?, target)?),
            "url.query_get" => {
                let parts = object(first()?, target)?;
                let key = string(argument(&values, 1, target)?, target)?;
                Ok(parts
                    .get("query")
                    .and_then(Value::as_str)
                    .and_then(|query| {
                        url::form_urlencoded::parse(query.as_bytes())
                            .find_map(|(name, value)| (name == key).then(|| value.into_owned()))
                    })
                    .map(Value::String)
                    .unwrap_or(Value::Null))
            }
            "url.normalize" => Ok(Value::String(string(
                object(first()?, target)?
                    .get("normalized")
                    .cloned()
                    .unwrap_or(Value::Null),
                target,
            )?)),
            "url.encode" => Ok(Value::String(carrier_url_encode(&string(
                first()?,
                target,
            )?))),
            "url.decode" => Ok(Value::String(percent_decode(&string(first()?, target)?)?)),
            "finance.decimal"
            | "finance.money"
            | "finance.add_money"
            | "finance.subtract_money"
            | "finance.round_money"
            | "finance.round"
            | "finance.abs"
            | "finance.tax_exclusive"
            | "finance.tax_inclusive"
            | "finance.withholding"
            | "finance.tax_exclusive_money"
            | "finance.tax_inclusive_money"
            | "finance.withholding_money" => carrier_finance(target, &values),
            "address.normalize" | "address.postal_valid" => carrier_address(target, &values),
            "phone.parse"
            | "phone.is_valid"
            | "phone.e164"
            | "phone.format_national"
            | "phone.format_international" => carrier_phone(target, &values),
            "locale.number_format" | "locale.currency_format" | "locale.date_format" => {
                carrier_locale(target, &values, argument_types)
            }
            "template.render_text" | "template.render_html" => carrier_template(target, &values),
            "time.now"
            | "time.today"
            | "time.timezone"
            | "time.local_datetime"
            | "time.zoned"
            | "time.schedule"
            | "time.add"
            | "time.diff"
            | "time.epoch_seconds" => {
                self.carrier_time(target, &arguments, &values, result_type, argument_types)
            }
            "math.min" | "math.max" | "math.abs" | "math.floor" | "math.ceil" | "math.round"
            | "math.pow" | "math.sqrt" | "math.log" => {
                carrier_math(target, &values, result_type, argument_types)
            }
            "overlaps" => {
                let left_start = numeric(first()?, target)?;
                let left_end = numeric(argument(&values, 1, target)?, target)?;
                let right_start = numeric(argument(&values, 2, target)?, target)?;
                let right_end = numeric(argument(&values, 3, target)?, target)?;
                Ok(Value::Bool(
                    left_start < right_end && right_start < left_end,
                ))
            }
            "crypto.sha256_hex" => Ok(Value::String(hex_encode(&Sha256::digest(
                string(first()?, target)?.as_bytes(),
            )))),
            "crypto.hmac_sha256_hex" => {
                let input = string(first()?, target)?;
                let secret = string(argument(&values, 1, target)?, target)?;
                let mut hmac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| {
                    AppRuntimeError::InvalidRequest("invalid HMAC secret".to_string())
                })?;
                hmac.update(input.as_bytes());
                Ok(Value::String(hex_encode(&hmac.finalize().into_bytes())))
            }
            "crypto.verify_hmac_sha256" => {
                let input = string(first()?, target)?;
                let secret = string(argument(&values, 1, target)?, target)?;
                let signature = hex_decode(&string(argument(&values, 2, target)?, target)?)?;
                let mut hmac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| {
                    AppRuntimeError::InvalidRequest("invalid HMAC secret".to_string())
                })?;
                hmac.update(input.as_bytes());
                Ok(Value::Bool(hmac.verify_slice(&signature).is_ok()))
            }
            "crypto.constant_time_equals" => {
                let left = string(first()?, target)?;
                let right = string(argument(&values, 1, target)?, target)?;
                let mut difference = left.len() ^ right.len();
                for (left, right) in left.bytes().zip(right.bytes()) {
                    difference |= usize::from(left ^ right);
                }
                Ok(Value::Bool(difference == 0))
            }
            "db.has_capability" => {
                let _capability = string(first()?, target)?;
                // BicDB currently exposes BicDB application graph helpers through their
                // compiler-signed bounded SQL fallback, so no optional graph
                // capability is advertised to application code.
                Ok(Value::Bool(false))
            }
            _ => self
                .host
                .call(ApplicationCallKindV1::Builtin, target, None, host_arguments),
        }
    }

    fn carrier_time(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
        values: &[Value],
        result_type: Option<ApplicationExpressionTypeV1>,
        argument_types: &[ApplicationExpressionTypeV1],
    ) -> Result<Value> {
        let first = || argument(values, 0, target);
        match target {
            "time.timezone" => {
                let timezone = string(first()?, target)?;
                parse_timezone(&timezone, target)?;
                Ok(Value::String(timezone))
            }
            "time.now" if values.is_empty() => self.host.call(
                ApplicationCallKindV1::Builtin,
                target,
                None,
                arguments.to_vec(),
            ),
            "time.now" => {
                let timezone = string(first()?, target)?;
                let instant =
                    self.host
                        .call(ApplicationCallKindV1::Builtin, "time.now", None, Vec::new())?;
                zoned_value(parse_timestamp(instant, target)?, &timezone, target)
            }
            "time.today" if values.is_empty() => self.host.call(
                ApplicationCallKindV1::Builtin,
                target,
                None,
                arguments.to_vec(),
            ),
            "time.today" => {
                let timezone = string(first()?, target)?;
                let instant =
                    self.host
                        .call(ApplicationCallKindV1::Builtin, "time.now", None, Vec::new())?;
                let instant = parse_timestamp(instant, target)?;
                let timezone = parse_timezone(&timezone, target)?;
                Ok(Value::String(
                    instant.with_timezone(&timezone).date_naive().to_string(),
                ))
            }
            "time.local_datetime" => {
                let date = parse_time_date(first()?, target)?;
                let hour = named_integer(arguments, "hour", target)?.ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "time.local_datetime requires named argument `hour`".to_string(),
                    )
                })?;
                let minute = named_integer(arguments, "minute", target)?.ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "time.local_datetime requires named argument `minute`".to_string(),
                    )
                })?;
                let second = named_integer(arguments, "second", target)?.unwrap_or(0);
                if !(0..=23).contains(&hour) {
                    return Err(invalid_time("time.local_datetime hour must be in 0..23"));
                }
                if !(0..=59).contains(&minute) {
                    return Err(invalid_time("time.local_datetime minute must be in 0..59"));
                }
                if !(0..=59).contains(&second) {
                    return Err(invalid_time("time.local_datetime second must be in 0..59"));
                }
                let local = date
                    .and_hms_opt(hour as u32, minute as u32, second as u32)
                    .ok_or_else(|| invalid_time("invalid local date-time"))?;
                Ok(Value::String(format_local_datetime(local)))
            }
            "time.zoned" => {
                let instant = parse_timestamp(first()?, target)?;
                let timezone = string(argument(values, 1, target)?, target)?;
                zoned_value(instant, &timezone, target)
            }
            "time.schedule" => {
                let local = parse_local_datetime(first()?, target)?;
                let timezone = string(argument(values, 1, target)?, target)?;
                let ambiguous = named_string(arguments, "ambiguous", target)?.ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "time.schedule requires named argument `ambiguous`".to_string(),
                    )
                })?;
                let gap = named_string(arguments, "gap", target)?.ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "time.schedule requires named argument `gap`".to_string(),
                    )
                })?;
                schedule_value(local, &timezone, &ambiguous, &gap, target)
            }
            "time.add" => {
                let seconds = named_integer(arguments, "seconds", target)?.unwrap_or(0);
                let minutes = named_integer(arguments, "minutes", target)?.unwrap_or(0);
                let hours = named_integer(arguments, "hours", target)?.unwrap_or(0);
                let days = named_integer(arguments, "days", target)?.unwrap_or(0);
                match result_type {
                    Some(ApplicationExpressionTypeV1::Date) => {
                        let date = parse_time_date(first()?, target)?;
                        let date = if days >= 0 {
                            date.checked_add_days(Days::new(days as u64))
                        } else {
                            date.checked_sub_days(Days::new(days.unsigned_abs()))
                        }
                        .ok_or_else(|| invalid_time("time.add date overflowed"))?;
                        Ok(Value::String(date.to_string()))
                    }
                    Some(ApplicationExpressionTypeV1::Timestamp) => {
                        let instant = parse_timestamp(first()?, target)?;
                        let instant = checked_time_add(instant, seconds, minutes, hours, days)?;
                        Ok(timestamp_value(instant))
                    }
                    Some(ApplicationExpressionTypeV1::ZonedDateTime) => {
                        let zoned = parse_zoned(first()?, target)?;
                        let duration = time_duration(seconds, minutes, hours, days)?;
                        let local = zoned
                            .local
                            .checked_add_signed(duration)
                            .ok_or_else(|| invalid_time("time.add zoned date-time overflowed"))?;
                        let ambiguous = named_string(arguments, "ambiguous", target)?
                            .unwrap_or_else(|| "earlier".to_string());
                        let gap = named_string(arguments, "gap", target)?
                            .unwrap_or_else(|| "next_valid".to_string());
                        schedule_value(local, &zoned.timezone, &ambiguous, &gap, target)
                    }
                    _ => Err(AppRuntimeError::InvalidPackage(
                        "time.add lacks a signed Date, Time, or ZonedDateTime result type"
                            .to_string(),
                    )),
                }
            }
            "time.diff" => {
                let unit = named_string(arguments, "unit", target)?.ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "time.diff requires named argument `unit`".to_string(),
                    )
                })?;
                let difference = match argument_types.first() {
                    Some(ApplicationExpressionTypeV1::Date) => {
                        if unit != "days" {
                            return Err(invalid_time(
                                "time.diff only supports unit `days` for Date values",
                            ));
                        }
                        parse_time_date(first()?, target)?
                            .signed_duration_since(parse_time_date(
                                argument(values, 1, target)?,
                                target,
                            )?)
                            .num_days()
                    }
                    Some(ApplicationExpressionTypeV1::Timestamp) => time_difference(
                        parse_timestamp(first()?, target)?,
                        parse_timestamp(argument(values, 1, target)?, target)?,
                        &unit,
                    )?,
                    Some(ApplicationExpressionTypeV1::ZonedDateTime) => time_difference(
                        parse_zoned(first()?, target)?.instant,
                        parse_zoned(argument(values, 1, target)?, target)?.instant,
                        &unit,
                    )?,
                    _ => {
                        return Err(AppRuntimeError::InvalidPackage(
                            "time.diff lacks signed operand types".to_string(),
                        ));
                    }
                };
                Ok(Value::Number(Number::from(difference)))
            }
            "time.epoch_seconds" if values.is_empty() => {
                let instant =
                    self.host
                        .call(ApplicationCallKindV1::Builtin, "time.now", None, Vec::new())?;
                Ok(Value::Number(Number::from(
                    parse_timestamp(instant, target)?.timestamp(),
                )))
            }
            "time.epoch_seconds" => {
                let instant = match argument_types.first() {
                    Some(ApplicationExpressionTypeV1::Timestamp) => {
                        parse_timestamp(first()?, target)?
                    }
                    Some(ApplicationExpressionTypeV1::ZonedDateTime) => {
                        parse_zoned(first()?, target)?.instant
                    }
                    _ => {
                        return Err(AppRuntimeError::InvalidPackage(
                            "time.epoch_seconds lacks a signed Time or ZonedDateTime argument type"
                                .to_string(),
                        ));
                    }
                };
                Ok(Value::Number(Number::from(instant.timestamp())))
            }
            _ => Err(AppRuntimeError::InvalidPackage(format!(
                "unknown BicDB application time builtin `{target}`"
            ))),
        }
    }
}

#[derive(Debug)]
struct ParsedZonedDateTime {
    instant: DateTime<Utc>,
    local: NaiveDateTime,
    timezone: String,
}

fn named_value(arguments: &[(Option<String>, Value)], name: &str) -> Option<Value> {
    arguments
        .iter()
        .find(|(candidate, _)| candidate.as_deref() == Some(name))
        .map(|(_, value)| value.clone())
}

fn named_integer(
    arguments: &[(Option<String>, Value)],
    name: &str,
    target: &str,
) -> Result<Option<i64>> {
    named_value(arguments, name)
        .map(|value| integer(value, target))
        .transpose()
}

fn named_string(
    arguments: &[(Option<String>, Value)],
    name: &str,
    target: &str,
) -> Result<Option<String>> {
    named_value(arguments, name)
        .map(|value| string(value, target))
        .transpose()
}

fn invalid_time(message: impl Into<String>) -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(message.into())
}

fn parse_timezone(value: &str, _target: &str) -> Result<Tz> {
    value.parse::<Tz>().map_err(|_| {
        invalid_time(format!(
            "timezone must be a valid IANA zone like `America/New_York`, found `{value}`"
        ))
    })
}

fn parse_timestamp(value: Value, target: &str) -> Result<DateTime<Utc>> {
    let value = string(value, target)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| invalid_time(format!("{target} requires an RFC 3339 Time: {error}")))
}

fn parse_time_date(value: Value, target: &str) -> Result<NaiveDate> {
    let value = string(value, target)?;
    NaiveDate::parse_from_str(&value, "%Y-%m-%d")
        .map_err(|error| invalid_time(format!("{target} requires an ISO Date: {error}")))
}

fn parse_local_datetime(value: Value, target: &str) -> Result<NaiveDateTime> {
    let value = string(value, target)?;
    NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f").map_err(|error| {
        invalid_time(format!(
            "{target} requires an ISO LocalDateTime without an offset: {error}"
        ))
    })
}

fn format_local_datetime(value: NaiveDateTime) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
}

fn timestamp_value(value: DateTime<Utc>) -> Value {
    Value::String(value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

fn zoned_value(instant: DateTime<Utc>, timezone: &str, target: &str) -> Result<Value> {
    let parsed_timezone = parse_timezone(timezone, target)?;
    let zoned = instant.with_timezone(&parsed_timezone);
    Ok(serde_json::json!({
        "instant": timestamp_value(instant),
        "local": format_local_datetime(zoned.naive_local()),
        "date": zoned.date_naive().to_string(),
        "timezone": timezone,
        "offset_seconds": i64::from(zoned.offset().fix().local_minus_utc()),
    }))
}

fn parse_zoned(value: Value, target: &str) -> Result<ParsedZonedDateTime> {
    let value = object(value, target)?;
    let instant = parse_timestamp(
        value
            .get("instant")
            .cloned()
            .ok_or_else(|| invalid_time(format!("{target} ZonedDateTime lacks `instant`")))?,
        target,
    )?;
    let local = parse_local_datetime(
        value
            .get("local")
            .cloned()
            .ok_or_else(|| invalid_time(format!("{target} ZonedDateTime lacks `local`")))?,
        target,
    )?;
    let timezone = string(
        value
            .get("timezone")
            .cloned()
            .ok_or_else(|| invalid_time(format!("{target} ZonedDateTime lacks `timezone`")))?,
        target,
    )?;
    parse_timezone(&timezone, target)?;
    Ok(ParsedZonedDateTime {
        instant,
        local,
        timezone,
    })
}

fn schedule_value(
    local: NaiveDateTime,
    timezone: &str,
    ambiguous: &str,
    gap: &str,
    target: &str,
) -> Result<Value> {
    let parsed_timezone = parse_timezone(timezone, target)?;
    let instant = resolve_local_datetime(local, parsed_timezone, ambiguous, gap)?;
    zoned_value(instant, timezone, target)
}

fn resolve_local_datetime(
    local: NaiveDateTime,
    timezone: Tz,
    ambiguous: &str,
    gap: &str,
) -> Result<DateTime<Utc>> {
    match timezone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value.with_timezone(&Utc)),
        LocalResult::Ambiguous(earlier, later) => match ambiguous {
            "earlier" => Ok(earlier.with_timezone(&Utc)),
            "later" => Ok(later.with_timezone(&Utc)),
            "reject" => Err(invalid_time(format!(
                "local time `{local}` is ambiguous in `{timezone}`; use ambiguous: \"earlier\" or \"later\""
            ))),
            _ => Err(invalid_time(
                "ambiguous must be one of `earlier`, `later`, or `reject`",
            )),
        },
        LocalResult::None => match gap {
            "reject" => Err(invalid_time(format!(
                "local time `{local}` does not exist in `{timezone}` because of a DST gap"
            ))),
            "next_valid" => resolve_next_valid_local_datetime(local, timezone),
            _ => Err(invalid_time(
                "gap must be one of `next_valid` or `reject`",
            )),
        },
    }
}

fn resolve_next_valid_local_datetime(local: NaiveDateTime, timezone: Tz) -> Result<DateTime<Utc>> {
    let mut candidate = local;
    for _ in 0..(6 * 60 * 60) {
        candidate = candidate
            .checked_add_signed(ChronoDuration::seconds(1))
            .ok_or_else(|| invalid_time("local time overflowed while resolving DST gap"))?;
        match timezone.from_local_datetime(&candidate) {
            LocalResult::Single(value) => return Ok(value.with_timezone(&Utc)),
            LocalResult::Ambiguous(earlier, _) => return Ok(earlier.with_timezone(&Utc)),
            LocalResult::None => {}
        }
    }
    Err(invalid_time(format!(
        "could not find a valid local time after `{local}` in `{timezone}`"
    )))
}

fn time_duration(seconds: i64, minutes: i64, hours: i64, days: i64) -> Result<ChronoDuration> {
    [
        ChronoDuration::try_seconds(seconds),
        ChronoDuration::try_minutes(minutes),
        ChronoDuration::try_hours(hours),
        ChronoDuration::try_days(days),
    ]
    .into_iter()
    .try_fold(ChronoDuration::zero(), |total, value| {
        let value = value.ok_or_else(|| invalid_time("time.add duration overflowed"))?;
        total
            .checked_add(&value)
            .ok_or_else(|| invalid_time("time.add duration overflowed"))
    })
}

fn checked_time_add(
    value: DateTime<Utc>,
    seconds: i64,
    minutes: i64,
    hours: i64,
    days: i64,
) -> Result<DateTime<Utc>> {
    value
        .checked_add_signed(time_duration(seconds, minutes, hours, days)?)
        .ok_or_else(|| invalid_time("time.add timestamp overflowed"))
}

fn time_difference(left: DateTime<Utc>, right: DateTime<Utc>, unit: &str) -> Result<i64> {
    let delta = left.signed_duration_since(right);
    match unit {
        "seconds" => Ok(delta.num_seconds()),
        "minutes" => Ok(delta.num_minutes()),
        "hours" => Ok(delta.num_hours()),
        "days" => Ok(delta.num_days()),
        _ => Err(invalid_time(
            "time.diff unit must be one of `seconds`, `minutes`, `hours`, or `days`",
        )),
    }
}

fn argument(values: &[Value], index: usize, target: &str) -> Result<Value> {
    values.get(index).cloned().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires argument {index}"))
    })
}

fn array(value: Value, label: &str) -> Result<Vec<Value>> {
    match value {
        Value::Array(items) => Ok(items),
        Value::Object(mut envelope)
            if envelope.get("page_info").is_some_and(Value::is_object)
                && envelope.get("items").is_some_and(Value::is_array) =>
        {
            Ok(envelope
                .remove("items")
                .and_then(|items| items.as_array().cloned())
                .expect("items checked above"))
        }
        _ => Err(AppRuntimeError::InvalidRequest(format!(
            "BicDB application {label} must be an array"
        ))),
    }
}

fn object(value: Value, label: &str) -> Result<Map<String, Value>> {
    value.as_object().cloned().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("BicDB application {label} must be an object"))
    })
}

fn carrier_map(value: Value, label: &str) -> Result<Vec<Map<String, Value>>> {
    array(value, label)?
        .into_iter()
        .map(|entry| {
            let entry = object(entry, label)?;
            if !entry.contains_key("key") || !entry.contains_key("value") {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "BicDB application {label} map entries require `key` and `value`"
                )));
            }
            Ok(entry)
        })
        .collect()
}

fn slug(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_alphanumeric() {
            output.push(character);
        } else if !output.is_empty() && !output.ends_with('-') {
            output.push('-');
        }
    }
    output.trim_matches('-').to_string()
}

fn normalize_string(value: &str) -> String {
    let mut output = String::new();
    for segment in value.split_whitespace() {
        if !output.is_empty() {
            output.push(' ');
        }
        output.extend(segment.chars().flat_map(char::to_lowercase));
    }
    output
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathSegment {
    Field(String),
    Index(usize),
    Wildcard,
}

fn json_path(value: &Value, path: &str) -> Result<Vec<Value>> {
    let segments = parse_json_path(path)?;
    let mut current = vec![value.clone()];
    for segment in &segments {
        let mut next = Vec::new();
        for candidate in &current {
            match (segment, candidate) {
                (JsonPathSegment::Field(field), Value::Object(object)) => {
                    if let Some(child) = object.get(field) {
                        next.push(child.clone());
                    }
                }
                (JsonPathSegment::Index(index), Value::Array(items)) => {
                    if let Some(child) = items.get(*index) {
                        next.push(child.clone());
                    }
                }
                (JsonPathSegment::Wildcard, Value::Array(items)) => {
                    next.extend(items.iter().cloned());
                }
                _ => {}
            }
        }
        current = next;
    }
    Ok(current)
}

fn parse_json_path(path: &str) -> Result<Vec<JsonPathSegment>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return Err(invalid_json_path(path, 0, "json path must start with `$`"));
    }

    let mut index = 1;
    let mut segments = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'.' => {
                index += 1;
                let start = index;
                while index < bytes.len() && !matches!(bytes[index], b'.' | b'[') {
                    index += 1;
                }
                if start == index {
                    return Err(invalid_json_path(
                        path,
                        start,
                        "expected a field name after `.`",
                    ));
                }
                let field = &path[start..index];
                if field.chars().any(char::is_whitespace) {
                    return Err(invalid_json_path(
                        path,
                        start,
                        "field names cannot contain whitespace",
                    ));
                }
                segments.push(JsonPathSegment::Field(field.to_string()));
            }
            b'[' => {
                index += 1;
                if index >= bytes.len() {
                    return Err(invalid_json_path(
                        path,
                        index - 1,
                        "unterminated `[` segment",
                    ));
                }
                if bytes[index] == b'*' {
                    index += 1;
                    if bytes.get(index).copied() != Some(b']') {
                        return Err(invalid_json_path(
                            path,
                            index,
                            "wildcard segments must be written as `[*]`",
                        ));
                    }
                    index += 1;
                    segments.push(JsonPathSegment::Wildcard);
                    continue;
                }
                let start = index;
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
                if start == index {
                    return Err(invalid_json_path(
                        path,
                        start,
                        "array segments require a non-negative index or `*`",
                    ));
                }
                if bytes.get(index).copied() != Some(b']') {
                    return Err(invalid_json_path(
                        path,
                        index.min(bytes.len().saturating_sub(1)),
                        "array index segments must end with `]`",
                    ));
                }
                let parsed = path[start..index].parse::<usize>().map_err(|error| {
                    invalid_json_path(path, start, &format!("array index is too large: {error}"))
                })?;
                index += 1;
                segments.push(JsonPathSegment::Index(parsed));
            }
            _ => {
                return Err(invalid_json_path(
                    path,
                    index,
                    "expected `.` or `[` after the current segment",
                ));
            }
        }
    }
    Ok(segments)
}

fn invalid_json_path(path: &str, byte_index: usize, message: &str) -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(format!(
        "invalid json path `{path}` at byte {byte_index}: {message}"
    ))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(AppRuntimeError::InvalidRequest(
            "hex input must contain an even number of digits".to_string(),
        ));
    }
    bytes
        .chunks_exact(2)
        .map(|digits| {
            let high = hex_nibble(digits[0]);
            let low = hex_nibble(digits[1]);
            match (high, low) {
                (Some(high), Some(low)) => Ok((high << 4) | low),
                _ => Err(AppRuntimeError::InvalidRequest(
                    "hex input contains a non-hexadecimal digit".to_string(),
                )),
            }
        })
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn carrier_base64url_decode(value: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(value))
        .map_err(|_| {
            AppRuntimeError::InvalidRequest(
                "base64url value must use the URL-safe alphabet with optional padding".to_string(),
            )
        })
}

fn carrier_url_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~') {
            output.push(byte as char);
        } else {
            output.push('%');
            output.push(uppercase_hex(byte >> 4));
            output.push(uppercase_hex(byte & 0x0f));
        }
    }
    output
}

fn uppercase_hex(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        _ => (b'A' + value - 10) as char,
    }
}

fn carrier_url_parse(input: &str) -> Result<Value> {
    match Url::parse(input) {
        Ok(url) => Ok(serde_json::json!({
            "scheme": url.scheme(),
            "host": url.host_str(),
            "port": url.port().map(i64::from),
            "path": url.path(),
            "query": url.query(),
            "fragment": url.fragment(),
            "is_absolute": true,
            "normalized": url.to_string(),
        })),
        Err(url::ParseError::RelativeUrlWithoutBase) => carrier_relative_url(input),
        Err(error) => Err(AppRuntimeError::InvalidRequest(format!(
            "invalid URL: {error}"
        ))),
    }
}

fn carrier_relative_url(input: &str) -> Result<Value> {
    let (without_fragment, fragment) = split_once(input, '#');
    let (location, query) = split_once(without_fragment, '?');
    let (host, port, path, normalized) = if location.starts_with("//") {
        let parsed = Url::parse(&format!("https:{input}"))
            .map_err(|error| AppRuntimeError::InvalidRequest(format!("invalid URL: {error}")))?;
        let host = parsed.host_str().map(str::to_string);
        let port = parsed.port().map(i64::from);
        let path = parsed.path().to_string();
        let normalized = normalize_relative_url(
            host.clone(),
            port,
            path.clone(),
            parsed.query().map(str::to_string),
            parsed.fragment().map(str::to_string),
        );
        (host, port, path, normalized)
    } else {
        let path = location.to_string();
        let normalized = normalize_relative_url(
            None,
            None,
            path.clone(),
            query.map(str::to_string),
            fragment.map(str::to_string),
        );
        (None, None, path, normalized)
    };
    Ok(serde_json::json!({
        "scheme": Value::Null,
        "host": host,
        "port": port,
        "path": path,
        "query": query,
        "fragment": fragment,
        "is_absolute": false,
        "normalized": normalized,
    }))
}

fn split_once(value: &str, delimiter: char) -> (&str, Option<&str>) {
    value
        .split_once(delimiter)
        .map_or((value, None), |(left, right)| (left, Some(right)))
}

fn normalize_relative_url(
    host: Option<String>,
    port: Option<i64>,
    path: String,
    query: Option<String>,
    fragment: Option<String>,
) -> String {
    let mut output = String::new();
    if let Some(host) = host {
        output.push_str("//");
        if host.contains(':') && !host.starts_with('[') {
            output.push('[');
            output.push_str(&host);
            output.push(']');
        } else {
            output.push_str(&host);
        }
        if let Some(port) = port {
            output.push(':');
            output.push_str(&port.to_string());
        }
    }
    output.push_str(&path);
    if let Some(query) = query {
        output.push('?');
        output.push_str(&query);
    }
    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(&fragment);
    }
    output
}

fn percent_decode(value: &str) -> Result<String> {
    let mut output = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let digits = std::str::from_utf8(&bytes[index + 1..index + 3])
                    .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?;
                output.push(
                    u8::from_str_radix(digits, 16)
                        .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?,
                );
                index += 3;
            }
            b'%' => {
                return Err(AppRuntimeError::InvalidRequest(
                    "URL escape is truncated".to_string(),
                ));
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(output).map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))
}

fn boolean(value: Value, label: &str) -> Result<bool> {
    value.as_bool().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("BicDB application {label} must be a boolean"))
    })
}

fn string(value: Value, label: &str) -> Result<String> {
    value.as_str().map(str::to_string).ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("BicDB application {label} must be a string"))
    })
}

fn numeric(value: Value, label: &str) -> Result<f64> {
    value.as_f64().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("BicDB application {label} must be numeric"))
    })
}

fn integer(value: Value, label: &str) -> Result<i64> {
    value.as_i64().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("BicDB application {label} must be an integer"))
    })
}

fn checked_integer_binary(operator: &str, left: Value, right: Value) -> Result<Value> {
    let left = integer(left, &format!("{operator} operand"))?;
    let right = integer(right, &format!("{operator} operand"))?;
    let value = match operator {
        "add" => left.checked_add(right).ok_or_else(int_overflow)?,
        "subtract" => left.checked_sub(right).ok_or_else(int_overflow)?,
        "multiply" => left.checked_mul(right).ok_or_else(int_overflow)?,
        "divide" if right == 0 => {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application Int division by zero is not allowed".to_string(),
            ));
        }
        "divide" => left.checked_div(right).ok_or_else(int_overflow)?,
        _ => {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application Int expression uses non-arithmetic operator `{operator}`"
            )));
        }
    };
    Ok(Value::Number(Number::from(value)))
}

fn int_overflow() -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(
        "BicDB application Int arithmetic overflowed the i64 range".to_string(),
    )
}

fn decimal(value: Value, label: &str) -> Result<Decimal> {
    // Integer literals are exact and coerce losslessly: the compiler types
    // `gross < 0` as a Decimal comparison, and refusing the 0 forced every
    // author to write finance.decimal("0") around plain constants (the same
    // defect fixed on the Rust target as BicDB application Issue 4). Floats stay
    // refused — binary floating point never becomes money.
    if let Some(value) = value.as_i64() {
        return Ok(Decimal::from(value));
    }
    let value = value.as_str().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application {label} must be an exact decimal string; got {value}"
        ))
    })?;
    value.parse::<Decimal>().map_err(|error| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application {label} is not a valid Decimal: {error}"
        ))
    })
}

fn decimal_value(value: Decimal) -> Value {
    Value::String(value.to_string())
}

fn checked_decimal_binary(operator: &str, left: Value, right: Value) -> Result<Value> {
    let left = decimal(left, &format!("{operator} operand"))?;
    let right = decimal(right, &format!("{operator} operand"))?;
    let value = match operator {
        "add" => left.checked_add(right).ok_or_else(decimal_overflow)?,
        "subtract" => left.checked_sub(right).ok_or_else(decimal_overflow)?,
        "multiply" => left.checked_mul(right).ok_or_else(decimal_overflow)?,
        "divide" if right == Decimal::ZERO => {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application Decimal division by zero is not allowed".to_string(),
            ));
        }
        "divide" => left.checked_div(right).ok_or_else(decimal_overflow)?,
        _ => {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application Decimal expression uses non-arithmetic operator `{operator}`"
            )));
        }
    };
    Ok(decimal_value(value))
}

fn decimal_compare(
    left: Value,
    right: Value,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Result<Value> {
    if left.is_null() || right.is_null() {
        // Same null semantics as `compare`: ordered comparison against a null
        // optional is false on every BicDB application backend.
        return Ok(Value::Bool(false));
    }
    let left = decimal(left, "comparison operand")?;
    let right = decimal(right, "comparison operand")?;
    Ok(Value::Bool(predicate(left.cmp(&right))))
}

fn decimal_overflow() -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(
        "BicDB application Decimal arithmetic overflowed the supported range".to_string(),
    )
}

fn sort_carrier_array(
    values: &mut [Value],
    item_type: Option<ApplicationExpressionTypeV1>,
) -> Result<()> {
    let mut error = None;
    values.sort_by(
        |left, right| match carrier_scalar_order(left, right, item_type) {
            Ok(ordering) => ordering,
            Err(current) => {
                if error.is_none() {
                    error = Some(current);
                }
                std::cmp::Ordering::Equal
            }
        },
    );
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn carrier_scalar_order(
    left: &Value,
    right: &Value,
    value_type: Option<ApplicationExpressionTypeV1>,
) -> Result<std::cmp::Ordering> {
    match value_type {
        Some(ApplicationExpressionTypeV1::Int) => {
            Ok(integer(left.clone(), "sort item")?.cmp(&integer(right.clone(), "sort item")?))
        }
        Some(ApplicationExpressionTypeV1::Float) => Ok(
            numeric(left.clone(), "sort item")?.total_cmp(&numeric(right.clone(), "sort item")?)
        ),
        Some(ApplicationExpressionTypeV1::Decimal) => {
            Ok(decimal(left.clone(), "sort item")?.cmp(&decimal(right.clone(), "sort item")?))
        }
        Some(ApplicationExpressionTypeV1::Bool) => {
            Ok(boolean(left.clone(), "sort item")?.cmp(&boolean(right.clone(), "sort item")?))
        }
        Some(ApplicationExpressionTypeV1::String) => {
            Ok(string(left.clone(), "sort item")?.cmp(&string(right.clone(), "sort item")?))
        }
        Some(_) | None => Ok(display(left).cmp(&display(right))),
    }
}

fn carrier_math(
    target: &str,
    values: &[Value],
    result_type: Option<ApplicationExpressionTypeV1>,
    argument_types: &[ApplicationExpressionTypeV1],
) -> Result<Value> {
    let first = || argument(values, 0, target);
    let second = || argument(values, 1, target);
    let numeric_type = result_type.or_else(|| argument_types.first().copied());
    match target {
        "math.min" | "math.max" => match numeric_type {
            Some(ApplicationExpressionTypeV1::Int) => {
                let left = integer(first()?, target)?;
                let right = integer(second()?, target)?;
                Ok(Value::Number(Number::from(if target == "math.min" {
                    left.min(right)
                } else {
                    left.max(right)
                })))
            }
            Some(ApplicationExpressionTypeV1::Decimal) => {
                let left = decimal(first()?, target)?;
                let right = decimal(second()?, target)?;
                Ok(decimal_value(if target == "math.min" {
                    left.min(right)
                } else {
                    left.max(right)
                }))
            }
            _ => {
                let left = numeric(first()?, target)?;
                let right = numeric(second()?, target)?;
                number(if target == "math.min" {
                    left.min(right)
                } else {
                    left.max(right)
                })
            }
        },
        "math.abs" => match numeric_type {
            Some(ApplicationExpressionTypeV1::Int) => integer(first()?, target)?
                .checked_abs()
                .map(|value| Value::Number(Number::from(value)))
                .ok_or_else(math_overflow),
            Some(ApplicationExpressionTypeV1::Decimal) => {
                let value = decimal(first()?, target)?;
                if value.is_sign_negative() {
                    Decimal::ZERO
                        .checked_sub(value)
                        .map(decimal_value)
                        .ok_or_else(math_overflow)
                } else {
                    Ok(decimal_value(value))
                }
            }
            _ => number(numeric(first()?, target)?.abs()),
        },
        "math.floor" | "math.ceil" | "math.round" => match numeric_type {
            Some(ApplicationExpressionTypeV1::Int) => {
                Ok(Value::Number(Number::from(integer(first()?, target)?)))
            }
            Some(ApplicationExpressionTypeV1::Decimal) => {
                let value = decimal(first()?, target)?;
                Ok(decimal_value(match target {
                    "math.floor" => value.floor(),
                    "math.ceil" => value.ceil(),
                    _ => value.round_dp_with_strategy(
                        0,
                        rust_decimal::RoundingStrategy::MidpointNearestEven,
                    ),
                }))
            }
            _ => {
                let value = numeric(first()?, target)?;
                number(match target {
                    "math.floor" => value.floor(),
                    "math.ceil" => value.ceil(),
                    _ => value.round(),
                })
            }
        },
        "math.pow" if numeric_type == Some(ApplicationExpressionTypeV1::Int) => {
            let base = integer(first()?, target)?;
            let exponent = u32::try_from(integer(second()?, target)?).map_err(|_| {
                AppRuntimeError::InvalidRequest(
                    "math.pow Int exponent must be non-negative and fit u32".to_string(),
                )
            })?;
            base.checked_pow(exponent)
                .map(|value| Value::Number(Number::from(value)))
                .ok_or_else(math_overflow)
        }
        "math.pow" => number(numeric(first()?, target)?.powf(numeric(second()?, target)?)),
        "math.sqrt" => {
            let value = numeric(first()?, target)?;
            if value < 0.0 {
                return Err(invalid_math_domain(
                    "math.sqrt expects a non-negative value",
                ));
            }
            number(value.sqrt())
        }
        "math.log" => {
            let value = numeric(first()?, target)?;
            if value <= 0.0 {
                return Err(invalid_math_domain("math.log expects value > 0"));
            }
            match values.get(1) {
                Some(base) => {
                    let base = numeric(base.clone(), target)?;
                    if base <= 0.0 || base == 1.0 {
                        return Err(invalid_math_domain(
                            "math.log expects base > 0 with base != 1",
                        ));
                    }
                    number(value.log(base))
                }
                None => number(value.ln()),
            }
        }
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application math builtin `{target}`"
        ))),
    }
}

fn carrier_finance(target: &str, values: &[Value]) -> Result<Value> {
    let first = || argument(values, 0, target);
    let second = || argument(values, 1, target);
    let third = || argument(values, 2, target);
    match target {
        "finance.decimal" => Ok(decimal_value(decimal(first()?, target)?)),
        "finance.money" => Ok(carrier_money_value(
            decimal(first()?, target)?,
            string(second()?, target)?,
        )),
        "finance.add_money" | "finance.subtract_money" => {
            let (left_amount, currency) = carrier_money(first()?, target)?;
            let (right_amount, right_currency) = carrier_money(second()?, target)?;
            if currency != right_currency {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "currency mismatch: {currency} vs {right_currency}"
                )));
            }
            let amount = if target == "finance.add_money" {
                left_amount.checked_add(right_amount)
            } else {
                left_amount.checked_sub(right_amount)
            }
            .ok_or_else(decimal_overflow)?;
            Ok(carrier_money_value(amount, currency))
        }
        "finance.round_money" => {
            let (amount, currency) = carrier_money(first()?, target)?;
            Ok(carrier_money_value(
                finance_round(amount, finance_scale(second()?, target)?),
                currency,
            ))
        }
        "finance.round" => Ok(decimal_value(finance_round(
            decimal(first()?, target)?,
            finance_scale(second()?, target)?,
        ))),
        "finance.abs" => {
            let value = decimal(first()?, target)?;
            let value = if value.is_sign_negative() {
                Decimal::ZERO
                    .checked_sub(value)
                    .ok_or_else(decimal_overflow)?
            } else {
                value
            };
            Ok(decimal_value(value))
        }
        "finance.tax_exclusive" | "finance.tax_inclusive" | "finance.withholding" => {
            Ok(decimal_value(finance_tax(
                target,
                decimal(first()?, target)?,
                decimal(second()?, target)?,
                finance_scale(third()?, target)?,
            )?))
        }
        "finance.tax_exclusive_money"
        | "finance.tax_inclusive_money"
        | "finance.withholding_money" => {
            let (amount, currency) = carrier_money(first()?, target)?;
            let decimal_target = target.strip_suffix("_money").unwrap_or(target);
            Ok(carrier_money_value(
                finance_tax(
                    decimal_target,
                    amount,
                    decimal(second()?, target)?,
                    finance_scale(third()?, target)?,
                )?,
                currency,
            ))
        }
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application finance builtin `{target}`"
        ))),
    }
}

fn carrier_money(value: Value, label: &str) -> Result<(Decimal, String)> {
    let value = object(value, label)?;
    let amount = value.get("amount").cloned().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application {label} Money requires `amount`"
        ))
    })?;
    let currency = value.get("currency").cloned().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application {label} Money requires `currency`"
        ))
    })?;
    Ok((decimal(amount, label)?, string(currency, label)?))
}

fn carrier_money_value(amount: Decimal, currency: String) -> Value {
    serde_json::json!({"amount": decimal_value(amount), "currency": currency})
}

fn finance_scale(value: Value, label: &str) -> Result<u32> {
    u32::try_from(integer(value, label)?)
        .map_err(|_| AppRuntimeError::InvalidRequest(format!("{label} scale must be non-negative")))
}

fn finance_round(value: Decimal, scale: u32) -> Decimal {
    value.round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::MidpointNearestEven)
}

fn finance_tax(target: &str, amount: Decimal, rate: Decimal, scale: u32) -> Result<Decimal> {
    if rate.is_sign_negative() {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "{target} rate must be non-negative"
        )));
    }
    let value = match target {
        "finance.tax_exclusive" | "finance.withholding" => {
            amount.checked_mul(rate).ok_or_else(decimal_overflow)?
        }
        "finance.tax_inclusive" => {
            let divisor = Decimal::ONE
                .checked_add(rate)
                .ok_or_else(decimal_overflow)?;
            let exclusive = amount.checked_div(divisor).ok_or_else(decimal_overflow)?;
            amount.checked_sub(exclusive).ok_or_else(decimal_overflow)?
        }
        _ => {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "unknown BicDB application tax builtin `{target}`"
            )));
        }
    };
    Ok(finance_round(value, scale))
}

fn carrier_address(target: &str, values: &[Value]) -> Result<Value> {
    match target {
        "address.postal_valid" => {
            let postal_code = string(argument(values, 0, target)?, target)?;
            let country = normalize_country_code(&string(argument(values, 1, target)?, target)?)?;
            Ok(Value::Bool(address_postal_valid(&postal_code, &country)?))
        }
        "address.normalize" => {
            let country = normalize_country_code(&string(argument(values, 3, target)?, target)?)?;
            let line1 = normalize_address_text(&string(argument(values, 0, target)?, target)?);
            let city = normalize_address_text(&string(argument(values, 1, target)?, target)?);
            let postal_code =
                normalize_postal_code(&string(argument(values, 2, target)?, target)?, &country);
            let line2 = values
                .get(4)
                .cloned()
                .map(|value| string(value, target))
                .transpose()?
                .map(|value| normalize_address_text(&value))
                .filter(|value| !value.is_empty());
            let region = values
                .get(5)
                .cloned()
                .map(|value| string(value, target))
                .transpose()?
                .map(|value| normalize_address_text(&value).to_ascii_uppercase())
                .filter(|value| !value.is_empty());
            let postal_valid = address_postal_valid(&postal_code, &country)?;
            let mut formatted = vec![line1.clone()];
            if let Some(line2) = &line2 {
                formatted.push(line2.clone());
            }
            let mut locality = city.clone();
            if let Some(region) = &region {
                locality.push_str(", ");
                locality.push_str(region);
            }
            if !postal_code.is_empty() {
                locality.push(' ');
                locality.push_str(&postal_code);
            }
            formatted.push(locality.trim().to_string());
            formatted.push(country.clone());
            Ok(serde_json::json!({
                "line1": line1,
                "line2": line2,
                "city": city,
                "region": region,
                "postal_code": postal_code,
                "country": country,
                "postal_valid": postal_valid,
                "formatted": formatted.join(", "),
            }))
        }
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application address builtin `{target}`"
        ))),
    }
}

fn normalize_country_code(country: &str) -> Result<String> {
    let country = country.trim().to_ascii_uppercase();
    if country.len() == 2 && country.chars().all(|value| value.is_ascii_alphabetic()) {
        Ok(country)
    } else {
        Err(AppRuntimeError::InvalidRequest(format!(
            "country must be a two-letter ISO 3166-1 code, found `{country}`"
        )))
    }
}

fn normalize_address_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_postal_code(postal_code: &str, country: &str) -> String {
    let compact = postal_code
        .trim()
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();
    if !compact.is_ascii() {
        return compact;
    }
    match country {
        "CA" if compact.len() == 6 => format!("{} {}", &compact[..3], &compact[3..]),
        "GB" if compact.len() > 3 => format!(
            "{} {}",
            &compact[..compact.len() - 3],
            &compact[compact.len() - 3..]
        ),
        "US" if compact.len() == 9 && compact.chars().all(|value| value.is_ascii_digit()) => {
            format!("{}-{}", &compact[..5], &compact[5..])
        }
        _ => compact,
    }
}

fn address_postal_valid(postal_code: &str, country: &str) -> Result<bool> {
    let postal_code = normalize_postal_code(postal_code, country);
    let pattern = match country {
        "US" => r"^\d{5}(-\d{4})?$",
        "CA" => r"^[A-Z]\d[A-Z] \d[A-Z]\d$",
        "GB" => r"^[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}$",
        "FR" | "DE" => r"^\d{5}$",
        _ => {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "unsupported country `{country}`; supported countries are US, CA, GB, FR, and DE"
            )));
        }
    };
    Ok(Regex::new(pattern)
        .expect("BicDB application postal validation regex is static")
        .is_match(&postal_code))
}

fn carrier_phone(target: &str, values: &[Value]) -> Result<Value> {
    let input = string(argument(values, 0, target)?, target)?;
    let region = values
        .get(1)
        .cloned()
        .map(|value| string(value, target))
        .transpose()?
        .map(|value| normalize_country_code(&value))
        .transpose()?
        .map(|value| {
            value.parse::<country::Id>().map_err(|_| {
                AppRuntimeError::InvalidRequest(format!("unsupported phone region `{value}`"))
            })
        })
        .transpose()?;
    let number = phonenumber::parse(region, &input).map_err(|error| {
        AppRuntimeError::InvalidRequest(format!("could not parse phone number `{input}`: {error}"))
    })?;
    if target == "phone.is_valid" {
        return Ok(Value::Bool(number.is_valid()));
    }
    if target != "phone.parse" && !number.is_valid() {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "phone number `{input}` is not valid for the inferred region"
        )));
    }
    match target {
        "phone.parse" => Ok(serde_json::json!({
            "country_code": i64::from(number.country().code()),
            "national_number": number.national().to_string(),
            "e164": number.format().mode(Mode::E164).to_string(),
            "international": number.format().mode(Mode::International).to_string(),
            "national": number.format().mode(Mode::National).to_string(),
            "region": number.country().id().map(|id| id.as_ref().to_string()),
            "valid": number.is_valid(),
        })),
        "phone.e164" => Ok(Value::String(number.format().mode(Mode::E164).to_string())),
        "phone.format_national" => Ok(Value::String(
            number.format().mode(Mode::National).to_string(),
        )),
        "phone.format_international" => Ok(Value::String(
            number.format().mode(Mode::International).to_string(),
        )),
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application phone builtin `{target}`"
        ))),
    }
}

#[derive(Clone, Copy)]
enum LocaleDateOrder {
    MonthDayYear,
    DayMonthYear,
}

#[derive(Clone, Copy)]
enum LocaleCurrencyLayout {
    Prefix,
    Suffix,
}

#[derive(Clone, Copy)]
struct LocaleSpec {
    decimal_separator: char,
    grouping_separator: char,
    date_separator: char,
    date_order: LocaleDateOrder,
    currency_layout: LocaleCurrencyLayout,
    currency_space: bool,
}

fn carrier_locale(
    target: &str,
    values: &[Value],
    argument_types: &[ApplicationExpressionTypeV1],
) -> Result<Value> {
    match target {
        "locale.number_format" => {
            let locale = string(argument(values, 1, target)?, target)?;
            let raw = locale_numeric_raw(
                argument(values, 0, target)?,
                argument_types.first().copied(),
                target,
            )?;
            Ok(Value::String(format_grouped_number(
                &raw,
                locale_spec(&locale)?,
                0,
                if argument_types.first() == Some(&ApplicationExpressionTypeV1::Int) {
                    0
                } else {
                    6
                },
            )?))
        }
        "locale.currency_format" => {
            let raw = locale_numeric_raw(
                argument(values, 0, target)?,
                argument_types.first().copied(),
                target,
            )?;
            let currency = string(argument(values, 1, target)?, target)?;
            let locale = string(argument(values, 2, target)?, target)?;
            Ok(Value::String(format_currency_value(
                &raw, &currency, &locale,
            )?))
        }
        "locale.date_format" => {
            let locale = string(argument(values, 1, target)?, target)?;
            let style = values
                .get(2)
                .cloned()
                .map(|value| string(value, target))
                .transpose()?
                .unwrap_or_else(|| "medium".to_string());
            Ok(Value::String(format_locale_date(
                argument(values, 0, target)?,
                argument_types.first().copied(),
                &locale,
                &style,
            )?))
        }
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application locale builtin `{target}`"
        ))),
    }
}

fn carrier_template(target: &str, values: &[Value]) -> Result<Value> {
    let template = string(argument(values, 0, target)?, target)?;
    let context = argument(values, 1, target)?;
    let mut registry = Handlebars::new();
    registry.set_strict_mode(true);
    if target == "template.render_text" {
        registry.register_escape_fn(handlebars::no_escape);
    }
    registry
        .register_template_string("__carrier", template)
        .map_err(|error| AppRuntimeError::InvalidRequest(format!("invalid template: {error}")))?;
    registry
        .render("__carrier", &context)
        .map(Value::String)
        .map_err(|error| {
            AppRuntimeError::InvalidRequest(format!("template render failed: {error}"))
        })
}

fn locale_numeric_raw(
    value: Value,
    value_type: Option<ApplicationExpressionTypeV1>,
    label: &str,
) -> Result<String> {
    match value_type {
        Some(ApplicationExpressionTypeV1::Int) => Ok(integer(value, label)?.to_string()),
        Some(ApplicationExpressionTypeV1::Decimal) => {
            Ok(decimal(value, label)?.normalize().to_string())
        }
        _ => Ok(format!("{:.6}", numeric(value, label)?)),
    }
}

fn normalize_locale(locale: &str) -> String {
    let trimmed = locale.trim().replace('_', "-");
    let mut parts = trimmed.split('-');
    let language = parts.next().unwrap_or_default().to_ascii_lowercase();
    let region = parts.next().unwrap_or_default().to_ascii_uppercase();
    if language.is_empty() || region.is_empty() {
        trimmed
    } else {
        format!("{language}-{region}")
    }
}

fn locale_spec(locale: &str) -> Result<LocaleSpec> {
    match normalize_locale(locale).as_str() {
        "en-US" => Ok(LocaleSpec {
            decimal_separator: '.',
            grouping_separator: ',',
            date_separator: '/',
            date_order: LocaleDateOrder::MonthDayYear,
            currency_layout: LocaleCurrencyLayout::Prefix,
            currency_space: false,
        }),
        "en-GB" | "en-CA" => Ok(LocaleSpec {
            decimal_separator: '.',
            grouping_separator: ',',
            date_separator: '/',
            date_order: LocaleDateOrder::DayMonthYear,
            currency_layout: LocaleCurrencyLayout::Prefix,
            currency_space: false,
        }),
        "fr-FR" => Ok(LocaleSpec {
            decimal_separator: ',',
            grouping_separator: ' ',
            date_separator: '/',
            date_order: LocaleDateOrder::DayMonthYear,
            currency_layout: LocaleCurrencyLayout::Suffix,
            currency_space: true,
        }),
        "de-DE" => Ok(LocaleSpec {
            decimal_separator: ',',
            grouping_separator: '.',
            date_separator: '.',
            date_order: LocaleDateOrder::DayMonthYear,
            currency_layout: LocaleCurrencyLayout::Suffix,
            currency_space: true,
        }),
        other => Err(AppRuntimeError::InvalidRequest(format!(
            "unsupported locale `{other}`; supported locales are en-US, en-GB, en-CA, fr-FR, and de-DE"
        ))),
    }
}

fn format_grouped_number(
    raw: &str,
    spec: LocaleSpec,
    minimum_fraction_digits: usize,
    maximum_fraction_digits: usize,
) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppRuntimeError::InvalidRequest(
            "cannot format an empty numeric value".to_string(),
        ));
    }
    let (negative, digits) = if let Some(value) = trimmed.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = trimmed.strip_prefix('+') {
        (false, value)
    } else {
        (false, trimmed)
    };
    let normalized = if digits.contains('e') || digits.contains('E') {
        format!(
            "{:.*}",
            maximum_fraction_digits.max(6),
            digits.parse::<f64>().map_err(|_| {
                AppRuntimeError::InvalidRequest(format!("could not format numeric value `{raw}`"))
            })?
        )
    } else {
        digits.to_string()
    };
    let mut parts = normalized.split('.');
    let integer = parts.next().unwrap_or_default();
    let mut fraction = parts.next().unwrap_or_default().to_string();
    while fraction.len() > maximum_fraction_digits && fraction.ends_with('0') {
        fraction.pop();
    }
    while fraction.len() > minimum_fraction_digits && fraction.ends_with('0') {
        fraction.pop();
    }
    while fraction.len() < minimum_fraction_digits {
        fraction.push('0');
    }
    let grouped_integer = group_ascii_digits(integer, spec.grouping_separator)?;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    output.push_str(&grouped_integer);
    if !fraction.is_empty() {
        output.push(spec.decimal_separator);
        output.push_str(&fraction);
    }
    Ok(output)
}

fn group_ascii_digits(integer: &str, separator: char) -> Result<String> {
    if integer.is_empty() || !integer.chars().all(|value| value.is_ascii_digit()) {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "could not format numeric value `{integer}`"
        )));
    }
    let mut output = String::with_capacity(integer.len() + integer.len() / 3);
    for (index, value) in integer.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            output.push(separator);
        }
        output.push(value);
    }
    Ok(output.chars().rev().collect())
}

fn currency_symbol(currency: &str) -> Option<&'static str> {
    match currency {
        "USD" => Some("$"),
        "CAD" => Some("CA$"),
        "GBP" => Some("£"),
        "EUR" => Some("€"),
        _ => None,
    }
}

fn currency_fraction_digits(currency: &str) -> usize {
    if currency == "JPY" {
        0
    } else {
        2
    }
}

fn format_currency_value(raw: &str, currency: &str, locale: &str) -> Result<String> {
    let spec = locale_spec(locale)?;
    let currency = currency.trim().to_ascii_uppercase();
    if currency.len() != 3 || !currency.chars().all(|value| value.is_ascii_uppercase()) {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "currency must be a three-letter ISO 4217 code, found `{currency}`"
        )));
    }
    let digits = currency_fraction_digits(&currency);
    let number = format_grouped_number(raw, spec, digits, digits)?;
    let symbol = currency_symbol(&currency).unwrap_or(currency.as_str());
    Ok(match spec.currency_layout {
        LocaleCurrencyLayout::Prefix => format!("{symbol}{number}"),
        LocaleCurrencyLayout::Suffix if spec.currency_space => format!("{number} {symbol}"),
        LocaleCurrencyLayout::Suffix => format!("{number}{symbol}"),
    })
}

fn format_locale_date(
    value: Value,
    value_type: Option<ApplicationExpressionTypeV1>,
    locale: &str,
    style: &str,
) -> Result<String> {
    let spec = locale_spec(locale)?;
    let style = normalize_date_style(style)?;
    match value_type {
        Some(ApplicationExpressionTypeV1::Date) => {
            let date = parse_date(&string(value, "locale.date_format")?)?;
            Ok(format_local_date(date, spec))
        }
        Some(ApplicationExpressionTypeV1::Timestamp) => {
            let value = DateTime::parse_from_rfc3339(&string(value, "locale.date_format")?)
                .map_err(|error| {
                    AppRuntimeError::InvalidRequest(format!("invalid timestamp: {error}"))
                })?
                .with_timezone(&Utc);
            let mut output = format_local_date(value.date_naive(), spec);
            append_time_portion(
                &mut output,
                value.hour(),
                value.minute(),
                value.second(),
                style,
            );
            if style == "long" {
                output.push_str(" UTC");
            }
            Ok(output)
        }
        Some(ApplicationExpressionTypeV1::LocalDateTime) => {
            let value = parse_local_date_time(&string(value, "locale.date_format")?)?;
            let mut output = format_local_date(value.date(), spec);
            append_time_portion(
                &mut output,
                value.hour(),
                value.minute(),
                value.second(),
                style,
            );
            Ok(output)
        }
        Some(ApplicationExpressionTypeV1::ZonedDateTime) => {
            let value = object(value, "locale.date_format")?;
            let date = parse_date(&string(
                value.get("date").cloned().unwrap_or(Value::Null),
                "locale.date_format date",
            )?)?;
            let local = parse_local_date_time(&string(
                value.get("local").cloned().unwrap_or(Value::Null),
                "locale.date_format local",
            )?)?;
            let timezone = string(
                value.get("timezone").cloned().unwrap_or(Value::Null),
                "locale.date_format timezone",
            )?;
            let offset = integer(
                value.get("offset_seconds").cloned().unwrap_or(Value::Null),
                "locale.date_format offset_seconds",
            )?;
            let mut output = format_local_date(date, spec);
            append_time_portion(
                &mut output,
                local.hour(),
                local.minute(),
                local.second(),
                style,
            );
            if style == "long" {
                output.push_str(&format!(
                    " {timezone} {:+03}:{:02}",
                    offset / 3600,
                    (offset.abs() % 3600) / 60
                ));
            }
            Ok(output)
        }
        _ => Err(AppRuntimeError::InvalidRequest(
            "locale.date_format requires a signed Date, Time, LocalDateTime, or ZonedDateTime type"
                .to_string(),
        )),
    }
}

fn parse_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| AppRuntimeError::InvalidRequest(format!("invalid date: {error}")))
}

fn parse_local_date_time(value: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").map_err(|error| {
        AppRuntimeError::InvalidRequest(format!("invalid local datetime: {error}"))
    })
}

fn normalize_date_style(style: &str) -> Result<&'static str> {
    match style.trim().to_ascii_lowercase().as_str() {
        "short" => Ok("short"),
        "medium" | "" => Ok("medium"),
        "long" => Ok("long"),
        other => Err(AppRuntimeError::InvalidRequest(format!(
            "unsupported locale.date_format style `{other}`; use short, medium, or long"
        ))),
    }
}

fn format_local_date(date: NaiveDate, spec: LocaleSpec) -> String {
    match spec.date_order {
        LocaleDateOrder::MonthDayYear => format!(
            "{:02}{}{:02}{}{:04}",
            date.month(),
            spec.date_separator,
            date.day(),
            spec.date_separator,
            date.year()
        ),
        LocaleDateOrder::DayMonthYear => format!(
            "{:02}{}{:02}{}{:04}",
            date.day(),
            spec.date_separator,
            date.month(),
            spec.date_separator,
            date.year()
        ),
    }
}

fn append_time_portion(output: &mut String, hour: u32, minute: u32, second: u32, style: &str) {
    if style == "short" {
        return;
    }
    output.push_str(&format!(" {hour:02}:{minute:02}"));
    if style == "long" || second != 0 {
        output.push_str(&format!(":{second:02}"));
    }
}

fn math_overflow() -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(
        "BicDB application math operation overflowed its result type".to_string(),
    )
}

fn invalid_math_domain(message: &str) -> AppRuntimeError {
    AppRuntimeError::InvalidRequest(message.to_string())
}

fn number(value: f64) -> Result<Value> {
    Number::from_f64(value).map(Value::Number).ok_or_else(|| {
        AppRuntimeError::InvalidRequest(
            "BicDB application arithmetic produced a non-finite number".to_string(),
        )
    })
}

fn display(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn compare(
    left: Value,
    right: Value,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Result<Value> {
    // An ordered comparison against null is false, matching the Node target's
    // semantics (`null > 0` is false in JS) that BicDB application behavior code has
    // been written and tested against on every other backend.
    if left.is_null() || right.is_null() {
        return Ok(Value::Bool(false));
    }
    let ordering = match (&left, &right) {
        (Value::Number(_), Value::Number(_)) => numeric(left, "comparison")?
            .partial_cmp(&numeric(right, "comparison")?)
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest("BicDB application comparison is unordered".to_string())
            })?,
        (Value::String(left), Value::String(right)) => left.cmp(right),
        // A Decimal travels as its exact string while Int literals arrive as
        // numbers, so `quantity > 0`-shaped comparisons mix the two. Compare
        // through Decimal — never through f64, which would round money.
        (Value::String(text), Value::Number(_)) => {
            let left = text.parse::<Decimal>().map_err(|_| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application ordered comparison requires matching numbers or strings; got {left} and {right}"
                ))
            })?;
            left.cmp(&decimal(right.clone(), "comparison operand")?)
        }
        (Value::Number(_), Value::String(text)) => {
            let right = text.parse::<Decimal>().map_err(|_| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application ordered comparison requires matching numbers or strings; got {left} and {right}"
                ))
            })?;
            decimal(left.clone(), "comparison operand")?.cmp(&right)
        }
        _ => {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application ordered comparison requires matching numbers or strings; got {left} and {right}"
            )))
        }
    };
    Ok(Value::Bool(predicate(ordering)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_extension::abi_v2::{ApplicationCallableV1, ApplicationExpressionV1 as Expr};
    use serde_json::json;

    #[derive(Default)]
    struct RecordingHost {
        arguments: Vec<(Option<String>, Value)>,
    }

    struct ExpiredHost;

    impl ApplicationProgramHost for ExpiredHost {
        fn call(
            &mut self,
            _kind: ApplicationCallKindV1,
            _target: &str,
            _method: Option<&str>,
            _arguments: Vec<(Option<String>, Value)>,
        ) -> Result<Value> {
            unreachable!("expired pure program must stop before a host effect")
        }

        fn check_deadline(&self) -> Result<()> {
            Err(AppRuntimeError::Timeout("expired".to_string()))
        }
    }

    #[test]
    fn trusted_deadline_interrupts_pure_program_steps() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 10,
            "max_call_depth": 16,
            "callables": {"pure": {
                "parameters": [],
                "body": [{"op": "return", "value": {"op": "literal", "value": 7}}]
            }}
        }))
        .unwrap();
        let error = execute_application_program(
            &program,
            "pure",
            BTreeMap::new(),
            Vec::new(),
            &mut ExpiredHost,
        )
        .unwrap_err();
        assert!(matches!(error, AppRuntimeError::Timeout(_)));
    }

    impl ApplicationProgramHost for RecordingHost {
        fn call(
            &mut self,
            _kind: ApplicationCallKindV1,
            _target: &str,
            _method: Option<&str>,
            arguments: Vec<(Option<String>, Value)>,
        ) -> Result<Value> {
            self.arguments = arguments;
            Ok(Value::Null)
        }
    }

    #[derive(Default)]
    struct ProjectionHost {
        clinic_reads: usize,
    }

    impl ApplicationProgramHost for ProjectionHost {
        fn call(
            &mut self,
            kind: ApplicationCallKindV1,
            target: &str,
            method: Option<&str>,
            arguments: Vec<(Option<String>, Value)>,
        ) -> Result<Value> {
            match (kind, target, method) {
                (ApplicationCallKindV1::Model, "Doctor", Some("list")) => Ok(json!({
                    "items": [
                        {"id": "doctor-1", "full_name": "Ada Lovelace", "clinic_id": "clinic-1"},
                        {"id": "doctor-2", "full_name": "Grace Hopper", "clinic_id": "clinic-1"},
                    ],
                    "page_info": {"page": 1, "per_page": 20, "total": 2},
                })),
                (ApplicationCallKindV1::Model, "Clinic", Some("get")) => {
                    assert_eq!(arguments, vec![(Some("id".to_string()), json!("clinic-1"))]);
                    self.clinic_reads += 1;
                    Ok(json!({"id": "clinic-1", "city": "London"}))
                }
                (ApplicationCallKindV1::Builtin, "sql.one_as", None) => Ok(json!({
                    "id": "vault-1",
                    "value": "cipher:secret",
                    "label": "one",
                })),
                (ApplicationCallKindV1::Builtin, "sql.list_as", None) => Ok(json!([
                    {"id": "vault-1", "value": "cipher:first", "label": "one"},
                    {"id": "vault-2", "value": "cipher:second", "label": "two"},
                ])),
                (ApplicationCallKindV1::Builtin, "decrypt", None) => {
                    assert_eq!(arguments[0].1, json!("Vault.value"));
                    Ok(json!(arguments[1]
                        .1
                        .as_str()
                        .expect("ciphertext")
                        .strip_prefix("cipher:")
                        .expect("ciphertext envelope")))
                }
                _ => Err(AppRuntimeError::CapabilityDenied(format!(
                    "unexpected projection host call {target}"
                ))),
            }
        }
    }

    #[derive(Default)]
    struct MemoizeHost {
        cache: BTreeMap<String, Value>,
        callback_calls: usize,
        cache_sets: usize,
    }

    impl ApplicationProgramHost for MemoizeHost {
        fn call(
            &mut self,
            _kind: ApplicationCallKindV1,
            target: &str,
            _method: Option<&str>,
            arguments: Vec<(Option<String>, Value)>,
        ) -> Result<Value> {
            match target {
                "cache.get_as" => {
                    let key = arguments[1].1.as_str().unwrap();
                    self.cache
                        .get(key)
                        .cloned()
                        .ok_or_else(|| AppRuntimeError::NotFound("cache miss".to_string()))
                }
                "cache.set" => {
                    self.cache.insert(
                        arguments[0].1.as_str().unwrap().to_string(),
                        arguments[1].1.clone(),
                    );
                    self.cache_sets += 1;
                    Ok(Value::Null)
                }
                "host.memoized_callback" => {
                    self.callback_calls += 1;
                    Ok(Value::from(self.callback_calls as u64))
                }
                _ => Err(AppRuntimeError::CapabilityDenied(target.to_string())),
            }
        }
    }

    #[derive(Default)]
    struct ResilienceHost {
        flaky_calls: usize,
        circuit_calls: usize,
        timeout_entries: usize,
        timeout_exits: usize,
        sleeps: Vec<StdDuration>,
    }

    impl ApplicationProgramHost for ResilienceHost {
        fn call(
            &mut self,
            _kind: ApplicationCallKindV1,
            target: &str,
            _method: Option<&str>,
            _arguments: Vec<(Option<String>, Value)>,
        ) -> Result<Value> {
            match target {
                "host.flaky" => {
                    self.flaky_calls += 1;
                    if self.flaky_calls < 3 {
                        Err(AppRuntimeError::Provider("transient".to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "host.circuit" => {
                    self.circuit_calls += 1;
                    if self.circuit_calls <= 2 {
                        Err(AppRuntimeError::Provider("unavailable".to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "host.slow" => {
                    std::thread::sleep(StdDuration::from_millis(5));
                    Ok(Value::Null)
                }
                _ => Ok(Value::Null),
            }
        }

        fn enter_timeout(&mut self, _timeout_ms: u64) -> Result<()> {
            self.timeout_entries += 1;
            Ok(())
        }

        fn exit_timeout(&mut self) -> Result<()> {
            self.timeout_exits += 1;
            Ok(())
        }

        fn sleep(&mut self, duration: StdDuration) -> Result<()> {
            self.sleeps.push(duration);
            Ok(())
        }
    }

    fn evaluate_builtin(target: &str, values: Vec<Value>) -> Result<Value> {
        evaluate_typed_builtin(target, values, None, &[], &[])
    }

    fn evaluate_typed_builtin(
        target: &str,
        values: Vec<Value>,
        result_type: Option<ApplicationExpressionTypeV1>,
        argument_types: &[ApplicationExpressionTypeV1],
        argument_item_types: &[Option<ApplicationExpressionTypeV1>],
    ) -> Result<Value> {
        evaluate_typed_builtin_arguments(
            target,
            values.into_iter().map(|value| (None, value)).collect(),
            result_type,
            argument_types,
            argument_item_types,
        )
    }

    fn evaluate_typed_builtin_arguments(
        target: &str,
        arguments: Vec<(Option<String>, Value)>,
        result_type: Option<ApplicationExpressionTypeV1>,
        argument_types: &[ApplicationExpressionTypeV1],
        argument_item_types: &[Option<ApplicationExpressionTypeV1>],
    ) -> Result<Value> {
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::new(),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let mut host = DenyApplicationProgramHost;
        Evaluator {
            program: &program,
            host: &mut host,
            globals: BTreeMap::new(),
            steps: 0,
            depth: 0,
            max_call_depth: 16,
            deadlines: Vec::new(),
        }
        .builtin(
            target,
            arguments,
            result_type,
            argument_types,
            argument_item_types,
        )
    }

    #[test]
    fn malformed_non_ascii_hex_is_rejected_without_panicking() {
        let error = hex_decode("é").expect_err("non-ASCII is not hexadecimal");
        assert!(error.to_string().contains("non-hexadecimal"));
    }

    #[test]
    fn pure_collection_string_and_json_helpers_match_carrier_boundaries() {
        assert_eq!(
            evaluate_builtin("string.normalize", vec![json!("  HéLLo\tWORLD  ")]).unwrap(),
            json!("héllo world")
        );
        assert_eq!(
            evaluate_builtin("string.split", vec![json!("a😀"), json!("")]).unwrap(),
            json!(["a", "😀"])
        );
        assert_eq!(
            evaluate_builtin("string.truncate", vec![json!("abc"), json!(-1)]).unwrap(),
            json!("")
        );
        assert_eq!(
            evaluate_builtin("array.slice", vec![json!([0, 1, 2, 3]), json!(1), json!(2)]).unwrap(),
            json!([1, 2])
        );
        let model_page = json!({
            "items": [{"id": "line-1"}, {"id": "line-2"}],
            "page_info": {"page": 1, "per_page": 50, "total": 2}
        });
        assert_eq!(
            evaluate_builtin("array.length", vec![model_page.clone()]).unwrap(),
            json!(2)
        );
        assert_eq!(
            evaluate_builtin("array.first", vec![model_page]).unwrap(),
            json!({"id": "line-1"})
        );
        assert_eq!(
            evaluate_carrier_expression(
                None,
                &ApplicationExpressionV1::Index {
                    target: Box::new(ApplicationExpressionV1::Literal {
                        value: json!({
                            "items": [{"id": "line-1"}, {"id": "line-2"}],
                            "page_info": {"page": 1, "per_page": 50, "total": 2}
                        }),
                    }),
                    index: Box::new(ApplicationExpressionV1::Literal { value: json!(0) }),
                },
                BTreeMap::new(),
            )
            .unwrap(),
            json!({"id": "line-1"})
        );
        assert_eq!(
            evaluate_builtin("array.chunk", vec![json!([1, 2]), json!(0)]).unwrap(),
            json!([])
        );
        assert_eq!(
            evaluate_builtin(
                "map.from",
                vec![json!([{"key": "a", "value": 1}, {"key": "b", "value": 2}])]
            )
            .unwrap(),
            json!([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
        );
        let map = json!([{"key": 7, "value": "seven"}]);
        assert_eq!(
            evaluate_builtin("map.get", vec![map.clone(), json!(7)]).unwrap(),
            json!("seven")
        );
        assert_eq!(
            evaluate_builtin("map.contains", vec![map, json!(8)]).unwrap(),
            json!(false)
        );

        let document = json!({"items": [{"name": "Ada"}, {"name": "Lin"}]});
        assert_eq!(
            evaluate_builtin(
                "json.path",
                vec![document.clone(), json!("$.items[*].name")]
            )
            .unwrap(),
            json!(["Ada", "Lin"])
        );
        assert_eq!(
            evaluate_builtin(
                "json.path_first",
                vec![document.clone(), json!("$.items[1].name")]
            )
            .unwrap(),
            json!("Lin")
        );
        assert_eq!(
            evaluate_builtin("json.exists", vec![document, json!("$.missing")]).unwrap(),
            json!(false)
        );
        assert!(
            evaluate_builtin("json.path", vec![json!({}), json!("items")])
                .unwrap_err()
                .to_string()
                .contains("must start with `$`")
        );

        let relative =
            evaluate_builtin("url.parse", vec![json!("/patients?q=Ada+Lovelace#top")]).unwrap();
        assert_eq!(relative["scheme"], Value::Null);
        assert_eq!(relative["path"], json!("/patients"));
        assert_eq!(relative["is_absolute"], json!(false));
        assert_eq!(
            evaluate_builtin("url.query_get", vec![relative.clone(), json!("q")]).unwrap(),
            json!("Ada Lovelace")
        );
        assert_eq!(
            evaluate_builtin("url.normalize", vec![relative]).unwrap(),
            json!("/patients?q=Ada+Lovelace#top")
        );
        let network =
            evaluate_builtin("url.parse", vec![json!("//Example.COM:8080/a?x=1")]).unwrap();
        assert_eq!(network["host"], json!("example.com"));
        assert_eq!(network["port"], json!(8080));
        assert_eq!(
            evaluate_builtin("url.normalize", vec![network]).unwrap(),
            json!("//example.com:8080/a?x=1")
        );
        assert_eq!(
            evaluate_builtin("url.encode", vec![json!("a+b c/😀")]).unwrap(),
            json!("a%2Bb%20c%2F%F0%9F%98%80")
        );
        assert_eq!(
            evaluate_builtin("url.decode", vec![json!("a+b%20c%2F%F0%9F%98%80")]).unwrap(),
            json!("a+b c/😀")
        );
        assert_eq!(
            evaluate_builtin("base64url.decode", vec![json!("TQ==")]).unwrap(),
            json!("M")
        );
        assert!(evaluate_builtin("url.decode", vec![json!("%XZ")])
            .unwrap_err()
            .to_string()
            .contains("invalid digit"));
        assert_eq!(
            evaluate_builtin("db.has_capability", vec![json!("graph")]).unwrap(),
            json!(false)
        );
    }

    #[test]
    fn carrier_int_arithmetic_preserves_i64_and_rejects_overflow() {
        assert_eq!(
            checked_integer_binary("add", serde_json::json!(4), serde_json::json!(1)).unwrap(),
            serde_json::json!(5)
        );
        assert!(
            checked_integer_binary("add", serde_json::json!(i64::MAX), serde_json::json!(1))
                .unwrap_err()
                .to_string()
                .contains("overflowed")
        );
        assert!(checked_integer_binary(
            "divide",
            serde_json::json!(i64::MIN),
            serde_json::json!(-1)
        )
        .unwrap_err()
        .to_string()
        .contains("overflowed"));
        assert!(
            checked_integer_binary("divide", serde_json::json!(1), serde_json::json!(0))
                .unwrap_err()
                .to_string()
                .contains("division by zero")
        );
    }

    #[test]
    fn carrier_decimal_arithmetic_and_comparison_are_exact() {
        assert_eq!(
            checked_decimal_binary("add", json!("1.20"), json!("2.3")).unwrap(),
            json!("3.50")
        );
        assert_eq!(
            checked_decimal_binary("divide", json!("1"), json!("4")).unwrap(),
            json!("0.25")
        );
        assert_eq!(
            decimal_compare(json!("10"), json!("2"), |ordering| ordering.is_gt()).unwrap(),
            Value::Bool(true)
        );
        assert!(checked_decimal_binary("divide", json!("1"), json!("0"))
            .unwrap_err()
            .to_string()
            .contains("division by zero"));
        assert!(
            checked_decimal_binary("add", json!(Decimal::MAX.to_string()), json!("1"))
                .unwrap_err()
                .to_string()
                .contains("overflowed")
        );
    }

    #[test]
    fn typed_math_and_sort_preserve_carrier_scalar_types() {
        assert_eq!(
            evaluate_typed_builtin(
                "finance.decimal",
                vec![json!("10.50")],
                Some(ApplicationExpressionTypeV1::Decimal),
                &[ApplicationExpressionTypeV1::String],
                &[],
            )
            .unwrap(),
            json!("10.50")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "math.pow",
                vec![json!(2), json!(10)],
                Some(ApplicationExpressionTypeV1::Int),
                &[
                    ApplicationExpressionTypeV1::Int,
                    ApplicationExpressionTypeV1::Int
                ],
                &[],
            )
            .unwrap(),
            json!(1024)
        );
        assert!(evaluate_typed_builtin(
            "math.pow",
            vec![json!(2), json!(-1)],
            Some(ApplicationExpressionTypeV1::Int),
            &[
                ApplicationExpressionTypeV1::Int,
                ApplicationExpressionTypeV1::Int
            ],
            &[],
        )
        .unwrap_err()
        .to_string()
        .contains("non-negative"));
        assert_eq!(
            evaluate_typed_builtin(
                "math.round",
                vec![json!("2.5")],
                Some(ApplicationExpressionTypeV1::Decimal),
                &[ApplicationExpressionTypeV1::Decimal],
                &[],
            )
            .unwrap(),
            json!("2")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "array.sort",
                vec![json!([10, 2, -1])],
                None,
                &[ApplicationExpressionTypeV1::Other],
                &[Some(ApplicationExpressionTypeV1::Int)],
            )
            .unwrap(),
            json!([-1, 2, 10])
        );
        assert_eq!(
            evaluate_typed_builtin(
                "array.sort",
                vec![json!(["10", "2", "-1"])],
                None,
                &[ApplicationExpressionTypeV1::Other],
                &[Some(ApplicationExpressionTypeV1::Decimal)],
            )
            .unwrap(),
            json!(["-1", "2", "10"])
        );
        assert!(evaluate_typed_builtin(
            "math.sqrt",
            vec![json!(-1)],
            Some(ApplicationExpressionTypeV1::Float),
            &[ApplicationExpressionTypeV1::Int],
            &[],
        )
        .unwrap_err()
        .to_string()
        .contains("non-negative"));
    }

    #[test]
    fn finance_builtins_preserve_decimal_and_money_contracts() {
        let money = evaluate_builtin("finance.money", vec![json!("12.345"), json!("USD")]).unwrap();
        assert_eq!(
            evaluate_builtin("finance.round_money", vec![money.clone(), json!(2)]).unwrap(),
            json!({"amount": "12.34", "currency": "USD"})
        );
        assert_eq!(
            evaluate_builtin(
                "finance.add_money",
                vec![money, json!({"amount": "1.005", "currency": "USD"})],
            )
            .unwrap(),
            json!({"amount": "13.350", "currency": "USD"})
        );
        assert_eq!(
            evaluate_builtin(
                "finance.tax_exclusive",
                vec![json!("19.99"), json!("0.0825"), json!(2)],
            )
            .unwrap(),
            json!("1.65")
        );
        assert_eq!(
            evaluate_builtin(
                "finance.tax_inclusive",
                vec![json!("107.00"), json!("0.07"), json!(2)],
            )
            .unwrap(),
            json!("7.00")
        );
        assert_eq!(
            evaluate_builtin(
                "finance.withholding_money",
                vec![
                    json!({"amount": "1500.00", "currency": "CAD"}),
                    json!("0.24"),
                    json!(2),
                ],
            )
            .unwrap(),
            json!({"amount": "360.00", "currency": "CAD"})
        );
        assert_eq!(
            evaluate_builtin("finance.abs", vec![json!("-7.50")]).unwrap(),
            json!("7.50")
        );

        assert!(evaluate_builtin(
            "finance.add_money",
            vec![
                json!({"amount": "1", "currency": "USD"}),
                json!({"amount": "1", "currency": "EUR"}),
            ],
        )
        .unwrap_err()
        .to_string()
        .contains("currency mismatch"));
        assert!(evaluate_builtin(
            "finance.tax_exclusive",
            vec![json!("10"), json!("-0.01"), json!(2)],
        )
        .unwrap_err()
        .to_string()
        .contains("non-negative"));
        assert!(
            evaluate_builtin("finance.round", vec![json!("1.25"), json!(-1)])
                .unwrap_err()
                .to_string()
                .contains("scale must be non-negative")
        );
    }

    #[test]
    fn address_builtins_follow_international_postal_contracts() {
        assert_eq!(
            evaluate_builtin(
                "address.normalize",
                vec![
                    json!(" 123   Main St "),
                    json!("Toronto"),
                    json!("m5v3l9"),
                    json!("ca"),
                    json!(" Unit 5 "),
                    json!("on"),
                ],
            )
            .unwrap(),
            json!({
                "line1": "123 Main St",
                "line2": "Unit 5",
                "city": "Toronto",
                "region": "ON",
                "postal_code": "M5V 3L9",
                "country": "CA",
                "postal_valid": true,
                "formatted": "123 Main St, Unit 5, Toronto, ON M5V 3L9, CA",
            })
        );
        assert!(
            evaluate_builtin("address.postal_valid", vec![json!("100-0001"), json!("JP")],)
                .unwrap_err()
                .to_string()
                .contains("unsupported country")
        );
    }

    #[test]
    fn phone_builtins_use_carriers_international_numbering_data() {
        let parsed =
            evaluate_builtin("phone.parse", vec![json!("(415) 555-2671"), json!("US")]).unwrap();
        assert_eq!(parsed["country_code"], json!(1));
        assert_eq!(parsed["national_number"], json!("4155552671"));
        assert_eq!(parsed["e164"], json!("+14155552671"));
        assert_eq!(parsed["region"], json!("US"));
        assert_eq!(parsed["valid"], json!(true));
        assert_eq!(
            evaluate_builtin("phone.e164", vec![json!("+44 20 7946 0958")]).unwrap(),
            json!("+442079460958")
        );
        assert_eq!(
            evaluate_builtin("phone.format_national", vec![json!("+14155552671")],).unwrap(),
            json!("(415) 555-2671")
        );
        assert!(evaluate_builtin(
            "phone.format_international",
            vec![json!("123"), json!("US")],
        )
        .unwrap_err()
        .to_string()
        .contains("not valid"));
        assert!(
            evaluate_builtin("phone.parse", vec![json!("123"), json!("ZZ")])
                .unwrap_err()
                .to_string()
                .contains("unsupported phone region")
        );
    }

    #[test]
    fn locale_builtins_use_signed_numeric_and_temporal_types() {
        assert_eq!(
            evaluate_typed_builtin(
                "locale.number_format",
                vec![json!(1234567), json!("en-US")],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::Int,
                    ApplicationExpressionTypeV1::String
                ],
                &[],
            )
            .unwrap(),
            json!("1,234,567")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "locale.number_format",
                vec![json!("1234567.89"), json!("de_DE")],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::Decimal,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!("1.234.567,89")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "locale.currency_format",
                vec![json!("1234.5"), json!("EUR"), json!("fr-FR")],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::Decimal,
                    ApplicationExpressionTypeV1::String,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!("1 234,50 €")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "locale.date_format",
                vec![json!("2024-12-31"), json!("en-GB"), json!("long")],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::Date,
                    ApplicationExpressionTypeV1::String,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!("31/12/2024")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "locale.date_format",
                vec![json!("2024-12-31T23:59:05Z"), json!("en-US"), json!("long"),],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::Timestamp,
                    ApplicationExpressionTypeV1::String,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!("12/31/2024 23:59:05 UTC")
        );
        assert_eq!(
            evaluate_typed_builtin(
                "locale.date_format",
                vec![
                    json!({
                        "instant": "2024-12-31T23:00:00Z",
                        "local": "2024-12-31T18:00:00",
                        "date": "2024-12-31",
                        "timezone": "America/New_York",
                        "offset_seconds": -18000,
                    }),
                    json!("en-US"),
                    json!("long"),
                ],
                Some(ApplicationExpressionTypeV1::String),
                &[
                    ApplicationExpressionTypeV1::ZonedDateTime,
                    ApplicationExpressionTypeV1::String,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!("12/31/2024 18:00:00 America/New_York -05:00")
        );
        assert!(evaluate_typed_builtin(
            "locale.number_format",
            vec![json!(1), json!("es-ES")],
            Some(ApplicationExpressionTypeV1::String),
            &[
                ApplicationExpressionTypeV1::Int,
                ApplicationExpressionTypeV1::String
            ],
            &[],
        )
        .unwrap_err()
        .to_string()
        .contains("unsupported locale"));
    }

    #[test]
    fn template_builtins_are_strict_and_channel_aware() {
        assert_eq!(
            evaluate_builtin(
                "template.render_text",
                vec![json!("Hello {{name}}"), json!({"name": "<Ada>"})],
            )
            .unwrap(),
            json!("Hello <Ada>")
        );
        assert_eq!(
            evaluate_builtin(
                "template.render_html",
                vec![json!("<p>{{name}}</p>"), json!({"name": "<Ada>"})],
            )
            .unwrap(),
            json!("<p>&lt;Ada&gt;</p>")
        );
        assert!(evaluate_builtin(
            "template.render_text",
            vec![json!("Hello {{missing}}"), json!({"name": "Ada"})],
        )
        .unwrap_err()
        .to_string()
        .contains("template render failed"));
        assert!(evaluate_builtin(
            "template.render_html",
            vec![json!("{{#if name}}"), json!({"name": "Ada"})],
        )
        .unwrap_err()
        .to_string()
        .contains("invalid template"));
    }

    #[test]
    fn temporal_builtins_match_carrier_timezone_and_dst_semantics() {
        assert_eq!(
            evaluate_builtin("time.timezone", vec![json!("America/New_York")]).unwrap(),
            json!("America/New_York")
        );
        assert!(
            evaluate_builtin("time.timezone", vec![json!("Mars/Olympus")])
                .unwrap_err()
                .to_string()
                .contains("valid IANA zone")
        );

        assert_eq!(
            evaluate_typed_builtin_arguments(
                "time.local_datetime",
                vec![
                    (None, json!("2024-11-03")),
                    (Some("hour".to_string()), json!(1)),
                    (Some("minute".to_string()), json!(30)),
                ],
                Some(ApplicationExpressionTypeV1::LocalDateTime),
                &[
                    ApplicationExpressionTypeV1::Date,
                    ApplicationExpressionTypeV1::Int,
                    ApplicationExpressionTypeV1::Int,
                ],
                &[],
            )
            .unwrap(),
            json!("2024-11-03T01:30:00")
        );
        assert!(evaluate_typed_builtin_arguments(
            "time.local_datetime",
            vec![
                (None, json!("2024-11-03")),
                (Some("hour".to_string()), json!(24)),
                (Some("minute".to_string()), json!(0)),
            ],
            Some(ApplicationExpressionTypeV1::LocalDateTime),
            &[],
            &[],
        )
        .unwrap_err()
        .to_string()
        .contains("hour must be in 0..23"));

        let zoned = evaluate_typed_builtin(
            "time.zoned",
            vec![json!("2024-07-01T12:00:00Z"), json!("America/New_York")],
            Some(ApplicationExpressionTypeV1::ZonedDateTime),
            &[
                ApplicationExpressionTypeV1::Timestamp,
                ApplicationExpressionTypeV1::TimeZone,
            ],
            &[],
        )
        .unwrap();
        assert_eq!(zoned["local"], json!("2024-07-01T08:00:00"));
        assert_eq!(zoned["offset_seconds"], json!(-14_400));

        let schedule = |ambiguous: &str, gap: &str, local: &str| {
            evaluate_typed_builtin_arguments(
                "time.schedule",
                vec![
                    (None, json!(local)),
                    (None, json!("America/New_York")),
                    (Some("ambiguous".to_string()), json!(ambiguous)),
                    (Some("gap".to_string()), json!(gap)),
                ],
                Some(ApplicationExpressionTypeV1::ZonedDateTime),
                &[
                    ApplicationExpressionTypeV1::LocalDateTime,
                    ApplicationExpressionTypeV1::TimeZone,
                    ApplicationExpressionTypeV1::String,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
        };
        assert_eq!(
            schedule("earlier", "reject", "2024-11-03T01:30:00").unwrap()["instant"],
            json!("2024-11-03T05:30:00Z")
        );
        assert_eq!(
            schedule("later", "reject", "2024-11-03T01:30:00").unwrap()["instant"],
            json!("2024-11-03T06:30:00Z")
        );
        assert!(schedule("reject", "reject", "2024-11-03T01:30:00")
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        let after_gap = schedule("earlier", "next_valid", "2024-03-10T02:30:00").unwrap();
        assert_eq!(after_gap["local"], json!("2024-03-10T03:00:00"));
        assert!(schedule("earlier", "reject", "2024-03-10T02:30:00")
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        assert_eq!(
            evaluate_typed_builtin_arguments(
                "time.add",
                vec![
                    (None, json!("2024-01-31")),
                    (Some("days".to_string()), json!(2)),
                ],
                Some(ApplicationExpressionTypeV1::Date),
                &[
                    ApplicationExpressionTypeV1::Date,
                    ApplicationExpressionTypeV1::Int
                ],
                &[],
            )
            .unwrap(),
            json!("2024-02-02")
        );
        assert_eq!(
            evaluate_typed_builtin_arguments(
                "time.add",
                vec![
                    (None, json!("2024-01-01T00:00:00Z")),
                    (Some("hours".to_string()), json!(25)),
                ],
                Some(ApplicationExpressionTypeV1::Timestamp),
                &[
                    ApplicationExpressionTypeV1::Timestamp,
                    ApplicationExpressionTypeV1::Int
                ],
                &[],
            )
            .unwrap(),
            json!("2024-01-02T01:00:00Z")
        );
        let before_dst = evaluate_typed_builtin(
            "time.zoned",
            vec![json!("2024-03-09T17:00:00Z"), json!("America/New_York")],
            Some(ApplicationExpressionTypeV1::ZonedDateTime),
            &[],
            &[],
        )
        .unwrap();
        let after_dst = evaluate_typed_builtin_arguments(
            "time.add",
            vec![(None, before_dst), (Some("days".to_string()), json!(1))],
            Some(ApplicationExpressionTypeV1::ZonedDateTime),
            &[
                ApplicationExpressionTypeV1::ZonedDateTime,
                ApplicationExpressionTypeV1::Int,
            ],
            &[],
        )
        .unwrap();
        assert_eq!(after_dst["local"], json!("2024-03-10T12:00:00"));
        assert_eq!(after_dst["instant"], json!("2024-03-10T16:00:00Z"));

        assert_eq!(
            evaluate_typed_builtin_arguments(
                "time.diff",
                vec![
                    (None, json!("2024-01-02T01:00:00Z")),
                    (None, json!("2024-01-01T00:00:00Z")),
                    (Some("unit".to_string()), json!("hours")),
                ],
                Some(ApplicationExpressionTypeV1::Int),
                &[
                    ApplicationExpressionTypeV1::Timestamp,
                    ApplicationExpressionTypeV1::Timestamp,
                    ApplicationExpressionTypeV1::String,
                ],
                &[],
            )
            .unwrap(),
            json!(25)
        );
        assert_eq!(
            evaluate_typed_builtin(
                "time.epoch_seconds",
                vec![json!("1970-01-01T00:01:40Z")],
                Some(ApplicationExpressionTypeV1::Int),
                &[ApplicationExpressionTypeV1::Timestamp],
                &[],
            )
            .unwrap(),
            json!(100)
        );
    }

    #[test]
    fn executes_parameters_control_flow_and_local_calls() {
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 1_000,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([
                (
                    "normalize".to_string(),
                    ApplicationCallableV1 {
                        parameters: vec!["value".to_string()],
                        body: vec![ApplicationStatementV1::Return {
                            value: Expr::Call {
                                kind: ApplicationCallKindV1::Builtin,
                                target: "string.lower".to_string(),
                                method: None,
                                result_type: None,
                                argument_types: Vec::new(),
                                argument_item_types: Vec::new(),
                                result_projection: None,
                                arguments: vec![ApplicationArgumentV1 {
                                    name: None,
                                    value: Expr::Call {
                                        kind: ApplicationCallKindV1::Builtin,
                                        target: "string.trim".to_string(),
                                        method: None,
                                        result_type: None,
                                        argument_types: Vec::new(),
                                        argument_item_types: Vec::new(),
                                        result_projection: None,
                                        arguments: vec![ApplicationArgumentV1 {
                                            name: None,
                                            value: Expr::Variable {
                                                name: "value".to_string(),
                                            },
                                        }],
                                    },
                                }],
                            },
                        }],
                    },
                ),
                (
                    "route".to_string(),
                    ApplicationCallableV1 {
                        parameters: vec![],
                        body: vec![ApplicationStatementV1::Return {
                            value: Expr::Call {
                                kind: ApplicationCallKindV1::Function,
                                target: "normalize".to_string(),
                                method: None,
                                result_type: None,
                                argument_types: Vec::new(),
                                argument_item_types: Vec::new(),
                                result_projection: None,
                                arguments: vec![ApplicationArgumentV1 {
                                    name: None,
                                    value: Expr::Field {
                                        target: Box::new(Expr::Variable {
                                            name: "input".to_string(),
                                        }),
                                        field: "name".to_string(),
                                    },
                                }],
                            },
                        }],
                    },
                ),
            ]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let mut host = DenyApplicationProgramHost;
        let result = execute_application_program(
            &program,
            "route",
            BTreeMap::from([("input".to_string(), serde_json::json!({"name": "  ADA "}))]),
            vec![],
            &mut host,
        )
        .unwrap();
        assert_eq!(result, Value::String("ada".to_string()));
    }

    #[test]
    fn memoize_calls_the_callback_only_on_a_cache_miss() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "callables": {
                "computed": {
                    "parameters": ["value"],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "host.memoized_callback",
                            "arguments": [{
                                "value": {"op": "variable", "name": "value"}
                            }]
                        }
                    }]
                },
                "route": {
                    "parameters": [],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "memoize.call",
                            "arguments": [
                                {"value": {"op": "literal", "value": "computed"}},
                                {"value": {"op": "literal", "value": "Ada"}},
                                {
                                    "name": "ttl_seconds",
                                    "value": {"op": "literal", "value": 60}
                                }
                            ]
                        }
                    }]
                }
            }
        }))
        .unwrap();
        let mut host = MemoizeHost::default();
        let first =
            execute_application_program(&program, "route", BTreeMap::new(), Vec::new(), &mut host)
                .unwrap();
        let second =
            execute_application_program(&program, "route", BTreeMap::new(), Vec::new(), &mut host)
                .unwrap();
        assert_eq!(first, json!(1));
        assert_eq!(second, first);
        assert_eq!(host.callback_calls, 1);
        assert_eq!(host.cache_sets, 1);
    }

    #[test]
    fn array_mutation_and_pure_callbacks_match_carrier_collections() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 1_000,
            "callables": {
                "positive": {
                    "parameters": ["item"],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "binary",
                            "operator": "greater",
                            "left": {"op": "variable", "name": "item"},
                            "right": {"op": "literal", "value": 0},
                            "value_type": "bool",
                            "left_type": "int",
                            "right_type": "int"
                        }
                    }]
                },
                "lower": {
                    "parameters": ["item"],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "string.lower",
                            "arguments": [{
                                "value": {"op": "variable", "name": "item"}
                            }]
                        }
                    }]
                },
                "item_key": {
                    "parameters": ["item"],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "field",
                            "target": {"op": "variable", "name": "item"},
                            "field": "key"
                        }
                    }]
                },
                "route": {
                    "parameters": [],
                    "body": [
                        {
                            "op": "let",
                            "name": "items",
                            "value": {
                                "op": "array",
                                "items": [
                                    {"op": "literal", "value": -2},
                                    {"op": "literal", "value": 1},
                                    {"op": "literal", "value": 2}
                                ]
                            }
                        },
                        {
                            "op": "expr",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "array.push",
                                "arguments": [
                                    {"value": {"op": "variable", "name": "items"}},
                                    {"value": {"op": "literal", "value": 3}}
                                ]
                            }
                        },
                        {
                            "op": "let",
                            "name": "partitioned",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "array.partition",
                                "arguments": [
                                    {"value": {"op": "variable", "name": "items"}},
                                    {"value": {"op": "literal", "value": "positive"}}
                                ]
                            }
                        },
                        {
                            "op": "let",
                            "name": "grouped",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "array.group_by",
                                "arguments": [
                                    {"value": {"op": "array", "items": [
                                        {"op": "literal", "value": "A"},
                                        {"op": "literal", "value": "a"},
                                        {"op": "literal", "value": "B"}
                                    ]}},
                                    {"value": {"op": "literal", "value": "lower"}}
                                ]
                            }
                        },
                        {
                            "op": "let",
                            "name": "indexed",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "array.index_by",
                                "arguments": [
                                    {"value": {"op": "array", "items": [
                                        {"op": "object", "fields": {
                                            "key": {"op": "literal", "value": "x"},
                                            "value": {"op": "literal", "value": 1}
                                        }},
                                        {"op": "object", "fields": {
                                            "key": {"op": "literal", "value": "x"},
                                            "value": {"op": "literal", "value": 2}
                                        }}
                                    ]}},
                                    {"value": {"op": "literal", "value": "item_key"}}
                                ]
                            }
                        },
                        {
                            "op": "return",
                            "value": {"op": "object", "fields": {
                                "items": {"op": "variable", "name": "items"},
                                "partitioned": {"op": "variable", "name": "partitioned"},
                                "grouped": {"op": "variable", "name": "grouped"},
                                "indexed": {"op": "variable", "name": "indexed"}
                            }}
                        }
                    ]
                }
            },
            "service_bindings": {},
            "client_bindings": {},
            "secret_bindings": {},
            "event_bindings": {},
            "mutation_bindings": [],
            "workflow_bindings": {}
        }))
        .unwrap();
        let mut host = DenyApplicationProgramHost;
        let result =
            execute_application_program(&program, "route", BTreeMap::new(), Vec::new(), &mut host)
                .unwrap();
        assert_eq!(result["items"], json!([-2, 1, 2, 3]));
        assert_eq!(
            result["partitioned"],
            json!({"matched": [1, 2, 3], "unmatched": [-2]})
        );
        assert_eq!(
            result["grouped"],
            json!([
                {"key": "a", "value": ["A", "a"]},
                {"key": "b", "value": ["B"]}
            ])
        );
        assert_eq!(
            result["indexed"],
            json!([{"key": "x", "value": {"key": "x", "value": 2}}])
        );
    }

    #[test]
    fn resilience_blocks_retry_timeout_and_open_circuits() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 10_000,
            "callables": {
                "retry": {
                    "parameters": [],
                    "body": [
                        {
                            "op": "with_retry",
                            "attempts": {"op": "literal", "value": 3},
                            "backoff_ms": {"op": "literal", "value": 2},
                            "max_backoff_ms": {"op": "literal", "value": 4},
                            "body": [{"op": "expr", "value": {
                                "op": "call", "kind": "builtin", "target": "host.flaky"
                            }}]
                        },
                        {"op": "return", "value": {"op": "literal", "value": "retried"}}
                    ]
                },
                "timeout": {
                    "parameters": [],
                    "body": [{
                        "op": "with_timeout",
                        "timeout_ms": {"op": "literal", "value": 1},
                        "body": [{"op": "expr", "value": {
                            "op": "call", "kind": "builtin", "target": "host.slow"
                        }}]
                    }]
                },
                "circuit": {
                    "parameters": [],
                    "body": [
                        {
                            "op": "circuit_breaker",
                            "name": {"op": "literal", "value": "bicdb-resilience-test"},
                            "failure_threshold": {"op": "literal", "value": 2},
                            "reset_timeout_ms": {"op": "literal", "value": 1},
                            "body": [{"op": "expr", "value": {
                                "op": "call", "kind": "builtin", "target": "host.circuit"
                            }}]
                        },
                        {"op": "return", "value": {"op": "literal", "value": "closed"}}
                    ]
                },
                "invalid_retry": {
                    "parameters": [],
                    "body": [{
                        "op": "with_retry",
                        "attempts": {"op": "literal", "value": 2},
                        "backoff_ms": {"op": "literal", "value": 5},
                        "max_backoff_ms": {"op": "literal", "value": 4},
                        "body": []
                    }]
                }
            },
            "service_bindings": {},
            "client_bindings": {},
            "secret_bindings": {},
            "event_bindings": {},
            "mutation_bindings": [],
            "workflow_bindings": {}
        }))
        .unwrap();
        let mut host = ResilienceHost::default();
        assert_eq!(
            execute_application_program(&program, "retry", BTreeMap::new(), Vec::new(), &mut host,)
                .unwrap(),
            json!("retried")
        );
        assert_eq!(host.flaky_calls, 3);
        assert_eq!(
            host.sleeps,
            vec![StdDuration::from_millis(2), StdDuration::from_millis(4)]
        );

        let timeout = execute_application_program(
            &program,
            "timeout",
            BTreeMap::new(),
            Vec::new(),
            &mut host,
        )
        .unwrap_err();
        assert!(matches!(timeout, AppRuntimeError::ResilienceTimeout(_)));
        assert_eq!(host.timeout_entries, 1);
        assert_eq!(host.timeout_exits, 1);

        for _ in 0..2 {
            assert!(matches!(
                execute_application_program(
                    &program,
                    "circuit",
                    BTreeMap::new(),
                    Vec::new(),
                    &mut host,
                ),
                Err(AppRuntimeError::Provider(_))
            ));
        }
        assert!(matches!(
            execute_application_program(
                &program,
                "circuit",
                BTreeMap::new(),
                Vec::new(),
                &mut host,
            ),
            Err(AppRuntimeError::CircuitOpen(_))
        ));
        assert_eq!(host.circuit_calls, 2);
        std::thread::sleep(StdDuration::from_millis(2));
        assert_eq!(
            execute_application_program(
                &program,
                "circuit",
                BTreeMap::new(),
                Vec::new(),
                &mut host,
            )
            .unwrap(),
            json!("closed")
        );

        assert!(execute_application_program(
            &program,
            "invalid_retry",
            BTreeMap::new(),
            Vec::new(),
            &mut host,
        )
        .unwrap_err()
        .to_string()
        .contains("greater than or equal"));
    }

    #[test]
    fn bounds_infinite_loops() {
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 10,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([(
                "loop".to_string(),
                ApplicationCallableV1 {
                    parameters: vec![],
                    body: vec![ApplicationStatementV1::While {
                        condition: Expr::Literal {
                            value: Value::Bool(true),
                        },
                        body: vec![],
                    }],
                },
            )]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let mut host = DenyApplicationProgramHost;
        assert!(matches!(
            execute_application_program(&program, "loop", BTreeMap::new(), vec![], &mut host),
            Err(AppRuntimeError::ResourceExhausted(_))
        ));
    }

    #[test]
    fn enforces_signed_call_depth_and_restores_depth_after_errors() {
        let recursive_call = Expr::Call {
            kind: ApplicationCallKindV1::Function,
            target: "recursive".to_string(),
            method: None,
            result_type: None,
            argument_types: Vec::new(),
            argument_item_types: Vec::new(),
            result_projection: None,
            arguments: Vec::new(),
        };
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 2,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([
                (
                    "recursive".to_string(),
                    ApplicationCallableV1 {
                        parameters: Vec::new(),
                        body: vec![ApplicationStatementV1::Return {
                            value: recursive_call,
                        }],
                    },
                ),
                (
                    "fails".to_string(),
                    ApplicationCallableV1 {
                        parameters: Vec::new(),
                        body: vec![ApplicationStatementV1::Fail {
                            code: Expr::Literal {
                                value: json!("expected_failure"),
                            },
                            message: Expr::Literal {
                                value: json!("expected"),
                            },
                        }],
                    },
                ),
                (
                    "succeeds".to_string(),
                    ApplicationCallableV1 {
                        parameters: Vec::new(),
                        body: vec![ApplicationStatementV1::Return {
                            value: Expr::Literal { value: json!(7) },
                        }],
                    },
                ),
            ]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let mut host = DenyApplicationProgramHost;
        assert!(matches!(
            execute_application_program(
                &program,
                "recursive",
                BTreeMap::new(),
                Vec::new(),
                &mut host,
            ),
            Err(AppRuntimeError::ResourceExhausted(_))
        ));

        let mut evaluator = Evaluator {
            program: &program,
            host: &mut host,
            globals: BTreeMap::new(),
            steps: 0,
            depth: 0,
            max_call_depth: usize::from(program.max_call_depth),
            deadlines: Vec::new(),
        };
        assert!(matches!(
            evaluator.call("fails", Vec::new()),
            Err(AppRuntimeError::ApplicationFailure { code, message, .. })
                if code == "expected_failure" && message == "expected"
        ));
        assert_eq!(evaluator.depth, 0);
        assert_eq!(evaluator.call("succeeds", Vec::new()).unwrap(), json!(7));
        assert_eq!(evaluator.depth, 0);
    }

    #[test]
    fn applies_signed_paged_model_projection_with_relations_and_computed_fields() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 100,
            "max_call_depth": 16,
            "callables": {
                "route": {
                    "parameters": [],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "model",
                            "target": "Doctor",
                            "method": "list",
                            "result_projection": {
                                "fields": [
                                    {"kind": "field", "name": "id", "field": "id"},
                                    {"kind": "field", "name": "display_name", "field": "full_name"},
                                    {
                                        "kind": "relation",
                                        "name": "clinic_city",
                                        "source_field": "clinic_id",
                                        "target": "Clinic",
                                        "target_key": "id",
                                        "target_field": "city"
                                    },
                                    {
                                        "kind": "computed",
                                        "name": "slug",
                                        "expression": {
                                            "op": "call",
                                            "kind": "builtin",
                                            "target": "string.slug",
                                            "arguments": [{
                                                "value": {
                                                    "op": "field",
                                                    "target": {"op": "variable", "name": "source"},
                                                    "field": "full_name"
                                                }
                                            }]
                                        }
                                    }
                                ]
                            },
                            "arguments": []
                        }
                    }]
                }
            }
        }))
        .unwrap();
        let mut host = ProjectionHost::default();
        let value =
            execute_application_program(&program, "route", BTreeMap::new(), vec![], &mut host)
                .unwrap();
        assert_eq!(
            value,
            json!({
                "items": [
                    {"id": "doctor-1", "display_name": "Ada Lovelace", "clinic_city": "London", "slug": "ada-lovelace"},
                    {"id": "doctor-2", "display_name": "Grace Hopper", "clinic_city": "London", "slug": "grace-hopper"},
                ],
                "page_info": {"page": 1, "per_page": 20, "total": 2},
            })
        );
        assert_eq!(host.clinic_reads, 1);
    }

    #[test]
    fn applies_signed_typed_sql_projection_to_one_and_list_results() {
        let projection = json!({
            "fields": [
                {"kind": "field", "name": "id", "field": "id"},
                {
                    "kind": "computed",
                    "name": "value",
                    "expression": {
                        "op": "call",
                        "kind": "builtin",
                        "target": "decrypt",
                        "arguments": [
                            {"value": {"op": "literal", "value": "Vault.value"}},
                            {
                                "value": {
                                    "op": "field",
                                    "target": {"op": "variable", "name": "source"},
                                    "field": "value"
                                }
                            }
                        ]
                    }
                },
                {"kind": "field", "name": "label", "field": "label"}
            ]
        });
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 100,
            "max_call_depth": 16,
            "callables": {
                "one": {
                    "parameters": [],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "sql.one_as",
                            "result_projection": projection.clone(),
                            "arguments": []
                        }
                    }]
                },
                "list": {
                    "parameters": [],
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "sql.list_as",
                            "result_projection": projection,
                            "arguments": []
                        }
                    }]
                }
            }
        }))
        .unwrap();
        let mut host = ProjectionHost::default();
        assert_eq!(
            execute_application_program(&program, "one", BTreeMap::new(), vec![], &mut host)
                .unwrap(),
            json!({"id": "vault-1", "value": "secret", "label": "one"})
        );
        assert_eq!(
            execute_application_program(&program, "list", BTreeMap::new(), vec![], &mut host)
                .unwrap(),
            json!([
                {"id": "vault-1", "value": "first", "label": "one"},
                {"id": "vault-2", "value": "second", "label": "two"},
            ])
        );
    }

    #[test]
    fn preserves_named_arguments_for_host_builtins() {
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 10,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([(
                "route".to_string(),
                ApplicationCallableV1 {
                    parameters: vec![],
                    body: vec![ApplicationStatementV1::Return {
                        value: Expr::Call {
                            kind: ApplicationCallKindV1::Builtin,
                            target: "host.test".to_string(),
                            method: None,
                            result_type: None,
                            argument_types: Vec::new(),
                            argument_item_types: Vec::new(),
                            result_projection: None,
                            arguments: vec![ApplicationArgumentV1 {
                                name: Some("baggage".to_string()),
                                value: Expr::Literal {
                                    value: serde_json::json!({"source": "test"}),
                                },
                            }],
                        },
                    }],
                },
            )]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let mut host = RecordingHost::default();
        execute_application_program(&program, "route", BTreeMap::new(), vec![], &mut host).unwrap();
        assert_eq!(host.arguments[0].0.as_deref(), Some("baggage"));
        assert_eq!(host.arguments[0].1, serde_json::json!({"source": "test"}));
    }

    #[test]
    fn paged_model_lists_work_with_array_operators_indexes_and_loops() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "max_steps": 100,
            "max_call_depth": 16,
            "callables": {
                "route": {
                    "parameters": [],
                    "body": [
                        {"op": "let", "name": "rows", "value": {"op": "literal", "value": {
                            "items": [{"value": 2}, {"value": 3}],
                            "page_info": {"page": 1, "per_page": 50, "total": 2}
                        }}},
                        {"op": "let", "name": "total", "value": {"op": "literal", "value": 0}},
                        {"op": "for", "name": "row", "iterable": {"op": "variable", "name": "rows"}, "body": [
                            {"op": "assign", "name": "total", "value": {
                                "op": "binary", "operator": "add",
                                "left": {"op": "variable", "name": "total"},
                                "right": {"op": "field", "target": {"op": "variable", "name": "row"}, "field": "value"},
                                "value_type": "int", "left_type": "int", "right_type": "int"
                            }}
                        ]},
                        {"op": "return", "value": {"op": "object", "fields": {
                            "count": {"op": "call", "kind": "builtin", "target": "array.length", "arguments": [
                                {"value": {"op": "variable", "name": "rows"}}
                            ]},
                            "first": {"op": "field", "target": {
                                "op": "index", "target": {"op": "variable", "name": "rows"},
                                "index": {"op": "literal", "value": 0}
                            }, "field": "value"},
                            "total": {"op": "variable", "name": "total"}
                        }}}
                    ]
                }
            }
        }))
        .unwrap();
        let mut host = DenyApplicationProgramHost;
        let result =
            execute_application_program(&program, "route", BTreeMap::new(), Vec::new(), &mut host)
                .unwrap();
        assert_eq!(result, json!({"count": 2, "first": 2, "total": 5}));
    }
}
