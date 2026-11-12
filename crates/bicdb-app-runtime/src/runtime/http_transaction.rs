//! Pick the HTTP idempotency transaction before beginning any database work.
//!
//! The wrapper must supply the authored outer transaction's minimum isolation.
//! Stop at transaction boundaries: promoting an invalid RC -> Serializable
//! nested block would hide a program error. The normal nested guard still owns
//! that rejection. Only signed, statically resolved local callables are followed;
//! this neither invents service authority nor evaluates request data.
use super::*;
use bicdb_extension::abi_v2::ApplicationStatementV1;

pub(super) fn idempotent_callable_isolation(
    program: &ApplicationProgramV1,
    callable: &str,
) -> Result<&'static str> {
    let mut inference = OuterIsolation {
        program,
        visited: BTreeSet::new(),
        minimum: 0,
    };
    inference.callable(callable)?;
    Ok(match inference.minimum {
        0 => "read_committed",
        1 => "repeatable_read",
        _ => "serializable",
    })
}

struct OuterIsolation<'a> {
    program: &'a ApplicationProgramV1,
    visited: BTreeSet<String>,
    minimum: u8,
}

impl OuterIsolation<'_> {
    fn callable(&mut self, name: &str) -> Result<()> {
        if !self.visited.insert(name.to_owned()) {
            return Ok(());
        }
        if let Some(callable) = self.program.callables.get(name) {
            self.block(&callable.body)?;
        }
        Ok(())
    }

    fn block(&mut self, statements: &[ApplicationStatementV1]) -> Result<()> {
        use ApplicationStatementV1::*;
        for statement in statements {
            match statement {
                Transaction { isolation, .. } => {
                    let minimum = match isolation.as_str() {
                        "read_committed" => 0,
                        "repeatable_read" => 1,
                        "serializable" => 2,
                        _ => {
                            return Err(AppRuntimeError::InvalidPackage(format!(
                                "unknown BicDB application transaction isolation `{isolation}`"
                            )))
                        }
                    };
                    self.minimum = self.minimum.max(minimum);
                }
                Let { value, .. }
                | Assign { value, .. }
                | Return { value }
                | Expr { value }
                | Emit { value, .. } => self.expression(value)?,
                If {
                    condition,
                    then_branch,
                    else_branch,
                } => {
                    self.expression(condition)?;
                    self.block(then_branch)?;
                    self.block(else_branch)?;
                }
                For { iterable, body, .. } => {
                    self.expression(iterable)?;
                    self.block(body)?;
                }
                While { condition, body } => {
                    self.expression(condition)?;
                    self.block(body)?;
                }
                Fail { code, message } => {
                    self.expression(code)?;
                    self.expression(message)?;
                }
                WithTimeout { timeout_ms, body } => {
                    self.expression(timeout_ms)?;
                    self.block(body)?;
                }
                WithRetry {
                    attempts,
                    backoff_ms,
                    max_backoff_ms,
                    body,
                } => {
                    self.expression(attempts)?;
                    self.expression(backoff_ms)?;
                    if let Some(value) = max_backoff_ms {
                        self.expression(value)?;
                    }
                    self.block(body)?;
                }
                CircuitBreaker {
                    name,
                    failure_threshold,
                    reset_timeout_ms,
                    body,
                } => {
                    self.expression(name)?;
                    self.expression(failure_threshold)?;
                    self.expression(reset_timeout_ms)?;
                    self.block(body)?;
                }
                Break | Continue => {}
            }
        }
        Ok(())
    }

    fn expression(&mut self, expression: &ApplicationExpressionV1) -> Result<()> {
        use ApplicationExpressionV1::*;
        match expression {
            Variable { .. } | Literal { .. } => {}
            Object { fields } => {
                for value in fields.values() {
                    self.expression(value)?;
                }
            }
            Array { items } => {
                for value in items {
                    self.expression(value)?;
                }
            }
            Field { target, .. } | Unary { value: target, .. } => self.expression(target)?,
            Index { target, index } => {
                self.expression(target)?;
                self.expression(index)?;
            }
            Binary { left, right, .. } => {
                self.expression(left)?;
                self.expression(right)?;
            }
            Match { value, arms } => {
                self.expression(value)?;
                for arm in arms {
                    self.expression(&arm.value)?;
                }
            }
            Call {
                kind,
                target,
                arguments,
                ..
            } => {
                for argument in arguments {
                    self.expression(&argument.value)?;
                }
                if matches!(
                    kind,
                    ApplicationCallKindV1::Function | ApplicationCallKindV1::Action
                ) {
                    self.callable(target)?;
                }
            }
            Exists { condition, .. } | Aggregate { condition, .. } => self.expression(condition)?,
        }
        Ok(())
    }
}
