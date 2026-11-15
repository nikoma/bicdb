//! Passive test-only timing DATA. Production macros evaluate the original
//! operation exactly once, with no tracing, new authority or recovery behavior.
macro_rules! stage {
    ($name:expr, $operation:expr) => {{
        #[cfg(test)]
        {
            let __name = $name;
            let __started = $crate::native_policy_trace::begin(__name);
            let __original = $operation;
            $crate::native_policy_trace::returned(__name, __started, &__original);
            __original
        }
        #[cfg(not(test))]
        {
            $operation
        }
    }};
}
macro_rules! root {
    ($seed:expr, $body:block) => {{
        #[cfg(test)]
        {
            $crate::native_policy_trace::observe_root($seed, || $body)
        }
        #[cfg(not(test))]
        {
            $body
        }
    }};
}
pub(crate) use root;
pub(crate) use stage;

#[cfg(test)]
mod actual {
    use crate::{BicDbError, CancellationToken, Result};
    use std::cell::RefCell;
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    const MAX_EVENTS: usize = 96;
    const ERROR_PREFIX_BYTES: usize = 768;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    thread_local! {static TRACE:RefCell<Option<Trace>>=const {RefCell::new(None)};}

    #[derive(Clone, Copy, Default)]
    pub(crate) struct Clocks {
        pub(crate) authority: &'static str,
        pub(crate) admission: Option<Instant>,
        pub(crate) all_phase: Option<Instant>,
        pub(crate) post_own: Option<Instant>,
        pub(crate) role: Option<Instant>,
        pub(crate) role_source: &'static str,
        pub(crate) role_own: Option<Instant>,
        pub(crate) origin_own: Option<Instant>,
    }
    pub(crate) struct Seed {
        scope: &'static str,
        node: String,
        range: crate::RangeId,
        parent: CancellationToken,
        clocks: Clocks,
    }
    impl Seed {
        pub(crate) fn new(
            scope: &'static str,
            node: String,
            range: crate::RangeId,
            parent: &CancellationToken,
            clocks: Clocks,
        ) -> Self {
            Self {
                scope,
                node,
                range,
                parent: parent.clone(),
                clocks,
            }
        }
    }
    #[derive(Clone, serde::Serialize)]
    pub(crate) struct NativeState {
        pub(crate) stage: &'static str,
        pub(crate) xid: Option<u64>,
        pub(crate) generation: Option<uuid::Uuid>,
        pub(crate) coordinator: uuid::Uuid,
        pub(crate) database_open: String,
    }
    #[derive(serde::Serialize)]
    struct ErrorData {
        discriminant: String,
        io_kind: Option<String>,
        io_os_code: Option<i32>,
        prefix: String,
        prefix_incomplete: bool,
        formatting_complete: bool,
    }
    /// No error formatter is invoked while an original guarded stage runs.
    /// Debug strings are produced only by serialization during root flush.
    struct StageError {
        discriminant: std::mem::Discriminant<BicDbError>,
        io_kind: Option<std::io::ErrorKind>,
        io_os_code: Option<i32>,
    }
    impl serde::Serialize for StageError {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeStruct as _;
            let mut value = serializer.serialize_struct("StageError", 5)?;
            value.serialize_field("discriminant", &format!("{:?}", self.discriminant))?;
            value.serialize_field("io_kind", &self.io_kind.map(|kind| format!("{kind:?}")))?;
            value.serialize_field("io_os_code", &self.io_os_code)?;
            value.serialize_field("display_prefix_available", &false)?;
            value.serialize_field("root_prefix_is_not_independent_stage_cause", &true)?;
            value.end()
        }
    }
    fn stage_error(error: &BicDbError) -> StageError {
        StageError {
            discriminant: std::mem::discriminant(error),
            io_kind: match error {
                BicDbError::Io(io) => Some(io.kind()),
                _ => None,
            },
            io_os_code: match error {
                BicDbError::Io(io) => io.raw_os_error(),
                _ => None,
            },
        }
    }
    fn error_data(error: &BicDbError) -> ErrorData {
        struct Prefix {
            text: String,
            complete: bool,
        }
        impl std::fmt::Write for Prefix {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                if !self.complete {
                    return Ok(());
                }
                let mut end = (ERROR_PREFIX_BYTES - self.text.len()).min(text.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                self.text.push_str(&text[..end]);
                self.complete &= end == text.len();
                Ok(())
            }
        }
        let mut prefix = Prefix {
            text: String::with_capacity(ERROR_PREFIX_BYTES),
            complete: true,
        };
        let formatting_complete = std::fmt::write(&mut prefix, format_args!("{error}")).is_ok();
        ErrorData {
            discriminant: format!("{:?}", std::mem::discriminant(error)),
            io_kind: match error {
                BicDbError::Io(io) => Some(format!("{:?}", io.kind())),
                _ => None,
            },
            io_os_code: match error {
                BicDbError::Io(io) => io.raw_os_error(),
                _ => None,
            },
            prefix: prefix.text,
            prefix_incomplete: !prefix.complete,
            formatting_complete,
        }
    }
    fn remaining(deadline: Option<Instant>, now: Instant) -> Option<i128> {
        deadline.map(|deadline| {
            if deadline >= now {
                deadline
                    .duration_since(now)
                    .as_micros()
                    .min(i128::MAX as u128) as i128
            } else {
                -(now
                    .duration_since(deadline)
                    .as_micros()
                    .min(i128::MAX as u128) as i128)
            }
        })
    }
    #[derive(serde::Serialize)]
    struct Event {
        stage: &'static str,
        point: &'static str,
        elapsed_us: u128,
        stage_elapsed_us: Option<u128>,
        parent_remaining_us: Option<i128>,
        parent_cancelled: bool,
        admission_remaining_us: Option<i128>,
        all_phase_remaining_us: Option<i128>,
        post_own_remaining_us: Option<i128>,
        role_remaining_us: Option<i128>,
        role_own_remaining_us: Option<i128>,
        origin_own_remaining_us: Option<i128>,
        actual_result_ok: Option<bool>,
        error: Option<StageError>,
        native: Option<NativeState>,
        actual_issued_xid: Option<u64>,
    }
    struct Trace {
        id: u64,
        seed: Seed,
        start: Instant,
        events: Vec<Event>,
        omitted: u64,
        last_native: Option<NativeState>,
        root_return: Option<(bool, Option<ErrorData>)>,
        root_operation_returned_us: Option<u128>,
    }
    struct RootGuard {
        previous: Option<Trace>,
    }
    impl Drop for RootGuard {
        fn drop(&mut self) {
            let actual = TRACE.with(|slot| {
                let mut slot = slot.borrow_mut();
                let current = slot.take();
                *slot = self.previous.take();
                current
            });
            if let Some(actual) = actual {
                // Root operation has returned/unwound and its actual guards
                // have dropped. No formatting/JSON/stderr in the version gate.
                let value = serde_json::json!({
                    "scope":actual.seed.scope,"node":actual.seed.node,"range":actual.seed.range,
                    "trace_id":actual.id,"authority":actual.seed.clocks.authority,
                    "role_clock_source":actual.seed.clocks.role_source,
                    "original_parent_deadline":format!("{:?}",actual.seed.parent.deadline()),
                    "root_actual_return":actual.root_return,"root_operation_returned_us":actual.root_operation_returned_us,
                    "trace_lifetime_us":actual.start.elapsed().as_micros(),
                    "events":actual.events,"omitted":actual.omitted,"latest_actual_native_state":actual.last_native,
                    "DATA_only_not_Ready_or_outcome_resolution":true,
                    "stderr_and_kernel_IO_not_hard_preemptible":true,
                });
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "NATIVE_FIRST_POLICY_STAGE_TRACE {}",
                    value
                );
            }
        }
    }
    fn event(
        stage: &'static str,
        point: &'static str,
        span: Option<(Instant, Instant)>,
        result: Option<(bool, Option<StageError>)>,
        native: Option<NativeState>,
        xid: Option<u64>,
    ) {
        TRACE.with(|slot| {
            let mut slot = slot.borrow_mut();
            let Some(trace) = slot.as_mut() else {
                return;
            };
            if let Some(native) = &native {
                trace.last_native = Some(native.clone());
            }
            if trace.events.len() == MAX_EVENTS {
                trace.omitted = trace.omitted.saturating_add(1);
                return;
            }
            // Return clocks describe the actual operation return, before any
            // error-prefix formatting/diagnostic insertion cost.
            let now = span
                .map(|(_, returned)| returned)
                .unwrap_or_else(Instant::now);
            let clocks = trace.seed.clocks;
            let (actual_result_ok, error) = match result {
                Some((ok, error)) => (Some(ok), error),
                None => (None, None),
            };
            trace.events.push(Event {
                stage,
                point,
                elapsed_us: now.duration_since(trace.start).as_micros(),
                stage_elapsed_us: span
                    .map(|(start, returned)| returned.duration_since(start).as_micros()),
                parent_remaining_us: remaining(trace.seed.parent.deadline(), now),
                parent_cancelled: trace.seed.parent.is_cancel_requested(),
                admission_remaining_us: remaining(clocks.admission, now),
                all_phase_remaining_us: remaining(clocks.all_phase, now),
                post_own_remaining_us: remaining(clocks.post_own, now),
                role_remaining_us: remaining(clocks.role, now),
                role_own_remaining_us: remaining(clocks.role_own, now),
                origin_own_remaining_us: remaining(clocks.origin_own, now),
                actual_result_ok,
                error,
                native,
                actual_issued_xid: xid,
            });
        });
    }
    pub(crate) fn observe_root<T>(seed: Seed, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let trace = Trace {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            seed,
            start: Instant::now(),
            events: Vec::with_capacity(MAX_EVENTS),
            omitted: 0,
            last_native: None,
            root_return: None,
            root_operation_returned_us: None,
        };
        let previous = TRACE.with(|slot| slot.replace(Some(trace)));
        let _guard = RootGuard { previous };
        event("root", "entry", None, None, None, None);
        let original = operation();
        let returned = Instant::now();
        let error = original.as_ref().err().map(error_data);
        TRACE.with(|slot| {
            if let Some(trace) = slot.borrow_mut().as_mut() {
                trace.root_return = Some((original.is_ok(), error));
                trace.root_operation_returned_us =
                    Some(returned.duration_since(trace.start).as_micros());
            }
        });
        original
    }
    pub(crate) fn begin(stage: &'static str) -> Option<Instant> {
        if !active() {
            return None;
        }
        let start = Instant::now();
        event(stage, "entry", None, None, None, None);
        Some(start)
    }
    pub(crate) fn returned<T>(stage: &'static str, start: Option<Instant>, original: &Result<T>) {
        let Some(start) = start else {
            return;
        };
        let returned = Instant::now();
        event(
            stage,
            "return",
            Some((start, returned)),
            Some((original.is_ok(), original.as_ref().err().map(stage_error))),
            None,
            None,
        );
    }
    pub(crate) fn active() -> bool {
        TRACE.with(|slot| slot.borrow().is_some())
    }
    pub(crate) fn note(stage: &'static str) {
        event(stage, "point", None, None, None, None);
    }
    pub(crate) fn native(stage: &'static str, native: NativeState) {
        event(stage, "native_state", None, None, Some(native), None);
    }
    pub(crate) fn issued_xid(xid: u64) {
        event(
            "native_real_transaction",
            "actual_issued_xid_before_put",
            None,
            None,
            None,
            Some(xid),
        );
    }
    pub(crate) fn origin_own(deadline: Instant) {
        TRACE.with(|slot| {
            if let Some(trace) = slot.borrow_mut().as_mut() {
                trace.seed.clocks.origin_own = Some(deadline);
            }
        });
        note("actual_fresh_origin_OWN_returned");
    }
}
#[cfg(test)]
pub(crate) use actual::*;
