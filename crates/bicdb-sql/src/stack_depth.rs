//! PostgreSQL's `stack depth limit exceeded` (SQLSTATE 54001) instead of a process abort.
//!
//! Query execution and routine calls recurse: a subquery executes a query, a SQL function body
//! executes a query that calls the function again. Unbounded recursion (a function that calls
//! itself, or any runaway rewrite) used to exhaust the worker thread's stack, and Rust aborts the
//! whole server process on stack overflow, so one statement from any user took every session
//! down.
//!
//! Recursive entry points call [`guarded`]. While the current stack has room it runs the closure
//! directly; near the end it continues on a fresh heap-allocated segment, so legitimately deep
//! queries keep working on small thread stacks. The number of segments a thread may stack up is
//! bounded (`BICDB_MAX_STACK_SEGMENTS`, default 8 segments of 8 MiB); past that the statement fails
//! with 54001 like PostgreSQL does past `max_stack_depth`.
use crate::{Result, SqlError};
use std::cell::Cell;

/// Below this much remaining stack, continue on a new segment. Unoptimized builds have large
/// frames between two guarded entries, so the margin is generous.
const RED_ZONE: usize = 1024 * 1024;
const SEGMENT_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_SEGMENTS: usize = 8;

thread_local! {
    static SEGMENTS: Cell<usize> = const { Cell::new(0) };
}

fn max_segments() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_STACK_SEGMENTS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_SEGMENTS)
    })
}

pub(crate) fn stack_depth_exceeded() -> SqlError {
    SqlError::resource_limit("54001", "stack depth limit exceeded")
}

struct SegmentGuard;

impl Drop for SegmentGuard {
    fn drop(&mut self) {
        SEGMENTS.with(|segments| segments.set(segments.get().saturating_sub(1)));
    }
}

/// Runs `f`, on a new stack segment when the current one is nearly exhausted, or fails with
/// 54001 when the recursion has already used every segment it may.
pub(crate) fn guarded<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    match stacker::remaining_stack() {
        Some(remaining) if remaining < RED_ZONE => {
            let used = SEGMENTS.with(Cell::get);
            if used >= max_segments() {
                return Err(stack_depth_exceeded());
            }
            SEGMENTS.with(|segments| segments.set(used + 1));
            let _guard = SegmentGuard;
            stacker::grow(SEGMENT_SIZE, f)
        }
        _ => f(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recurse(depth: u64) -> Result<u64> {
        guarded(|| {
            // A frame of a few KiB, like the executor's.
            let pad = std::hint::black_box([depth as u8; 4096]);
            if depth == 0 {
                return Ok(u64::from(pad[0]));
            }
            recurse(depth - 1).map(|value| value + 1)
        })
    }

    #[test]
    fn unbounded_recursion_fails_with_54001_instead_of_aborting() {
        let (error, segments_after) = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| (recurse(u64::MAX).unwrap_err(), SEGMENTS.with(Cell::get)))
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(error.sqlstate(), "54001");
        assert_eq!(error.to_string(), "stack depth limit exceeded");
        // The segment count is released again for the next statement.
        assert_eq!(segments_after, 0);
    }

    #[test]
    fn deep_but_bounded_recursion_still_succeeds_on_a_small_stack() {
        let value = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| recurse(2_000))
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(value, 2_000);
    }
}
