//! Bound cumulative owned allocations while executing a join pipeline.
//!
//! This is a cooperative materialization envelope, not an allocator/RSS limit.
//! Counting prior intermediates conservatively even after they are freed also
//! bounds nested pipelines without needing allocation ownership in every RowSet.
use crate::{Result, SqlError, SqlValue};
use std::{cell::Cell, marker::PhantomData, rc::Rc, sync::OnceLock};

thread_local! {
    static RETAINED: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(crate) struct Scope {
    root: bool,
    _thread: PhantomData<Rc<()>>,
}
impl Scope {
    pub(crate) fn enter() -> Self {
        let root = RETAINED.with(|bytes| {
            if bytes.get().is_some() {
                false
            } else {
                bytes.set(Some(0));
                true
            }
        });
        Self {
            root,
            _thread: PhantomData,
        }
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if self.root {
            RETAINED.with(|bytes| bytes.set(None));
        }
    }
}

fn configured_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        match std::env::var("BICDB_MAX_JOIN_MATERIALIZED_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
        {
            Some(0) => usize::MAX,
            Some(limit) => limit,
            None => 512 * 1024 * 1024,
        }
    })
}

pub(crate) fn retain(left: &[SqlValue], right: &[SqlValue]) -> Result<()> {
    let limit = bicdb_core::query_budget::limit(configured_limit());
    if limit == usize::MAX {
        return bicdb_core::query_budget::check().map_err(Into::into);
    }
    // Include outer-vector growth/reallocation and each inline SqlValue slot.
    // Counting all right values overestimates duplicate-column elision safely.
    let mut bytes = 3 * std::mem::size_of::<Vec<SqlValue>>();
    for value in left.iter().chain(right) {
        bytes = bytes
            .saturating_add(std::mem::size_of::<SqlValue>())
            .saturating_add(clone_heap_bytes(value, 0));
    }
    retain_bytes(bytes)
}

/// Charge a known new allocation before reserving its backing storage.
pub(crate) fn retain_bytes(bytes: usize) -> Result<()> {
    let limit = bicdb_core::query_budget::limit(configured_limit());
    let total = RETAINED.with(|retained| {
        let previous = retained.get().ok_or_else(|| {
            SqlError::resource_limit(
                "53200",
                "join materialization has no execution budget scope",
            )
        })?;
        let total = previous.saturating_add(bytes);
        retained.set(Some(total));
        Ok::<_, SqlError>(total)
    })?;
    bicdb_core::query_budget::observe(total);
    if total > limit {
        return Err(SqlError::resource_limit(
            "53200",
            format!(
                "join materialization would allocate {total} bytes; limit is {limit} \
             (reduce the intermediate rows or raise BICDB_MAX_JOIN_MATERIALIZED_BYTES)"
            ),
        ));
    }
    bicdb_core::query_budget::check().map_err(Into::into)
}

/// The batched point join transfers its left row and sometimes its right
/// payload. Charge only new slot storage and payloads actually cloned.
pub(crate) fn retain_extension<'a>(
    row_len: usize,
    capacity: usize,
    additional: usize,
    cloned_values: impl IntoIterator<Item = &'a SqlValue>,
) -> Result<()> {
    let required = row_len.saturating_add(additional);
    let slots = if required > capacity {
        required.saturating_mul(std::mem::size_of::<SqlValue>())
    } else {
        0
    };
    let bytes = cloned_values.into_iter().fold(slots, |n, value| {
        n.saturating_add(clone_heap_bytes(value, 0))
    });
    retain_bytes(bytes)
}

fn clone_heap_bytes(value: &SqlValue, depth: usize) -> usize {
    if depth >= 128 {
        return usize::MAX;
    }
    match value {
        SqlValue::Null | SqlValue::Bool(_) | SqlValue::Int(_) | SqlValue::Float(_)
        // These payloads are shared by Arc; row cloning only copies the slot.
        | SqlValue::Json(_) | SqlValue::OracleLob(_) => 0,
        SqlValue::String(text) => text.len(),
        SqlValue::JsonText(value) => value.raw().len().saturating_add(json_heap(value.parsed(), depth + 1)),
        SqlValue::Geometry(value) => geometry_heap(value, depth + 1),
        SqlValue::TsQuery(value) => value.root.as_ref().map_or(0, |v| query_heap(v, depth + 1)),
        SqlValue::Composite(value) => value.fields.iter().fold(value.type_name.len(), |n, field| {
            n.saturating_add(std::mem::size_of::<crate::SqlCompositeField>())
                .saturating_add(field.name.len()).saturating_add(field.pg_type.len())
                .saturating_add(clone_heap_bytes(&field.value, depth + 1))
        }),
    }
}

fn json_heap(value: &serde_json::Value, depth: usize) -> usize {
    if depth >= 128 {
        return usize::MAX;
    }
    match value {
        // arbitrary_precision stores Number's digit text in an owned String.
        serde_json::Value::Number(number) => number.as_str().len(),
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(values) => values.iter().fold(0usize, |n, v| {
            n.saturating_add(std::mem::size_of::<serde_json::Value>())
                .saturating_add(json_heap(v, depth + 1))
        }),
        serde_json::Value::Object(values) => values.iter().fold(1024usize, |n, (key, v)| {
            n.saturating_add(128)
                .saturating_add(key.len())
                .saturating_add(json_heap(v, depth + 1))
        }),
        _ => 0,
    }
}

fn query_heap(value: &crate::PgTsQueryNode, depth: usize) -> usize {
    use crate::PgTsQueryNode::*;
    if depth >= 128 {
        return usize::MAX;
    }
    let node = std::mem::size_of::<crate::PgTsQueryNode>();
    match value {
        Operand(value) => value.text.len(),
        Not(value) => node.saturating_add(query_heap(value, depth + 1)),
        And(left, right) | Or(left, right) | Phrase { left, right, .. } => node
            .saturating_mul(2)
            .saturating_add(query_heap(left, depth + 1))
            .saturating_add(query_heap(right, depth + 1)),
    }
}

fn geometry_heap(value: &bicdb_core::Geometry, depth: usize) -> usize {
    use bicdb_core::Geometry::*;
    if depth >= 128 {
        return usize::MAX;
    }
    // Geometry clones allocate owned coordinate vectors, unlike shared JSON.
    match value {
        Point(_) | Envelope(_) => 0,
        LineString(line) => line.0.len().saturating_mul(16),
        Polygon(polygon) => polygon.interiors().iter().fold(
            polygon.exterior().0.len().saturating_mul(16),
            |n, ring| {
                n.saturating_add(std::mem::size_of_val(ring))
                    .saturating_add(ring.0.len().saturating_mul(16))
            },
        ),
        MultiPoint(points) => points.0.len().saturating_mul(16),
        MultiLineString(lines) => lines.0.iter().fold(0usize, |n, line| {
            n.saturating_add(std::mem::size_of_val(line))
                .saturating_add(line.0.len().saturating_mul(16))
        }),
        MultiPolygon(polygons) => polygons.0.iter().fold(0usize, |n, polygon| {
            let rings = polygon.interiors().iter().fold(
                polygon.exterior().0.len().saturating_mul(16),
                |n, ring| {
                    n.saturating_add(std::mem::size_of_val(ring))
                        .saturating_add(ring.0.len().saturating_mul(16))
                },
            );
            n.saturating_add(std::mem::size_of_val(polygon))
                .saturating_add(rings)
        }),
        GeometryCollection(values) => values.iter().fold(0usize, |n, value| {
            n.saturating_add(std::mem::size_of_val(value))
                .saturating_add(geometry_heap(value, depth + 1))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_json_text_pays_for_both_owned_copies_but_jsonb_stays_shared() {
        let _scope = Scope::enter();
        let digits = "1".repeat(4096);
        let value = crate::PgJsonText::parse(digits.clone()).unwrap();
        let parsed = value.parsed().clone();
        let _memory = bicdb_core::query_budget::QueryMemoryScope::new(6000);
        // Raw spelling and arbitrary-precision parsed digits each own 4096B.
        assert!(retain(&[SqlValue::JsonText(value)], &[]).is_err());
        // A new execution scope is needed after resource rejection.
        drop(_memory);
        drop(_scope);
        let _scope = Scope::enter();
        let _memory = bicdb_core::query_budget::QueryMemoryScope::new(6000);
        assert!(retain(&[SqlValue::Json(std::sync::Arc::new(parsed))], &[]).is_ok());
    }
}
