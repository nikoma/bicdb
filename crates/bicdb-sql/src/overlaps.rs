//! `(start1, end1) OVERLAPS (start2, end2)` over dates, times, timestamps
//! and timestamptz, with an interval allowed as each pair's second item, and
//! PostgreSQL's NULL rules (`timestamp_overlaps`).

use crate::{PgInterval, PgTime, PgTimestamp, Result, SqlError, SqlValue};

/// A point on a common axis: microseconds (UTC for date-bearing values,
/// since midnight for times).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Point(i128);

#[derive(Clone, Copy)]
enum Kind {
    Dated,
    Time,
}

fn pair(value: &SqlValue) -> Result<(SqlValue, SqlValue, Option<String>)> {
    match value {
        SqlValue::Composite(record) if record.fields.len() == 2 => Ok((
            record.fields[0].value.clone(),
            record.fields[1].value.clone(),
            Some(record.fields[1].pg_type.clone()),
        )),
        _ => Err(SqlError::InvalidSql(
            "OVERLAPS requires two two-item row expressions".to_string(),
        )),
    }
}

fn dated_timestamp(text: &str) -> Option<PgTimestamp> {
    crate::eval::parse_timestamptz(text).ok()
}

fn point(value: &SqlValue) -> Result<Option<(Point, Kind)>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let text = value.to_cell();
    let trimmed = text.trim();
    match trimmed {
        "infinity" => return Ok(Some((Point(i128::MAX), Kind::Dated))),
        "-infinity" => return Ok(Some((Point(i128::MIN), Kind::Dated))),
        _ => {}
    }
    let has_date = trimmed.len() >= 8 && trimmed[1..].contains('-') && !trimmed.contains(" day");
    if has_date {
        if let Some(PgTimestamp::Finite(micros)) = dated_timestamp(trimmed) {
            return Ok(Some((Point(i128::from(micros)), Kind::Dated)));
        }
    }
    if let Ok(time) = PgTime::from_postgres_text(trimmed) {
        return Ok(Some((
            Point(i128::from(time.micros_since_midnight())),
            Kind::Time,
        )));
    }
    Err(SqlError::invalid_datetime_format(format!(
        "invalid input syntax for type timestamp with time zone: \"{text}\""
    )))
}

/// The pair's end: a point, or the start plus an interval.
fn end_point(
    start: Option<(Point, Kind)>,
    start_value: &SqlValue,
    end: &SqlValue,
    end_type: Option<&str>,
) -> Result<Option<Point>> {
    if matches!(end, SqlValue::Null) {
        return Ok(None);
    }
    let text = end.to_cell();
    let is_interval = end_type.is_some_and(|kind| kind == "interval")
        || (matches!(start, Some((_, Kind::Dated)))
            && point(end).is_err()
            && PgInterval::from_postgres_text(&text).is_ok());
    if !is_interval {
        return Ok(point(end)?.map(|(point, _)| point));
    }
    let interval = PgInterval::from_postgres_text(&text).map_err(|_| {
        SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{text}\""
        ))
    })?;
    let Some((start_point, kind)) = start else {
        return Ok(None);
    };
    match kind {
        Kind::Dated => {
            let start_text = start_value.to_cell();
            let base = match start_text.trim() {
                "infinity" => PgTimestamp::PositiveInfinity,
                "-infinity" => PgTimestamp::NegativeInfinity,
                other => dated_timestamp(other).unwrap_or(PgTimestamp::PositiveInfinity),
            };
            let shifted = base
                .checked_add_interval(interval)
                .map_err(|_| SqlError::data_exception("22008", "timestamp out of range", None))?;
            Ok(Some(match shifted {
                PgTimestamp::Finite(micros) => Point(i128::from(micros)),
                PgTimestamp::PositiveInfinity => Point(i128::MAX),
                PgTimestamp::NegativeInfinity => Point(i128::MIN),
            }))
        }
        Kind::Time => {
            let day = 86_400_000_000_i128;
            let shifted = (start_point.0 + i128::from(interval.micros)).rem_euclid(day);
            Ok(Some(Point(shifted)))
        }
    }
}

pub(crate) fn eval_overlaps(left: &SqlValue, right: &SqlValue) -> Result<SqlValue> {
    let (start1, end1, end1_type) = pair(left)?;
    let (start2, end2, end2_type) = pair(right)?;
    let s1 = point(&start1)?;
    let e1 = end_point(s1, &start1, &end1, end1_type.as_deref())?;
    let s2 = point(&start2)?;
    let e2 = end_point(s2, &start2, &end2, end2_type.as_deref())?;
    Ok(
        match overlaps(s1.map(|(p, _)| p), e1, s2.map(|(p, _)| p), e2) {
            Some(value) => SqlValue::Bool(value),
            None => SqlValue::Null,
        },
    )
}

/// PostgreSQL's `timestamp_overlaps` with its NULL handling.
fn overlaps(
    ts1: Option<Point>,
    te1: Option<Point>,
    ts2: Option<Point>,
    te2: Option<Point>,
) -> Option<bool> {
    let normalize = |start: Option<Point>, end: Option<Point>| -> Option<(Point, Option<Point>)> {
        match (start, end) {
            (None, None) => None,
            (None, Some(end)) => Some((end, None)),
            (Some(start), Some(end)) if start > end => Some((end, Some(start))),
            (Some(start), end) => Some((start, end)),
        }
    };
    let (ts1, te1) = normalize(ts1, te1)?;
    let (ts2, te2) = normalize(ts2, te2)?;
    if ts1 > ts2 {
        let te2 = te2?;
        if ts1 < te2 {
            return Some(true);
        }
        te1?;
        Some(false)
    } else if ts1 < ts2 {
        let te1 = te1?;
        if ts2 < te1 {
            return Some(true);
        }
        te2?;
        Some(false)
    } else {
        te1?;
        te2?;
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(value: i128) -> Option<Point> {
        Some(Point(value))
    }

    #[test]
    fn null_rules_follow_postgresql() {
        assert_eq!(overlaps(p(1), p(3), p(2), p(4)), Some(true));
        assert_eq!(overlaps(p(1), p(2), p(2), p(4)), Some(false));
        assert_eq!(overlaps(p(3), p(1), p(2), p(4)), Some(true));
        assert_eq!(overlaps(p(1), None, p(2), p(4)), None);
        assert_eq!(overlaps(p(1), p(3), p(1), None), None);
        assert_eq!(overlaps(p(2), None, p(1), p(4)), Some(true));
        assert_eq!(overlaps(None, None, p(1), p(4)), None);
    }
}

/// `date + timetz` (either order): the timestamptz of that date and time
/// at the time's own offset, as PostgreSQL's `datetimetz_timestamptz`.
pub(crate) fn eval_date_timetz_addition(
    left: &SqlValue,
    op: &sqlparser::ast::BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    if !matches!(op, sqlparser::ast::BinaryOperator::Plus) {
        return Ok(None);
    }
    let (date, time) = match (left_type, right_type) {
        (Some("date"), Some("timetz")) => (left, right),
        (Some("timetz"), Some("date")) => (right, left),
        _ => return Ok(None),
    };
    if matches!(date, SqlValue::Null) || matches!(time, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    date_and_timetz(&date.to_cell(), &time.to_cell()).map(|value| Some(SqlValue::String(value)))
}

/// The timestamptz text of `date` combined with a `timetz` text.
pub(crate) fn date_and_timetz(date: &str, time: &str) -> Result<String> {
    let date = crate::PgDate::from_postgres_text(date)
        .map_err(|error| crate::eval::postgres_date_input_error(date, error))?;
    let time =
        crate::PgTimeTz::from_postgres_text(time, crate::eval::current_timezone_offset_seconds())
            .map_err(|_| {
            SqlError::invalid_datetime_format(format!(
                "invalid input syntax for type time with time zone: \"{time}\""
            ))
        })?;
    let Some(days) = date.epoch_days() else {
        return Ok(date.to_iso_text());
    };
    let local = i64::from(days) * 86_400_000_000 + time.time.micros_since_midnight();
    let utc = local - i64::from(time.utc_offset_seconds) * 1_000_000;
    Ok(crate::eval::render_timestamptz(PgTimestamp::Finite(utc)))
}

/// The timestamptz text of `date` at a local `time` in the session zone.
pub(crate) fn date_and_time(date: &str, time: &str, with_timezone: bool) -> Result<String> {
    let date_value = crate::PgDate::from_postgres_text(date)
        .map_err(|error| crate::eval::postgres_date_input_error(date, error))?;
    let time_value = PgTime::from_postgres_text(time).map_err(|_| {
        SqlError::invalid_datetime_format(format!("invalid input syntax for type time: \"{time}\""))
    })?;
    let Some(days) = date_value.epoch_days() else {
        return Ok(date_value.to_iso_text());
    };
    let local = i64::from(days) * 86_400_000_000 + time_value.micros_since_midnight();
    if !with_timezone {
        return Ok(PgTimestamp::Finite(local).to_iso_text(false));
    }
    let offset = crate::eval::local_timestamp_offset_seconds(
        PgTimestamp::Finite(local),
        &crate::eval::current_timezone_name(),
    )
    .unwrap_or(0);
    Ok(crate::eval::render_timestamptz(PgTimestamp::Finite(
        local - i64::from(offset) * 1_000_000,
    )))
}
