//! Datetime items of the jsonpath `.datetime()` method family: PostgreSQL's
//! `executeDateTimeMethod`, `compareDatetime` and `JsonEncodeDateTime`.
//!
//! The evaluator's items are JSON values, so a datetime item travels as a
//! one-member object under a key no JSON input can contain (jsonb rejects
//! `\u0000`); `JsonPath::query` turns every such item into its ISO string
//! before results leave the evaluator.

use std::cmp::Ordering;

use serde_json::{Map, Value as JsonValue};

use crate::datetime_template::{
    civil_from_days, parse_datetime_std, round_micros_to_precision, ParsedKind,
};
use crate::eval::{
    current_timezone_name, local_timestamp_offset_seconds, timezone_offset_at_timestamp,
    transaction_timestamp_micros,
};
use crate::{PgTimestamp, Result, SqlError, SqlNotice};

const ITEM_KEY: &str = "\u{0}bicdb:jsonpath-datetime";
const DAY: i64 = 86_400_000_000;
const UNIX_TO_PG_DAYS: i64 = 10_957;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

/// `value`: days since 2000-01-01 (date), microseconds of the day (time,
/// local for timetz), local microseconds since 2000-01-01 (timestamp), UTC
/// microseconds since 2000-01-01 (timestamptz). `zone`: seconds east of UTC
/// (timetz, and the display zone of a timestamptz).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Item {
    kind: Kind,
    value: i64,
    zone: i32,
}

/// The method being applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Target {
    DateTime,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

impl Target {
    fn name(self) -> &'static str {
        match self {
            Self::DateTime => "datetime",
            Self::Date => "date",
            Self::Time => "time",
            Self::TimeTz => "time_tz",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamp_tz",
        }
    }
}

impl Kind {
    fn code(self) -> i64 {
        match self {
            Self::Date => 0,
            Self::Time => 1,
            Self::TimeTz => 2,
            Self::Timestamp => 3,
            Self::TimestampTz => 4,
        }
    }

    fn from_code(code: i64) -> Option<Self> {
        Some(match code {
            0 => Self::Date,
            1 => Self::Time,
            2 => Self::TimeTz,
            3 => Self::Timestamp,
            4 => Self::TimestampTz,
            _ => return None,
        })
    }
}

impl Item {
    pub(super) fn to_json(self) -> JsonValue {
        let mut object = Map::new();
        object.insert(
            ITEM_KEY.to_string(),
            JsonValue::Array(vec![
                self.kind.code().into(),
                self.value.into(),
                self.zone.into(),
            ]),
        );
        JsonValue::Object(object)
    }

    pub(super) fn from_json(value: &JsonValue) -> Option<Self> {
        let JsonValue::Object(object) = value else {
            return None;
        };
        if object.len() != 1 {
            return None;
        }
        let JsonValue::Array(parts) = object.get(ITEM_KEY)? else {
            return None;
        };
        let [kind, value, zone] = parts.as_slice() else {
            return None;
        };
        Some(Self {
            kind: Kind::from_code(kind.as_i64()?)?,
            value: value.as_i64()?,
            zone: i32::try_from(zone.as_i64()?).ok()?,
        })
    }

    /// `.type()`.
    pub(super) fn type_name(self) -> &'static str {
        match self.kind {
            Kind::Date => "date",
            Kind::Time => "time without time zone",
            Kind::TimeTz => "time with time zone",
            Kind::Timestamp => "timestamp without time zone",
            Kind::TimestampTz => "timestamp with time zone",
        }
    }

    /// `JsonEncodeDateTime`: the ISO 8601 text of `.string()` and of a
    /// datetime result, independent of DateStyle and TimeZone.
    pub(super) fn encode(self) -> String {
        match self.kind {
            Kind::Date => {
                let (date, bc) = encode_date(self.value);
                if bc {
                    format!("{date} BC")
                } else {
                    date
                }
            }
            Kind::Time => encode_clock(self.value),
            Kind::TimeTz => format!("{}{}", encode_clock(self.value), encode_zone(self.zone)),
            Kind::Timestamp => encode_timestamp(self.value, None),
            Kind::TimestampTz => encode_timestamp(
                self.value.saturating_add(i64::from(self.zone) * 1_000_000),
                Some(self.zone),
            ),
        }
    }
}

pub(super) fn is_item(value: &JsonValue) -> bool {
    Item::from_json(value).is_some()
}

/// Replaces every datetime item (at the top of a result list) by its text.
pub(super) fn finalize(values: Vec<JsonValue>) -> Vec<JsonValue> {
    values
        .into_iter()
        .map(|value| match Item::from_json(&value) {
            Some(item) => JsonValue::String(item.encode()),
            None => value,
        })
        .collect()
}

fn encode_date(days: i64) -> (String, bool) {
    let (year, month, day) = civil_from_days(days + UNIX_TO_PG_DAYS);
    if year <= 0 {
        (format!("{:04}-{month:02}-{day:02}", 1 - year), true)
    } else {
        (format!("{year:04}-{month:02}-{day:02}"), false)
    }
}

fn encode_clock(micros: i64) -> String {
    let seconds = micros / 1_000_000;
    let fraction = micros % 1_000_000;
    let mut text = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if fraction != 0 {
        let digits = format!("{fraction:06}");
        text.push('.');
        text.push_str(digits.trim_end_matches('0'));
    }
    text
}

/// `EncodeTimezone` in XSD style: `+hh:mm`, or `+hh:mm:ss`.
fn encode_zone(east: i32) -> String {
    let sign = if east >= 0 { '+' } else { '-' };
    let total = east.unsigned_abs();
    let (hours, minutes, seconds) = (total / 3600, total / 60 % 60, total % 60);
    if seconds != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}")
    }
}

fn encode_timestamp(local: i64, zone: Option<i32>) -> String {
    let (date, bc) = encode_date(local.div_euclid(DAY));
    let mut text = format!("{date}T{}", encode_clock(local.rem_euclid(DAY)));
    if let Some(zone) = zone {
        text.push_str(&encode_zone(zone));
    }
    if bc {
        text.push_str(" BC");
    }
    text
}

/// The session time zone's offset (seconds east) at a local timestamp.
fn offset_at_local(local: i64) -> i32 {
    local_timestamp_offset_seconds(PgTimestamp::Finite(local), &current_timezone_name())
        .unwrap_or(0)
}

/// The session time zone's offset (seconds east) at a UTC timestamp.
fn offset_at_utc(utc: i64) -> i32 {
    timezone_offset_at_timestamp(&current_timezone_name(), PgTimestamp::Finite(utc)).unwrap_or(0)
}

fn local_to_utc(local: i64) -> (i64, i32) {
    let offset = offset_at_local(local);
    (local - i64::from(offset) * 1_000_000, offset)
}

fn utc_to_local(utc: i64) -> i64 {
    utc + i64::from(offset_at_utc(utc)) * 1_000_000
}

/// `checkTimezoneIsUsedForCast`: raised even in silent mode.
fn require_timezone(use_tz: bool, from: &str, to: &str) -> Result<()> {
    if use_tz {
        return Ok(());
    }
    Err(SqlError::data_exception_public(
        "0A000",
        format!(
            "cannot convert value from {from} to {to} without time zone usage\nHINT:  Use *_tz() function for time zone support."
        ),
        None,
    ))
}

/// Whether an error is one PostgreSQL raises outside the jsonpath error
/// handling, so the `silent` argument does not suppress it.
pub(super) fn is_unsuppressible(error: &SqlError) -> bool {
    error.sqlstate() == "0A000"
}

fn datetime_error(message: String) -> SqlError {
    SqlError::data_exception_public("22031", message, None)
}

fn not_recognized(target: &str, text: &str) -> SqlError {
    datetime_error(format!("{target} format is not recognized: \"{text}\""))
}

/// ISO formats tried in order when no template is given.
const ISO_FORMATS: [&str; 13] = [
    "yyyy-mm-dd",
    "HH24:MI:SS.USTZ",
    "HH24:MI:SSTZ",
    "HH24:MI:SS.US",
    "HH24:MI:SS",
    "yyyy-mm-dd HH24:MI:SS.USTZ",
    "yyyy-mm-dd HH24:MI:SSTZ",
    "yyyy-mm-dd\"T\"HH24:MI:SS.USTZ",
    "yyyy-mm-dd\"T\"HH24:MI:SSTZ",
    "yyyy-mm-dd HH24:MI:SS.US",
    "yyyy-mm-dd HH24:MI:SS",
    "yyyy-mm-dd\"T\"HH24:MI:SS.US",
    "yyyy-mm-dd\"T\"HH24:MI:SS",
];

/// A precision argument as written: `Err` when it does not fit an integer.
pub(super) type Precision = std::result::Result<i64, ()>;

/// `executeDateTimeMethod` on a string item.
pub(super) fn execute(
    target: Target,
    text: &str,
    template: Option<&str>,
    precision: Option<Precision>,
    use_tz: bool,
) -> Result<Item> {
    let precision = match precision {
        None => None,
        Some(Ok(value)) if i32::try_from(value).is_ok() => Some(value as i32),
        Some(_) => {
            return Err(datetime_error(format!(
                "time precision of jsonpath item method .{}() is out of range for type integer",
                target.name()
            )))
        }
    };
    let parsed = match template {
        Some(template) => parse_datetime_std(text, template)?,
        None => ISO_FORMATS
            .iter()
            .find_map(|format| parse_datetime_std(text, format).ok())
            .ok_or_else(|| {
                if target == Target::DateTime {
                    datetime_error(format!(
                        "datetime format is not recognized: \"{text}\"\nHINT:  Use a datetime template argument to specify the input data format."
                    ))
                } else {
                    not_recognized(target.name(), text)
                }
            })?,
    };
    let mut item = Item {
        kind: match parsed.kind {
            ParsedKind::Date => Kind::Date,
            ParsedKind::Time => Kind::Time,
            ParsedKind::TimeTz => Kind::TimeTz,
            ParsedKind::Timestamp => Kind::Timestamp,
            ParsedKind::TimestampTz => Kind::TimestampTz,
        },
        value: parsed.value,
        zone: parsed.offset,
    };
    let name = target.name();
    item = match (target, item.kind) {
        (Target::DateTime, _) => item,
        (Target::Date, Kind::Date) => item,
        (Target::Date, Kind::Time | Kind::TimeTz) => return Err(not_recognized(name, text)),
        (Target::Date, Kind::Timestamp) => date(item.value.div_euclid(DAY)),
        (Target::Date, Kind::TimestampTz) => {
            require_timezone(use_tz, "timestamptz", "date")?;
            date(utc_to_local(item.value).div_euclid(DAY))
        }
        (Target::Time, Kind::Date) => return Err(not_recognized(name, text)),
        (Target::Time, Kind::Time) => item,
        (Target::Time, Kind::TimeTz) => {
            require_timezone(use_tz, "timetz", "time")?;
            time(item.value)
        }
        (Target::Time, Kind::Timestamp) => time(item.value.rem_euclid(DAY)),
        (Target::Time, Kind::TimestampTz) => {
            require_timezone(use_tz, "timestamptz", "time")?;
            time(utc_to_local(item.value).rem_euclid(DAY))
        }
        (Target::TimeTz, Kind::Date | Kind::Timestamp) => return Err(not_recognized(name, text)),
        (Target::TimeTz, Kind::Time) => time_to_timetz(item, use_tz)?,
        (Target::TimeTz, Kind::TimeTz) => item,
        (Target::TimeTz, Kind::TimestampTz) => {
            let offset = offset_at_utc(item.value);
            Item {
                kind: Kind::TimeTz,
                value: (item.value + i64::from(offset) * 1_000_000).rem_euclid(DAY),
                zone: offset,
            }
        }
        (Target::Timestamp, Kind::Date) => timestamp(date_to_timestamp(item.value)?),
        (Target::Timestamp, Kind::Time | Kind::TimeTz) => return Err(not_recognized(name, text)),
        (Target::Timestamp, Kind::Timestamp) => item,
        (Target::Timestamp, Kind::TimestampTz) => {
            require_timezone(use_tz, "timestamptz", "timestamp")?;
            timestamp(utc_to_local(item.value))
        }
        (Target::TimestampTz, Kind::Date) => {
            require_timezone(use_tz, "date", "timestamptz")?;
            timestamptz_from_local(date_to_timestamp(item.value)?)
        }
        (Target::TimestampTz, Kind::Time | Kind::TimeTz) => return Err(not_recognized(name, text)),
        (Target::TimestampTz, Kind::Timestamp) => {
            require_timezone(use_tz, "timestamp", "timestamptz")?;
            timestamptz_from_local(item.value)
        }
        (Target::TimestampTz, Kind::TimestampTz) => item,
    };
    if let Some(precision) = precision {
        let precision = typmod_check(target, precision)?;
        item.value = round_micros_to_precision(item.value, precision);
    }
    Ok(item)
}

/// `date2timestamp`: midnight of the date, within the timestamp range.
fn date_to_timestamp(days: i64) -> Result<i64> {
    days.checked_mul(DAY)
        .filter(|micros| *micros < 9_223_371_331_200_000_000)
        .ok_or_else(|| {
            SqlError::data_exception_public("22008", "date out of range for timestamp", None)
        })
}

fn date(days: i64) -> Item {
    Item {
        kind: Kind::Date,
        value: days,
        zone: 0,
    }
}

fn time(micros: i64) -> Item {
    Item {
        kind: Kind::Time,
        value: micros,
        zone: 0,
    }
}

fn timestamp(local: i64) -> Item {
    Item {
        kind: Kind::Timestamp,
        value: local,
        zone: 0,
    }
}

fn timestamptz_from_local(local: i64) -> Item {
    let (utc, offset) = local_to_utc(local);
    Item {
        kind: Kind::TimestampTz,
        value: utc,
        zone: offset,
    }
}

/// `time_timetz`: the session zone's offset on the current date.
fn time_to_timetz(item: Item, use_tz: bool) -> Result<Item> {
    require_timezone(use_tz, "time", "timetz")?;
    let now = transaction_timestamp_micros() - UNIX_TO_PG_DAYS * DAY;
    let today = utc_to_local(now).div_euclid(DAY);
    Ok(Item {
        kind: Kind::TimeTz,
        value: item.value,
        zone: offset_at_local(today * DAY + item.value),
    })
}

/// `anytime_typmod_check` / `anytimestamp_typmod_check`: precisions above 6
/// are reduced with a WARNING.
fn typmod_check(target: Target, precision: i32) -> Result<i32> {
    let (type_name, zoned) = match target {
        Target::Time => ("TIME", false),
        Target::TimeTz => ("TIME", true),
        Target::Timestamp => ("TIMESTAMP", false),
        _ => ("TIMESTAMP", true),
    };
    let suffix = if zoned { " WITH TIME ZONE" } else { "" };
    if precision < 0 {
        return Err(SqlError::data_exception_public(
            "22023",
            format!("{type_name}({precision}){suffix} precision must not be negative"),
            None,
        ));
    }
    if precision > 6 {
        // Outside a statement context there is no client to warn.
        let _ = crate::statement_context::note_notice(SqlNotice {
            sqlstate: "22023".to_string(),
            message: format!(
                "{type_name}({precision}){suffix} precision reduced to maximum allowed, 6"
            ),
            fields: vec![crate::SqlErrorField::Severity("WARNING".to_string())],
        });
        return Ok(6);
    }
    Ok(precision)
}

/// `compareDatetime`: `None` when the types are not comparable.
pub(super) fn compare(left: Item, right: Item, use_tz: bool) -> Result<Option<Ordering>> {
    use Kind::*;
    // A date past the timestamp range compares beyond every timestamp
    // (`date_cmp_timestamp_internal`).
    let date_micros = |days: i64| -> i128 {
        match date_to_timestamp(days) {
            Ok(micros) => i128::from(micros),
            Err(_) if days > 0 => i128::MAX,
            Err(_) => i128::MIN,
        }
    };
    let date_utc = |days: i64| -> i128 {
        match date_to_timestamp(days) {
            Ok(micros) => i128::from(local_to_utc(micros).0),
            Err(_) => date_micros(days),
        }
    };
    Ok(Some(match (left.kind, right.kind) {
        (Date, Date) | (Time, Time) | (Timestamp, Timestamp) | (TimestampTz, TimestampTz) => {
            left.value.cmp(&right.value)
        }
        (Date, Timestamp) => date_micros(left.value).cmp(&i128::from(right.value)),
        (Timestamp, Date) => i128::from(left.value).cmp(&date_micros(right.value)),
        (Date, TimestampTz) => {
            require_timezone(use_tz, "date", "timestamptz")?;
            date_utc(left.value).cmp(&i128::from(right.value))
        }
        (TimestampTz, Date) => {
            require_timezone(use_tz, "date", "timestamptz")?;
            i128::from(left.value).cmp(&date_utc(right.value))
        }
        (Timestamp, TimestampTz) => {
            require_timezone(use_tz, "timestamp", "timestamptz")?;
            local_to_utc(left.value).0.cmp(&right.value)
        }
        (TimestampTz, Timestamp) => {
            require_timezone(use_tz, "timestamp", "timestamptz")?;
            left.value.cmp(&local_to_utc(right.value).0)
        }
        (Time, TimeTz) => timetz_cmp(time_to_timetz(left, use_tz)?, right),
        (TimeTz, Time) => timetz_cmp(left, time_to_timetz(right, use_tz)?),
        (TimeTz, TimeTz) => timetz_cmp(left, right),
        _ => return Ok(None),
    }))
}

/// `timetz_cmp_internal`: by UTC time, then by zone (west first).
fn timetz_cmp(left: Item, right: Item) -> Ordering {
    let utc = |item: Item| item.value - i64::from(item.zone) * 1_000_000;
    utc(left)
        .cmp(&utc(right))
        .then_with(|| right.zone.cmp(&left.zone))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(target: Target, text: &str, template: Option<&str>) -> Result<String> {
        execute(target, text, template, None, true).map(Item::encode)
    }

    #[test]
    fn iso_formats_pick_the_first_fitting_type() {
        let cases = [
            ("2017-03-10", "2017-03-10", Kind::Date),
            (
                "2017-03-10 12:34:56",
                "2017-03-10T12:34:56",
                Kind::Timestamp,
            ),
            (
                "2017-03-10 12:34:56+3",
                "2017-03-10T12:34:56+03:00",
                Kind::TimestampTz,
            ),
            (
                "2017-03-10T12:34:56.789+3:10",
                "2017-03-10T12:34:56.789+03:10",
                Kind::TimestampTz,
            ),
            ("12:34:56", "12:34:56", Kind::Time),
            ("12:34:56+3:10", "12:34:56+03:10", Kind::TimeTz),
            (
                "2017-03-10T12:34:56.789EST",
                "2017-03-10T12:34:56.789-05:00",
                Kind::TimestampTz,
            ),
        ];
        for (input, expected, kind) in cases {
            let item = execute(Target::DateTime, input, None, None, false).unwrap();
            assert_eq!(
                (item.encode(), item.kind),
                (expected.to_string(), kind),
                "{input}"
            );
        }
        // A lowercase `t` does not match the quoted "T" of the ISO formats.
        assert!(run(Target::DateTime, "2017-03-10t12:34:56+3:10", None).is_err());
        assert!(run(Target::DateTime, "bogus", None)
            .unwrap_err()
            .to_string()
            .contains("datetime format is not recognized: \"bogus\""));
    }

    #[test]
    fn templates_follow_standard_mode() {
        assert_eq!(
            run(Target::DateTime, "10-03-2017", Some("dd-mm-yyyy")).unwrap(),
            "2017-03-10"
        );
        assert_eq!(
            run(
                Target::DateTime,
                "10-03-2017T12:34:56",
                Some("dd-mm-yyyy\"T\"HH24:MI:SS")
            )
            .unwrap(),
            "2017-03-10T12:34:56"
        );
        assert_eq!(
            run(Target::DateTime, "12:34 -05:20", Some("HH24:MI TZH:TZM")).unwrap(),
            "12:34:00-05:20"
        );
        for (input, template, message) in [
            (
                "10-03-2017 12:34",
                "dd-mm-yyyy",
                "trailing characters remain",
            ),
            (
                "10-03-2017t12:34:56",
                "dd-mm-yyyy\"T\"HH24:MI:SS",
                "unmatched format character \"T\"",
            ),
            ("12:34", "aaa", "invalid datetime format separator: \"a\""),
            ("12:34", "HH24:MI TZH", "input string is too short"),
        ] {
            let error = run(Target::DateTime, input, Some(template)).unwrap_err();
            assert!(error.to_string().contains(message), "{input}: {error}");
        }
    }

    #[test]
    fn conversions_need_time_zone_usage_where_postgres_does() {
        let error = execute(Target::Time, "12:34:56 +05:30", None, None, false).unwrap_err();
        assert_eq!(error.sqlstate(), "0A000");
        assert!(is_unsuppressible(&error));
        assert_eq!(
            run(Target::Time, "12:34:56 +05:30", None).unwrap(),
            "12:34:56"
        );
        assert_eq!(
            run(Target::Timestamp, "2023-08-15", None).unwrap(),
            "2023-08-15T00:00:00"
        );
        assert!(run(Target::Date, "12:34:56", None)
            .unwrap_err()
            .to_string()
            .contains("date format is not recognized"));
        let rounded = execute(Target::Time, "12:34:56.789", None, Some(Ok(2)), false).unwrap();
        assert_eq!(rounded.encode(), "12:34:56.79");
        let error = execute(
            Target::Time,
            "12:34:56",
            None,
            Some(Ok(12_345_678_901)),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("out of range for type integer"));
    }

    #[test]
    fn comparisons_follow_compare_datetime() {
        let item = |target, text| execute(target, text, None, None, true).unwrap();
        let date = item(Target::Date, "2017-03-10");
        let stamp = item(Target::Timestamp, "2017-03-10 00:00:00");
        assert_eq!(compare(date, stamp, false).unwrap(), Some(Ordering::Equal));
        let time = item(Target::Time, "12:35:00");
        assert_eq!(compare(date, time, false).unwrap(), None);
        let east = item(Target::TimeTz, "12:35:00 +1");
        let west = item(Target::TimeTz, "13:35:00 +2");
        assert_eq!(compare(east, west, false).unwrap(), Some(Ordering::Greater));
        let zoned = item(Target::TimestampTz, "2017-03-10 12:35:00 +1");
        assert_eq!(
            compare(stamp, zoned, false).unwrap_err().sqlstate(),
            "0A000"
        );
    }
}
