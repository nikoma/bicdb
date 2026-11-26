//! DateStyle output: date, timestamp and timestamptz text in the session's
//! `DateStyle` (ISO, Postgres, SQL, German with its MDY/DMY/YMD order), as
//! PostgreSQL's output functions print them. Values are carried as ISO text;
//! the protocol layer renders them per connection with [`TemporalOutput`].

use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateStyle {
    Iso,
    Postgres,
    Sql,
    German,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

/// The per-connection output settings for date/time text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemporalOutput {
    pub style: DateStyle,
    pub order: DateOrder,
    /// The session time zone, for zone abbreviations.
    pub timezone: String,
    /// The session `IntervalStyle`.
    pub interval_style: String,
}

impl Default for TemporalOutput {
    fn default() -> Self {
        Self {
            style: DateStyle::Iso,
            order: DateOrder::Mdy,
            timezone: "UTC".to_string(),
            interval_style: "postgres".to_string(),
        }
    }
}

const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// `(style, order)` of a normalized or raw `DateStyle` value.
pub fn parse_datestyle(value: &str) -> (DateStyle, DateOrder) {
    let mut style = DateStyle::Iso;
    let mut order = DateOrder::Mdy;
    let mut order_set = false;
    for token in value
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .filter(|token| !token.is_empty())
    {
        match token.to_ascii_lowercase().as_str() {
            "iso" => style = DateStyle::Iso,
            "sql" => style = DateStyle::Sql,
            "postgres" => style = DateStyle::Postgres,
            "german" => {
                style = DateStyle::German;
                // German implies DMY unless an order is given.
                if !order_set {
                    order = DateOrder::Dmy;
                }
            }
            "mdy" | "us" | "noneuropean" => {
                order = DateOrder::Mdy;
                order_set = true;
            }
            "dmy" | "euro" | "european" => {
                order = DateOrder::Dmy;
                order_set = true;
            }
            "ymd" => {
                order = DateOrder::Ymd;
                order_set = true;
            }
            _ => {}
        }
    }
    (style, order)
}

/// Session GUC key prefix recording the connection-start value a `RESET`
/// returns `DateStyle`, `TimeZone` and `IntervalStyle` to.
pub const STARTUP_DEFAULT_PREFIX: &str = "bicdb_startup_default:";

/// The canonical value of a date/time setting given at connection startup
/// (`DateStyle`, `TimeZone`, `IntervalStyle`), or `None` when the name is not
/// one of them or the value is invalid.
pub fn normalize_startup_temporal_setting(name: &str, value: &str) -> Option<(String, String)> {
    match name.to_ascii_lowercase().as_str() {
        "datestyle" => crate::eval::normalize_datestyle_setting(value)
            .ok()
            .map(|value| ("datestyle".to_string(), value)),
        "timezone" => crate::eval::validate_timezone_name(value)
            .then(|| ("timezone".to_string(), value.to_string())),
        "intervalstyle" => {
            let lower = value.trim().to_ascii_lowercase();
            matches!(
                lower.as_str(),
                "postgres" | "postgres_verbose" | "sql_standard" | "iso_8601"
            )
            .then(|| ("intervalstyle".to_string(), lower))
        }
        _ => None,
    }
}

impl TemporalOutput {
    /// The output settings of a session's GUCs.
    pub fn from_gucs(gucs: &HashMap<String, String>) -> Self {
        let (style, order) = gucs
            .get("datestyle")
            .map(|value| parse_datestyle(value))
            .unwrap_or((DateStyle::Iso, DateOrder::Mdy));
        Self {
            style,
            order,
            timezone: gucs
                .get("timezone")
                .cloned()
                .unwrap_or_else(|| "UTC".to_string()),
            interval_style: gucs
                .get("intervalstyle")
                .cloned()
                .unwrap_or_else(|| "postgres".to_string()),
        }
    }

    pub fn is_iso(&self) -> bool {
        self.style == DateStyle::Iso
    }

    /// Whether values of type `oid` are rendered by [`Self::render`].
    pub fn styles(&self, oid: i32) -> bool {
        oid == 1186 || (!self.is_iso() && matches!(oid, 1082 | 1114 | 1184))
    }

    /// `text` (ISO output of a value of type `oid`) in this style; `None`
    /// when the type is not styled or the text is not ISO date/time text.
    pub fn render(&self, oid: i32, text: &str) -> Option<String> {
        if oid == 1186 {
            // Interval text in the default `postgres` layout (as stored) is
            // printed in the connection's IntervalStyle.
            if self.interval_style.eq_ignore_ascii_case("postgres")
                || !is_postgres_interval_text(text)
            {
                return None;
            }
            let interval = crate::PgInterval::from_postgres_text(text).ok()?;
            let styled = interval.to_style_text(&self.interval_style);
            return (styled != text).then_some(styled);
        }
        if !self.styles(oid) {
            return None;
        }
        if matches!(text, "infinity" | "-infinity") {
            return None;
        }
        let parts = IsoParts::parse(text, oid != 1082)?;
        Some(self.render_parts(&parts, oid))
    }

    fn render_parts(&self, parts: &IsoParts<'_>, oid: i32) -> String {
        let dmy = self.order == DateOrder::Dmy;
        let year = format!("{:04}", parts.year);
        let mut out = String::with_capacity(32);
        if oid == 1082 {
            match self.style {
                DateStyle::Sql if dmy => {
                    out.push_str(&format!("{:02}/{:02}/{year}", parts.day, parts.month))
                }
                DateStyle::Sql => {
                    out.push_str(&format!("{:02}/{:02}/{year}", parts.month, parts.day))
                }
                DateStyle::German => {
                    out.push_str(&format!("{:02}.{:02}.{year}", parts.day, parts.month))
                }
                DateStyle::Postgres if dmy => {
                    out.push_str(&format!("{:02}-{:02}-{year}", parts.day, parts.month))
                }
                _ => out.push_str(&format!("{:02}-{:02}-{year}", parts.month, parts.day)),
            }
        } else {
            let time = parts.time.unwrap_or("00:00:00");
            let zone = (oid == 1184).then(|| self.zone_text(parts)).flatten();
            match self.style {
                DateStyle::Sql | DateStyle::German => {
                    let date = match self.style {
                        DateStyle::German => {
                            format!("{:02}.{:02}.{year}", parts.day, parts.month)
                        }
                        _ if dmy => format!("{:02}/{:02}/{year}", parts.day, parts.month),
                        _ => format!("{:02}/{:02}/{year}", parts.month, parts.day),
                    };
                    out.push_str(&date);
                    out.push(' ');
                    out.push_str(time);
                    match zone {
                        Some(Zone::Name(name)) => {
                            out.push(' ');
                            out.push_str(&name);
                        }
                        Some(Zone::Offset(offset)) => out.push_str(offset),
                        None => {}
                    }
                }
                _ => {
                    out.push_str(DAYS[parts.weekday()]);
                    out.push(' ');
                    let month = MONTHS[parts.month as usize - 1];
                    if dmy {
                        out.push_str(&format!("{:02} {month}", parts.day));
                    } else {
                        out.push_str(&format!("{month} {:02}", parts.day));
                    }
                    out.push(' ');
                    out.push_str(time);
                    out.push(' ');
                    out.push_str(&year);
                    match zone {
                        Some(Zone::Name(name)) => {
                            out.push(' ');
                            out.push_str(&name);
                        }
                        Some(Zone::Offset(offset)) => {
                            out.push(' ');
                            out.push_str(offset);
                        }
                        None => {}
                    }
                }
            }
        }
        if parts.bc {
            out.push_str(" BC");
        }
        out
    }

    /// The session zone's abbreviation at the instant, or the numeric
    /// offset when the zone has none.
    fn zone_text<'a>(&self, parts: &IsoParts<'a>) -> Option<Zone<'a>> {
        use chrono::offset::{Offset, TimeZone};
        use chrono_tz::OffsetName;
        let offset_text = parts.offset?;
        let numeric = Some(Zone::Offset(offset_text));
        let offset = parse_offset(offset_text)?;
        let Ok(zone) = self.timezone.parse::<chrono_tz::Tz>() else {
            // An abbreviation or a POSIX zone names itself.
            if let Some((name, zone_offset)) = crate::eval::posix_timezone(&self.timezone) {
                if zone_offset == offset {
                    return Some(Zone::Name(name));
                }
            }
            if crate::eval::timezone_abbreviation_offset(&self.timezone) == Some(offset) {
                return Some(Zone::Name(self.timezone.to_ascii_uppercase()));
            }
            return numeric;
        };
        let year = if parts.bc { 1 - parts.year } else { parts.year };
        let (hour, minute, second) = parts.clock()?;
        let local = chrono::NaiveDate::from_ymd_opt(year as i32, parts.month, parts.day)?
            .and_hms_opt(hour, minute, second)?;
        let utc = local - chrono::Duration::seconds(i64::from(offset));
        let at = zone.offset_from_utc_datetime(&utc);
        if at.fix().local_minus_utc() != offset {
            return numeric;
        }
        match at.abbreviation() {
            Some(name) if !name.is_empty() => Some(Zone::Name(name.to_string())),
            _ => numeric,
        }
    }
}

/// Interval text in the `postgres` IntervalStyle: unit words
/// (`1 year 2 mons 3 days`) with an optional clock, or a bare clock.
fn is_postgres_interval_text(text: &str) -> bool {
    if text.starts_with(['@', 'P', '+']) || matches!(text, "infinity" | "-infinity") {
        return false;
    }
    let words_ok = text.split_whitespace().all(|word| {
        let word = word.trim_start_matches('-');
        word.bytes().all(|byte| byte.is_ascii_digit())
            || matches!(word, "year" | "years" | "mon" | "mons" | "day" | "days")
            || word
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b':' | b'.'))
                && word.contains(':')
    });
    words_ok
        && (text.contains(':')
            || text.contains("year")
            || text.contains("mon")
            || text.contains("day"))
}

enum Zone<'a> {
    Name(String),
    Offset(&'a str),
}

fn parse_offset(text: &str) -> Option<i32> {
    let sign = match text.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let mut fields = text[1..].split(':');
    let hours = fields.next()?.parse::<i32>().ok()?;
    let minutes = fields.next().map_or(Some(0), |field| field.parse().ok())?;
    let seconds = fields.next().map_or(Some(0), |field| field.parse().ok())?;
    Some(sign * (hours * 3600 + minutes * 60 + seconds))
}

struct IsoParts<'a> {
    /// The displayed year (positive; `bc` marks BC years).
    year: i64,
    month: u32,
    day: u32,
    time: Option<&'a str>,
    offset: Option<&'a str>,
    bc: bool,
}

impl<'a> IsoParts<'a> {
    fn parse(text: &'a str, with_time: bool) -> Option<Self> {
        let (text, bc) = match text.strip_suffix(" BC") {
            Some(rest) => (rest, true),
            None => (text, false),
        };
        let (date, rest) = match text.split_once(' ') {
            Some((date, rest)) => (date, Some(rest)),
            None => (text, None),
        };
        if rest.is_some() != with_time {
            return None;
        }
        let mut fields = date.rsplitn(3, '-');
        let day = fields.next()?.parse::<u32>().ok()?;
        let month = fields.next()?.parse::<u32>().ok()?;
        let year = fields.next()?.parse::<i64>().ok()?;
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) || year < 0 {
            return None;
        }
        let (time, offset) = match rest {
            None => (None, None),
            Some(rest) => match rest.find(['+', '-']) {
                Some(at) => (Some(&rest[..at]), Some(&rest[at..])),
                None => (Some(rest), None),
            },
        };
        if time.is_some_and(|time| {
            !time
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b':' | b'.'))
        }) {
            return None;
        }
        Some(Self {
            year,
            month,
            day,
            time,
            offset,
            bc,
        })
    }

    fn clock(&self) -> Option<(u32, u32, u32)> {
        let time = self.time?;
        let mut fields = time.split(':');
        let hour = fields.next()?.parse().ok()?;
        let minute = fields.next()?.parse().ok()?;
        let second = fields
            .next()
            .map_or(Some(0), |field| field.split('.').next()?.parse().ok())?;
        Some((hour, minute, second))
    }

    /// 0 = Sunday.
    fn weekday(&self) -> usize {
        let year = if self.bc { 1 - self.year } else { self.year };
        let (y, m) = if self.month <= 2 {
            (year - 1, i64::from(self.month) + 9)
        } else {
            (year, i64::from(self.month) - 3)
        };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let doy = (153 * m + 2) / 5 + i64::from(self.day) - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146_097 + doe - 719_468;
        // 1970-01-01 was a Thursday.
        (days + 4).rem_euclid(7) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(style: &str, timezone: &str) -> TemporalOutput {
        let (style, order) = parse_datestyle(style);
        TemporalOutput {
            style,
            order,
            timezone: timezone.into(),
            interval_style: "postgres".into(),
        }
    }

    #[test]
    fn styles_render_as_postgresql_prints_them() {
        let pg = output("Postgres, MDY", "America/Los_Angeles");
        assert_eq!(pg.render(1082, "1997-02-10").unwrap(), "02-10-1997");
        assert_eq!(
            pg.render(1114, "1997-02-10 17:32:01.4").unwrap(),
            "Mon Feb 10 17:32:01.4 1997"
        );
        assert_eq!(
            pg.render(1184, "1997-02-10 17:32:01-08").unwrap(),
            "Mon Feb 10 17:32:01 1997 PST"
        );
        assert_eq!(
            pg.render(1114, "0097-02-16 17:32:01 BC").unwrap(),
            "Tue Feb 16 17:32:01 0097 BC"
        );
        assert_eq!(pg.render(1114, "infinity"), None);
        let dmy = output("Postgres, DMY", "UTC");
        assert_eq!(
            dmy.render(1184, "2000-01-01 00:00:00+00").unwrap(),
            "Sat 01 Jan 00:00:00 2000 UTC"
        );
        let sql = output("SQL, MDY", "America/Los_Angeles");
        assert_eq!(
            sql.render(1184, "1997-07-10 17:32:01-07").unwrap(),
            "07/10/1997 17:32:01 PDT"
        );
        assert_eq!(sql.render(1082, "1997-07-10").unwrap(), "07/10/1997");
        let german = output("German", "Europe/Berlin");
        assert_eq!(german.render(1082, "1997-07-10").unwrap(), "10.07.1997");
        assert_eq!(
            german.render(1184, "1997-07-10 17:32:01+02").unwrap(),
            "10.07.1997 17:32:01 CEST"
        );
        let fixed = output("Postgres", "+05");
        assert_eq!(
            fixed.render(1184, "2000-01-01 00:00:00-05").unwrap(),
            "Sat Jan 01 00:00:00 2000 -05"
        );
        assert!(output("ISO", "UTC").render(1082, "1997-07-10").is_none());
    }
}
