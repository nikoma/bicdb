//! POSIX time zone specifications with daylight-saving rules
//! (`CST7CDT,M4.1.0,M10.5.0`), as PostgreSQL's `tzparse` accepts them for
//! `SET TIME ZONE`.

/// A parsed `STDoffset DST[offset][,start[/time],end[/time]]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PosixZone {
    pub(crate) std_name: String,
    /// Seconds east of UTC.
    pub(crate) std_offset: i32,
    pub(crate) dst_name: String,
    pub(crate) dst_offset: i32,
    start: Rule,
    end: Rule,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Day {
    /// `Jn`: day 1..=365, February 29 never counted.
    Julian(i64),
    /// `n`: zero-based day of the year, leap days counted.
    ZeroBased(i64),
    /// `Mm.w.d`: day `d` (0 = Sunday) of week `w` (5 = last) of month `m`.
    MonthWeekDay(i64, i64, i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Rule {
    day: Day,
    /// Seconds after local midnight.
    time: i64,
}

fn name(text: &str) -> Option<(String, &str)> {
    if let Some(quoted) = text.strip_prefix('<') {
        let end = quoted.find('>')?;
        let name = &quoted[..end];
        if name.len() < 3 {
            return None;
        }
        return Some((name.to_string(), &quoted[end + 1..]));
    }
    let letters = text.bytes().take_while(u8::is_ascii_alphabetic).count();
    if letters < 3 {
        return None;
    }
    Some((text[..letters].to_string(), &text[letters..]))
}

fn number(text: &str, max_digits: usize) -> Option<(i64, &str)> {
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 || digits > max_digits {
        return None;
    }
    Some((text[..digits].parse().ok()?, &text[digits..]))
}

/// `[+-]hh[:mm[:ss]]` in seconds, positive as written.
fn clock(text: &str, max_hours: i64) -> Option<(i64, &str)> {
    let (sign, text) = match text.as_bytes().first() {
        Some(b'-') => (-1, &text[1..]),
        Some(b'+') => (1, &text[1..]),
        _ => (1, text),
    };
    let (hours, mut rest) = number(text, 3)?;
    if hours > max_hours {
        return None;
    }
    let mut seconds = hours * 3600;
    for scale in [60, 1] {
        let Some(after) = rest.strip_prefix(':') else {
            break;
        };
        let (value, after) = number(after, 2)?;
        if value > 59 {
            return None;
        }
        seconds += value * scale;
        rest = after;
    }
    Some((sign * seconds, rest))
}

fn rule(text: &str) -> Option<(Rule, &str)> {
    let (day, rest) = if let Some(after) = text.strip_prefix('J') {
        let (day, rest) = number(after, 3)?;
        (1..=365)
            .contains(&day)
            .then_some((Day::Julian(day), rest))?
    } else if let Some(after) = text.strip_prefix('M') {
        let (month, rest) = number(after, 2)?;
        let (week, rest) = number(rest.strip_prefix('.')?, 1)?;
        let (weekday, rest) = number(rest.strip_prefix('.')?, 1)?;
        ((1..=12).contains(&month) && (1..=5).contains(&week) && weekday <= 6)
            .then_some((Day::MonthWeekDay(month, week, weekday), rest))?
    } else {
        let (day, rest) = number(text, 3)?;
        (day <= 365).then_some((Day::ZeroBased(day), rest))?
    };
    let (time, rest) = match rest.strip_prefix('/') {
        Some(after) => clock(after, 167)?,
        None => (7200, rest),
    };
    Some((Rule { day, time }, rest))
}

/// Parses a POSIX zone that has a daylight-saving part; zones without one
/// are fixed offsets handled elsewhere.
pub(crate) fn parse(spec: &str) -> Option<PosixZone> {
    let spec = spec.trim();
    let (std_name, rest) = name(spec)?;
    let (std_west, rest) = clock(rest, 24)?;
    let (dst_name, rest) = name(rest)?;
    let (dst_west, rest) = if rest.is_empty() || rest.starts_with(',') {
        (std_west - 3600, rest)
    } else {
        clock(rest, 24)?
    };
    // Without rules, PostgreSQL applies its default (US) rules.
    let rest = if rest.is_empty() {
        ",M3.2.0,M11.1.0"
    } else {
        rest
    };
    let (start, rest) = rule(rest.strip_prefix(',')?)?;
    let (end, rest) = rule(rest.strip_prefix(',')?)?;
    if !rest.is_empty() {
        return None;
    }
    Some(PosixZone {
        std_name,
        std_offset: -(std_west as i32),
        dst_name,
        dst_offset: -(dst_west as i32),
        start,
        end,
    })
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Days from 1970-01-01.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn year_of_days(days: i64) -> i64 {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    yoe + era * 400 + i64::from(mp >= 10)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if is_leap(year) => 29,
        _ => 28,
    }
}

impl Rule {
    /// Local seconds (from 1970-01-01) of the rule's transition in `year`.
    fn local_seconds(self, year: i64) -> i64 {
        let jan1 = days_from_civil(year, 1, 1);
        let day = match self.day {
            Day::Julian(day) => {
                let day = jan1 + day - 1;
                if is_leap(year) && day >= jan1 + 59 {
                    day + 1
                } else {
                    day
                }
            }
            Day::ZeroBased(day) => jan1 + day,
            Day::MonthWeekDay(month, week, weekday) => {
                let first = days_from_civil(year, month, 1);
                // 1970-01-01 was a Thursday (weekday 4).
                let first_weekday = (first + 4).rem_euclid(7);
                let mut day = first + (weekday - first_weekday).rem_euclid(7) + (week - 1) * 7;
                while day >= first + days_in_month(year, month) {
                    day -= 7;
                }
                day
            }
        };
        day * 86_400 + self.time
    }
}

impl PosixZone {
    /// Whether daylight-saving time is in effect at UTC second `utc`
    /// (seconds from 1970-01-01).
    fn is_dst(&self, utc: i64) -> bool {
        let year = year_of_days((utc + i64::from(self.std_offset)).div_euclid(86_400));
        let start = self.start.local_seconds(year) - i64::from(self.std_offset);
        let end = self.end.local_seconds(year) - i64::from(self.dst_offset);
        if start < end {
            start <= utc && utc < end
        } else {
            !(end <= utc && utc < start)
        }
    }

    /// The offset (seconds east) and abbreviation in effect at a UTC
    /// instant (seconds from 1970-01-01).
    pub(crate) fn at_utc(&self, utc: i64) -> (i32, &str) {
        if self.is_dst(utc) {
            (self.dst_offset, &self.dst_name)
        } else {
            (self.std_offset, &self.std_name)
        }
    }

    /// The offset for a local time (seconds from 1970-01-01): an ambiguous
    /// or skipped local time takes standard time, as PostgreSQL resolves a
    /// transition (before a spring-forward, after a fall-back).
    pub(crate) fn offset_for_local(&self, local: i64) -> i32 {
        let dst = self.at_utc(local - i64::from(self.dst_offset)).0 == self.dst_offset;
        let std = self.at_utc(local - i64::from(self.std_offset)).0 == self.std_offset;
        if dst && !std {
            self.dst_offset
        } else {
            self.std_offset
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rules_and_applies_them() {
        let zone = parse("CST7CDT,M4.1.0,M10.5.0").unwrap();
        assert_eq!((zone.std_offset, zone.dst_offset), (-7 * 3600, -6 * 3600));
        // 2005-04-03 is the first Sunday of April; DST begins at 02:00 local.
        let local = days_from_civil(2005, 4, 3) * 86_400 + 12 * 3600;
        assert_eq!(zone.offset_for_local(local), -6 * 3600);
        let before = days_from_civil(2005, 4, 2) * 86_400 + 12 * 3600;
        assert_eq!(zone.offset_for_local(before), -7 * 3600);
        assert_eq!(zone.at_utc(local + 6 * 3600).1, "CDT");
        assert!(parse("PST8PDT").is_some());
        assert!(parse("EST5").is_none());
        assert!(parse("<+04>-4<+05>,J60/1,300").is_some());
        assert!(parse("CST7CDT,M13.1.0,M10.5.0").is_none());
    }
}
