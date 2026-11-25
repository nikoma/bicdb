//! Date and timestamp text input as PostgreSQL decodes it: the field
//! splitting of `ParseDateTime`, the field rules of `DecodeDateTime`,
//! `DecodeDate` and `DecodeNumber` (with the session's DateStyle field
//! order), and `ValidateDate`, with PostgreSQL's error classes.

use crate::PgCanonicalValueError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldKind {
    Number,
    Date,
    Time,
    Str,
    Tz,
    Special,
}

/// Errors of the decoder, mapped to PostgreSQL's DTERR classes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodeError {
    BadFormat,
    FieldOverflow,
    /// Month or day out of range: PostgreSQL adds a DateStyle hint.
    MdFieldOverflow,
    TzDispOverflow,
    /// An unknown time zone name (lowercased, as PostgreSQL reports it).
    BadTimezone(String),
}

impl DecodeError {
    /// The canonical-value error for input of type `kind`.
    pub(crate) fn into_canonical(self, kind: &'static str, value: &str) -> PgCanonicalValueError {
        match self {
            Self::BadFormat => PgCanonicalValueError::InvalidTemporal {
                kind,
                value: value.to_string(),
            },
            Self::FieldOverflow => PgCanonicalValueError::TemporalFieldOverflow(kind),
            Self::MdFieldOverflow => PgCanonicalValueError::TemporalFieldOverflow(match kind {
                "date" => "date/md",
                "timestamptz" => "timestamptz/md",
                _ => "timestamp/md",
            }),
            Self::TzDispOverflow => PgCanonicalValueError::InvalidTimezoneOffset(0),
            Self::BadTimezone(name) => PgCanonicalValueError::InvalidTemporal {
                kind: "timezone",
                value: name,
            },
        }
    }
}

const YEAR: u32 = 1 << 0;
const MONTH: u32 = 1 << 1;
const DAY: u32 = 1 << 2;
const DOY: u32 = 1 << 3;
const TIME: u32 = 1 << 4;
const TZ: u32 = 1 << 5;
const DTZMOD: u32 = 1 << 6;
const DATE_M: u32 = YEAR | MONTH | DAY;

/// The decoded fields of a date/time string.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Decoded {
    /// Astronomical year (1 BC is 0).
    pub(crate) year: i64,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) micros: u32,
    /// Explicit UTC offset, seconds east.
    pub(crate) offset: Option<i32>,
    /// A full zone name or POSIX zone resolved at the local time.
    pub(crate) zone_name: Option<String>,
    /// An abbreviation the session zone uses (`LMT`), resolved at the
    /// local time: (zone, abbreviation).
    pub(crate) zone_abbreviation: Option<(String, String)>,
    pub(crate) has_time: bool,
}

fn split_fields(input: &str) -> Result<Vec<(FieldKind, String)>, DecodeError> {
    let bytes = input.as_bytes();
    let mut fields = Vec::new();
    let mut index = 0;
    let lower = |byte: u8| byte.to_ascii_lowercase() as char;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        let mut field = String::new();
        let kind;
        if byte.is_ascii_digit() {
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                field.push(bytes[index] as char);
                index += 1;
            }
            if bytes.get(index) == Some(&b':') {
                kind = FieldKind::Time;
                while index < bytes.len()
                    && (bytes[index].is_ascii_digit() || matches!(bytes[index], b':' | b'.'))
                {
                    field.push(bytes[index] as char);
                    index += 1;
                }
            } else if let Some(&delim @ (b'-' | b'/' | b'.')) = bytes.get(index) {
                field.push(delim as char);
                index += 1;
                if bytes.get(index).is_some_and(u8::is_ascii_digit) {
                    let mut local_kind = if delim == b'.' {
                        FieldKind::Number
                    } else {
                        FieldKind::Date
                    };
                    while index < bytes.len() && bytes[index].is_ascii_digit() {
                        field.push(bytes[index] as char);
                        index += 1;
                    }
                    if bytes.get(index) == Some(&delim) {
                        local_kind = FieldKind::Date;
                        field.push(delim as char);
                        index += 1;
                        while index < bytes.len()
                            && (bytes[index].is_ascii_digit() || bytes[index] == delim)
                        {
                            field.push(bytes[index] as char);
                            index += 1;
                        }
                    }
                    kind = local_kind;
                } else {
                    kind = FieldKind::Date;
                    while index < bytes.len()
                        && (bytes[index].is_ascii_alphanumeric() || bytes[index] == delim)
                    {
                        field.push(lower(bytes[index]));
                        index += 1;
                    }
                }
            } else {
                kind = FieldKind::Number;
            }
        } else if byte == b'.' {
            field.push('.');
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                field.push(bytes[index] as char);
                index += 1;
            }
            kind = FieldKind::Number;
        } else if byte.is_ascii_alphabetic() {
            while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                field.push(lower(bytes[index]));
                index += 1;
            }
            let next = bytes.get(index).copied();
            let is_date = match next {
                Some(b'-' | b'/' | b'.') => true,
                Some(b'+') => !is_core_token(&field),
                Some(byte) if byte.is_ascii_digit() => !is_core_token(&field),
                _ => false,
            };
            if is_date {
                kind = FieldKind::Date;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric()
                        || matches!(bytes[index], b'+' | b'-' | b'/' | b'_' | b'.' | b':'))
                {
                    field.push(lower(bytes[index]));
                    index += 1;
                }
            } else {
                kind = FieldKind::Str;
            }
        } else if byte == b'+' || byte == b'-' {
            field.push(byte as char);
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if bytes.get(index).is_some_and(u8::is_ascii_digit) {
                kind = FieldKind::Tz;
                while index < bytes.len()
                    && (bytes[index].is_ascii_digit() || matches!(bytes[index], b':' | b'.'))
                {
                    field.push(bytes[index] as char);
                    index += 1;
                }
            } else if bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
                kind = FieldKind::Special;
                while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                    field.push(lower(bytes[index]));
                    index += 1;
                }
            } else {
                return Err(DecodeError::BadFormat);
            }
        } else if byte.is_ascii_punctuation() {
            index += 1;
            continue;
        } else {
            return Err(DecodeError::BadFormat);
        }
        fields.push((kind, field));
    }
    Ok(fields)
}

const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];
const DAYS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

fn month_of(word: &str) -> Option<u32> {
    if word == "sept" {
        return Some(9);
    }
    MONTHS
        .iter()
        .position(|month| *month == word || (word.len() == 3 && month.starts_with(word)))
        .map(|position| position as u32 + 1)
}

fn is_day_name(word: &str) -> bool {
    matches!(word, "tues" | "wednes" | "thur" | "thurs")
        || DAYS
            .iter()
            .any(|day| *day == word || (word.len() == 3 && day.starts_with(word)))
}

/// Keywords of PostgreSQL's core date token table (not zone names).
fn is_core_token(word: &str) -> bool {
    month_of(word).is_some()
        || is_day_name(word)
        || matches!(
            word,
            "am" | "pm"
                | "ad"
                | "bc"
                | "allballs"
                | "epoch"
                | "infinity"
                | "j"
                | "julian"
                | "jd"
                | "now"
                | "on"
                | "t"
                | "today"
                | "tomorrow"
                | "yesterday"
                | "z"
                | "zulu"
                | "y"
                | "m"
                | "d"
                | "h"
                | "s"
        )
}

fn parse_int(text: &str) -> Result<i64, DecodeError> {
    text.parse::<i64>().map_err(|_| DecodeError::FieldOverflow)
}

fn fraction_micros(text: &str) -> Result<u32, DecodeError> {
    // `text` starts with '.'
    let digits = &text[1..];
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DecodeError::BadFormat);
    }
    let value = format!("0.{digits}")
        .parse::<f64>()
        .map_err(|_| DecodeError::BadFormat)?;
    Ok((value * 1_000_000.0).round() as u32)
}

struct State {
    fmask: u32,
    tm: Decoded,
    have_text_month: bool,
    is2digits: bool,
    bc: bool,
    /// 0 = 24-hour, 1 = AM, 2 = PM.
    meridian: u8,
    doy: i64,
    julian: bool,
    pending_julian: bool,
    dst: bool,
    order: DateOrder,
}

impl State {
    fn set(&mut self, mask: u32) -> Result<(), DecodeError> {
        if self.fmask & mask != 0 {
            return Err(DecodeError::BadFormat);
        }
        self.fmask |= mask;
        Ok(())
    }

    fn decode_number(&mut self, text: &str) -> Result<(), DecodeError> {
        let digits = text.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(DecodeError::BadFormat);
        }
        let flen = text.len();
        let value = parse_int(&text[..digits])?;
        let rest = &text[digits..];
        if rest.starts_with('.') {
            if digits > 2 {
                return self.decode_number_field(text, self.fmask | DATE_M);
            }
            self.tm.micros = fraction_micros(rest)?;
        } else if !rest.is_empty() {
            return Err(DecodeError::BadFormat);
        }
        if flen == 3 && self.fmask & DATE_M == YEAR && (1..=366).contains(&value) {
            self.set(DOY | MONTH | DAY)?;
            self.doy = value;
            return Ok(());
        }
        let as_i32 = || i32::try_from(value).map_err(|_| DecodeError::FieldOverflow);
        let mask = match self.fmask & DATE_M {
            0 => {
                if flen >= 3 || self.order == DateOrder::Ymd {
                    self.tm.year = value;
                    YEAR
                } else if self.order == DateOrder::Dmy {
                    self.tm.day = as_i32()? as u32;
                    DAY
                } else {
                    self.tm.month = as_i32()? as u32;
                    MONTH
                }
            }
            YEAR => {
                self.tm.month = as_i32()? as u32;
                MONTH
            }
            MONTH => {
                if self.have_text_month && (flen >= 3 || self.order == DateOrder::Ymd) {
                    self.tm.year = value;
                    YEAR
                } else {
                    self.tm.day = as_i32()? as u32;
                    DAY
                }
            }
            mask if mask == YEAR | MONTH => {
                if self.have_text_month && flen >= 3 && self.is2digits {
                    self.tm.day = self.tm.year as u32;
                    self.tm.year = value;
                    self.is2digits = false;
                } else {
                    self.tm.day = as_i32()? as u32;
                }
                DAY
            }
            DAY => {
                self.tm.month = as_i32()? as u32;
                MONTH
            }
            mask if mask == MONTH | DAY => {
                self.tm.year = value;
                YEAR
            }
            DATE_M => return self.decode_number_field(text, self.fmask),
            _ => return Err(DecodeError::BadFormat),
        };
        self.set(mask)?;
        if mask == YEAR {
            self.is2digits = flen <= 2;
        }
        Ok(())
    }

    /// Run-together digits: `YYYYMMDD`, `YYMMDD`, `hhmmss`, `hhmm`.
    fn decode_number_field(&mut self, text: &str, fmask: u32) -> Result<(), DecodeError> {
        let (digits, fraction) = match text.find('.') {
            Some(at) => (&text[..at], Some(&text[at..])),
            None => (text, None),
        };
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DecodeError::BadFormat);
        }
        let len = digits.len();
        if let Some(fraction) = fraction {
            self.tm.micros = if fraction.len() == 1 {
                0
            } else {
                fraction_micros(fraction)?
            };
        } else if fmask & DATE_M != DATE_M && len >= 6 {
            self.set(DATE_M)?;
            self.tm.day = digits[len - 2..]
                .parse()
                .map_err(|_| DecodeError::BadFormat)?;
            self.tm.month = digits[len - 4..len - 2]
                .parse()
                .map_err(|_| DecodeError::BadFormat)?;
            self.tm.year = parse_int(&digits[..len - 4])?;
            if len - 4 == 2 {
                self.is2digits = true;
            }
            return Ok(());
        }
        if fmask & TIME != TIME && (len == 6 || len == 4) {
            self.set(TIME)?;
            self.tm.hour = digits[..2].parse().map_err(|_| DecodeError::BadFormat)?;
            self.tm.minute = digits[2..4].parse().map_err(|_| DecodeError::BadFormat)?;
            self.tm.second = if len == 6 {
                digits[4..].parse().map_err(|_| DecodeError::BadFormat)?
            } else {
                0
            };
            self.tm.has_time = true;
            return Ok(());
        }
        Err(DecodeError::BadFormat)
    }

    /// `DecodeDate`: one field with embedded separators.
    fn decode_date(&mut self, text: &str) -> Result<(), DecodeError> {
        let mut parts: Vec<&str> = Vec::new();
        let bytes = text.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            while index < bytes.len() && !bytes[index].is_ascii_alphanumeric() {
                index += 1;
            }
            if index >= bytes.len() {
                return Err(DecodeError::BadFormat);
            }
            let start = index;
            if bytes[index].is_ascii_digit() {
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
            } else {
                while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                    index += 1;
                }
            }
            parts.push(&text[start..index]);
            if index < bytes.len() {
                index += 1;
            }
        }
        let mut numeric = Vec::new();
        for part in parts {
            if part.as_bytes()[0].is_ascii_alphabetic() {
                if let Some(month) = month_of(part) {
                    self.set(MONTH)?;
                    self.tm.month = month;
                    self.have_text_month = true;
                } else if part == "bc" || part == "ad" {
                    self.bc = part == "bc";
                } else if is_day_name(part) {
                } else {
                    return Err(DecodeError::BadFormat);
                }
            } else {
                numeric.push(part);
            }
        }
        for part in numeric {
            self.decode_number(part)?;
        }
        if self.fmask & DATE_M != DATE_M {
            return Err(DecodeError::BadFormat);
        }
        Ok(())
    }

    /// `DecodeTime`: `hh:mm[:ss[.f]]` or `mm:ss.f`.
    fn decode_time(&mut self, text: &str) -> Result<(), DecodeError> {
        self.set(TIME)?;
        let mut parts = text.splitn(3, ':');
        let hour = parse_int(parts.next().unwrap_or(""))?;
        let minute_text = parts.next().ok_or(DecodeError::BadFormat)?;
        let (hour, minute, second, micros) = match parts.next() {
            Some(second_text) => {
                let minute = parse_int(minute_text)?;
                let (second, micros) = match second_text.find('.') {
                    Some(at) => (
                        parse_int(&second_text[..at])?,
                        fraction_micros(&second_text[at..])?,
                    ),
                    None => (parse_int(second_text)?, 0),
                };
                (hour, minute, second, micros)
            }
            None => match minute_text.find('.') {
                // mm:ss.sss
                Some(at) => (
                    0,
                    hour,
                    parse_int(&minute_text[..at])?,
                    fraction_micros(&minute_text[at..])?,
                ),
                None => (hour, parse_int(minute_text)?, 0, 0),
            },
        };
        if minute > 59 || second > 60 || micros > 1_000_000 {
            return Err(DecodeError::FieldOverflow);
        }
        self.tm.hour = u32::try_from(hour).map_err(|_| DecodeError::FieldOverflow)?;
        self.tm.minute = minute as u32;
        self.tm.second = second as u32;
        self.tm.micros = micros;
        self.tm.has_time = true;
        Ok(())
    }

    fn set_zone_name(&mut self, name: &str) -> Result<(), DecodeError> {
        // An abbreviation the session zone uses takes precedence over the
        // abbreviation table, with the meaning it has in that zone.
        let session = crate::eval::current_timezone_name();
        if name.bytes().all(|byte| byte.is_ascii_alphabetic())
            && zone_uses_abbreviation(&session, name)
        {
            self.set(TZ)?;
            self.tm.zone_abbreviation = Some((session, name.to_string()));
            return Ok(());
        }
        if let Some(offset) = crate::eval::posix_or_abbreviation_offset(name) {
            self.set(TZ)?;
            self.tm.offset = Some(offset);
            return Ok(());
        }
        let Some(zone) = resolve_zone_name(name) else {
            return Err(DecodeError::BadTimezone(name.to_string()));
        };
        self.set(TZ)?;
        self.tm.zone_name = Some(zone);
        Ok(())
    }
}

/// The periods of a zone: (first UTC second, offset east, abbreviation),
/// sampled weekly from 1800 to 2100 after the zone's earliest (local mean
/// time) period.
fn zone_periods(zone: &str) -> Option<std::sync::Arc<Vec<(i64, i32, String)>>> {
    use chrono::offset::{Offset, TimeZone};
    use chrono_tz::OffsetName;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Vec<(i64, i32, String)>>>>> = OnceLock::new();
    let tz = zone.parse::<chrono_tz::Tz>().ok()?;
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(periods) = cache.lock().ok()?.get(zone) {
        return Some(periods.clone());
    }
    const YEAR_1000: i64 = -30_610_224_000;
    const YEAR_1800: i64 = -5_364_662_400;
    const YEAR_2100: i64 = 4_102_444_800;
    let mut periods: Vec<(i64, i32, String)> = Vec::new();
    let mut sample = |utc: i64| {
        let Some(at) = chrono::DateTime::from_timestamp(utc, 0) else {
            return;
        };
        let offset = tz.offset_from_utc_datetime(&at.naive_utc());
        let east = offset.fix().local_minus_utc();
        let name = offset.abbreviation().unwrap_or_default().to_string();
        if periods
            .last()
            .is_none_or(|(_, last_east, last_name)| *last_east != east || *last_name != name)
        {
            periods.push((utc, east, name));
        }
    };
    sample(YEAR_1000);
    let mut utc = YEAR_1800;
    while utc < YEAR_2100 {
        sample(utc);
        utc += 7 * 86_400;
    }
    let periods = Arc::new(periods);
    cache.lock().ok()?.insert(zone.to_string(), periods.clone());
    Some(periods)
}

/// Whether the zone ever uses the abbreviation (ignoring case).
pub(crate) fn zone_uses_abbreviation(zone: &str, abbreviation: &str) -> bool {
    zone_periods(zone).is_some_and(|periods| {
        periods
            .iter()
            .any(|(_, _, name)| name.eq_ignore_ascii_case(abbreviation))
    })
}

/// `pg_interpret_timezone_abbrev`: the abbreviation's offset in the zone's
/// latest period using it at or before `utc`, else its earliest later one.
pub(crate) fn zone_abbreviation_offset(zone: &str, abbreviation: &str, utc: i64) -> Option<i32> {
    let periods = zone_periods(zone)?;
    let matching = || {
        periods
            .iter()
            .filter(|(_, _, name)| name.eq_ignore_ascii_case(abbreviation))
    };
    matching()
        .filter(|(start, _, _)| *start <= utc)
        .last()
        .or_else(|| matching().next())
        .map(|(_, east, _)| *east)
}

/// A zone name matched case-insensitively against the zone database.
fn resolve_zone_name(name: &str) -> Option<String> {
    if let Ok(zone) = name.parse::<chrono_tz::Tz>() {
        return Some(zone.name().to_string());
    }
    chrono_tz::TZ_VARIANTS
        .iter()
        .find(|zone| zone.name().eq_ignore_ascii_case(name))
        .map(|zone| zone.name().to_string())
}

/// `DecodeTimezone`: `+hh`, `+hhmm`, `+hh:mm[:ss]`; seconds east.
fn decode_timezone(text: &str) -> Result<i32, DecodeError> {
    let negative = text.starts_with('-');
    let body = &text[1..];
    let digits = body.bytes().take_while(u8::is_ascii_digit).count();
    let mut hours = parse_int(&body[..digits])?;
    let mut rest = &body[digits..];
    let mut minutes = 0;
    let mut seconds = 0;
    if let Some(after) = rest.strip_prefix(':') {
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        minutes = parse_int(&after[..digits])?;
        rest = &after[digits..];
        if let Some(after) = rest.strip_prefix(':') {
            let digits = after.bytes().take_while(u8::is_ascii_digit).count();
            seconds = parse_int(&after[..digits])?;
            rest = &after[digits..];
        }
    } else if rest.is_empty() && text.len() > 3 {
        minutes = hours % 100;
        hours /= 100;
    }
    if !(0..=15).contains(&hours) || !(0..60).contains(&minutes) || !(0..60).contains(&seconds) {
        return Err(DecodeError::TzDispOverflow);
    }
    if !rest.is_empty() {
        return Err(DecodeError::BadFormat);
    }
    let offset = ((hours * 60 + minutes) * 60 + seconds) as i32;
    Ok(if negative { -offset } else { offset })
}

/// Days from 1970-01-01 of a proleptic Gregorian date.
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (i64::from(month) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Decode a date or timestamp string. `None` for the special words this
/// decoder leaves to its caller (`epoch`, `infinity`, ...).
pub(crate) fn decode(input: &str, order: DateOrder) -> Result<Decoded, DecodeError> {
    let fields = split_fields(input)?;
    let mut state = State {
        fmask: 0,
        tm: Decoded::default(),
        have_text_month: false,
        is2digits: false,
        bc: false,
        meridian: 0,
        doy: 0,
        julian: false,
        pending_julian: false,
        dst: false,
        order,
    };
    for (kind, text) in &fields {
        match kind {
            FieldKind::Date => {
                if state.fmask & DATE_M == DATE_M {
                    // An unknown plain word is a syntax error; only a zone
                    // name (`Area/City`) reports an unrecognized time zone.
                    let word = text.bytes().all(|byte| byte.is_ascii_alphabetic());
                    state.set_zone_name(text).map_err(|error| match error {
                        DecodeError::BadTimezone(_) if word => DecodeError::BadFormat,
                        other => other,
                    })?;
                } else {
                    state.decode_date(text)?;
                }
            }
            FieldKind::Time => state.decode_time(text)?,
            FieldKind::Tz => {
                let offset = decode_timezone(text)?;
                state.set(TZ)?;
                state.tm.offset = Some(offset);
            }
            FieldKind::Special => return Err(DecodeError::BadFormat),
            FieldKind::Number => {
                if state.pending_julian {
                    state.pending_julian = false;
                    let julian = parse_int(text)?;
                    state.set(DATE_M)?;
                    let (year, month, day) = civil_from_days(julian - 2_440_588);
                    state.tm.year = year;
                    state.tm.month = month;
                    state.tm.day = day;
                    state.julian = true;
                    continue;
                }
                let has_dot = text.contains('.');
                if has_dot && state.fmask & DATE_M == 0 {
                    state.decode_date(text)?;
                } else if has_dot && text.find('.').unwrap_or(0) > 2 {
                    state.decode_number_field(text, state.fmask)?;
                } else if text.len() >= 6 && (state.fmask & DATE_M == 0 || state.fmask & TIME == 0)
                {
                    state.decode_number_field(text, state.fmask)?;
                } else {
                    state.decode_number(text)?;
                }
            }
            FieldKind::Str => {
                let word = text.as_str();
                if let Some(month) = month_of(word) {
                    if state.fmask & MONTH != 0
                        && !state.have_text_month
                        && state.fmask & DAY == 0
                        && (1..=31).contains(&state.tm.month)
                    {
                        state.tm.day = state.tm.month;
                        state.fmask |= DAY;
                        state.fmask &= !MONTH;
                    }
                    state.set(MONTH)?;
                    state.have_text_month = true;
                    state.tm.month = month;
                } else if is_day_name(word) || word == "on" {
                } else if word == "am" || word == "pm" {
                    if state.meridian != 0 {
                        return Err(DecodeError::BadFormat);
                    }
                    state.meridian = if word == "am" { 1 } else { 2 };
                } else if word == "ad" || word == "bc" {
                    state.bc = word == "bc";
                } else if word == "t" {
                } else if matches!(word, "j" | "jd" | "julian") {
                    state.pending_julian = true;
                } else if word == "dst" {
                    state.set(DTZMOD)?;
                    state.dst = true;
                } else if matches!(word, "today" | "tomorrow" | "yesterday") {
                    state.set(DATE_M)?;
                    let local = crate::eval::special_local_now_micros();
                    let days = local.div_euclid(86_400_000_000)
                        + 10_957
                        + match word {
                            "tomorrow" => 1,
                            "yesterday" => -1,
                            _ => 0,
                        };
                    let (year, month, day) = civil_from_days(days);
                    state.tm.year = year;
                    state.tm.month = month;
                    state.tm.day = day;
                    state.julian = true;
                } else if word == "allballs" {
                    state.set(TIME)?;
                    state.tm.has_time = true;
                } else {
                    // An unknown word is a syntax error unless it names a zone.
                    state
                        .set_zone_name(word)
                        .map_err(|_| DecodeError::BadFormat)?;
                }
            }
        }
    }
    validate(&mut state)?;
    let mut tm = state.tm;
    if state.meridian != 0 && tm.hour > 12 {
        return Err(DecodeError::FieldOverflow);
    }
    if state.meridian == 1 && tm.hour == 12 {
        tm.hour = 0;
    } else if state.meridian == 2 && tm.hour != 12 {
        tm.hour += 12;
    }
    if tm.hour > 24 || (tm.hour == 24 && (tm.minute > 0 || tm.second > 0 || tm.micros > 0)) {
        return Err(DecodeError::FieldOverflow);
    }
    if state.fmask & DATE_M != DATE_M {
        return Err(DecodeError::BadFormat);
    }
    if state.dst {
        match tm.offset.as_mut() {
            Some(offset) => *offset += 3_600,
            None => return Err(DecodeError::BadFormat),
        }
    }
    Ok(tm)
}

/// `ValidateDate`.
fn validate(state: &mut State) -> Result<(), DecodeError> {
    let fmask = state.fmask;
    let tm = &mut state.tm;
    if fmask & YEAR != 0 {
        if state.julian {
        } else if state.bc {
            if tm.year <= 0 {
                return Err(DecodeError::FieldOverflow);
            }
            tm.year = -(tm.year - 1);
        } else if state.is2digits {
            if tm.year < 0 {
                return Err(DecodeError::FieldOverflow);
            }
            if tm.year < 70 {
                tm.year += 2000;
            } else if tm.year < 100 {
                tm.year += 1900;
            }
        } else if tm.year <= 0 {
            return Err(DecodeError::FieldOverflow);
        }
    }
    if fmask & DOY != 0 {
        let (year, month, day) = civil_from_days(days_from_civil(tm.year, 1, 1) + state.doy - 1);
        tm.year = year;
        tm.month = month;
        tm.day = day;
    }
    if fmask & MONTH != 0 && !(1..=12).contains(&tm.month) {
        return Err(DecodeError::MdFieldOverflow);
    }
    if fmask & DAY != 0 && !(1..=31).contains(&tm.day) {
        return Err(DecodeError::MdFieldOverflow);
    }
    if fmask & DATE_M == DATE_M {
        let days = match tm.month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if is_leap(tm.year) => 29,
            _ => 28,
        };
        if tm.day > days {
            return Err(DecodeError::FieldOverflow);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd(input: &str, order: DateOrder) -> Result<(i64, u32, u32), DecodeError> {
        decode(input, order).map(|tm| (tm.year, tm.month, tm.day))
    }

    #[test]
    fn date_forms_follow_postgresql() {
        use DateOrder::*;
        assert_eq!(ymd("January 8, 1999", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("1/8/1999", Ymd), Err(DecodeError::MdFieldOverflow));
        assert_eq!(ymd("01/02/03", Ymd), Ok((2001, 2, 3)));
        assert_eq!(ymd("01/02/03", Mdy), Ok((2003, 1, 2)));
        assert_eq!(ymd("01/02/03", Dmy), Ok((2003, 2, 1)));
        assert_eq!(ymd("19990108", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("990108", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("1999.008", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("J2451187", Ymd), Ok((1999, 1, 8)));
        assert_eq!(
            ymd("January 8, 99 BC", Ymd),
            Err(DecodeError::MdFieldOverflow)
        );
        assert_eq!(ymd("99-Jan-08", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("08-Jan-99", Ymd), Err(DecodeError::MdFieldOverflow));
        assert_eq!(ymd("08-Jan-1999", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("Jan-08-99", Ymd), Err(DecodeError::MdFieldOverflow));
        assert_eq!(ymd("99-08-Jan", Ymd), Err(DecodeError::BadFormat));
        assert_eq!(ymd("99 Jan 08", Ymd), Ok((1999, 1, 8)));
        assert_eq!(ymd("08 Jan 99", Ymd), Err(DecodeError::MdFieldOverflow));
        assert_eq!(ymd("Jan 08 99", Mdy), Ok((1999, 1, 8)));
        assert_eq!(ymd("January 8, 99 BC", Mdy), Ok((-98, 1, 8)));
        let tm = decode("Mon Feb 10 17:32:01.4 1997 PST", Mdy).unwrap();
        assert_eq!(
            (tm.year, tm.month, tm.day, tm.hour, tm.micros, tm.offset),
            (1997, 2, 10, 17, 400_000, Some(-8 * 3600))
        );
        let tm = decode("Feb 10 5:32PM 1997", Mdy).unwrap();
        assert_eq!((tm.hour, tm.minute), (17, 32));
        let tm = decode("19970210 173201 -0800", Mdy).unwrap();
        assert_eq!(
            (tm.day, tm.hour, tm.second, tm.offset),
            (10, 17, 1, Some(-8 * 3600))
        );
        let tm = decode("1997-02-10 17:32:01 America/New_York", Mdy).unwrap();
        assert_eq!(tm.zone_name.as_deref(), Some("America/New_York"));
        assert_eq!(
            decode("19970710 173201 America/Does_not_exist", Mdy),
            Err(DecodeError::BadTimezone("america/does_not_exist".into()))
        );
        let tm = decode("2000-03-15 08:14:01 GMT+8", Mdy).unwrap();
        assert_eq!(tm.offset, Some(-8 * 3600));
    }
}

/// `YYYY-MM-DD` (four or more year digits) with an optional ` BC`.
fn is_plain_iso_date(value: &str) -> bool {
    let value = value.strip_suffix(" BC").unwrap_or(value);
    let bytes = value.as_bytes();
    let len = bytes.len();
    len >= 10
        && bytes[len - 3] == b'-'
        && bytes[len - 6] == b'-'
        && bytes[..len - 6].iter().all(u8::is_ascii_digit)
        && bytes[len - 5..len - 3].iter().all(u8::is_ascii_digit)
        && bytes[len - 2..].iter().all(u8::is_ascii_digit)
}

/// An ISO 8601 timestamp the canonical fast path parses:
/// `YYYY-MM-DD[( |T)hh:mm[:ss[.f]]][Z|±hh[[:]mm[:ss]]][ BC]`.
pub(crate) fn is_plain_iso_timestamp(value: &str) -> bool {
    let value = value.strip_suffix(" BC").unwrap_or(value);
    let Some(split) = value.find([' ', 'T', 't']) else {
        return is_plain_iso_date(value);
    };
    if !is_plain_iso_date(&value[..split]) {
        return false;
    }
    let rest = &value[split + 1..];
    let clock_end = rest
        .find(|ch: char| !(ch.is_ascii_digit() || ch == ':' || ch == '.'))
        .unwrap_or(rest.len());
    let clock = &rest[..clock_end];
    if clock.len() < 5 || clock.as_bytes()[2] != b':' {
        return false;
    }
    let zone = &rest[clock_end..];
    zone.is_empty()
        || zone == "Z"
        || zone == "z"
        || (zone.starts_with(['+', '-'])
            && zone.len() >= 3
            && zone[1..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b':'))
}

/// A date from PostgreSQL's date/time decoder.
pub(crate) fn decode_date(value: &str) -> Result<crate::PgDate, PgCanonicalValueError> {
    let tm = decode(value, crate::eval::current_date_order())
        .map_err(|error| error.into_canonical("date", value))?;
    let year =
        i32::try_from(tm.year).map_err(|_| PgCanonicalValueError::TemporalOverflow("date"))?;
    crate::PgDate::from_ymd(year, tm.month, tm.day)
}

/// A timestamp from PostgreSQL's date/time decoder: local time for
/// `timestamp` (a zone is checked and ignored), UTC for `timestamptz` with
/// the explicit offset, the named zone, or `default_zone`.
pub(crate) fn decode_timestamp(
    value: &str,
    with_timezone: bool,
    default_zone: &str,
) -> Result<crate::PgTimestamp, PgCanonicalValueError> {
    let kind = if with_timezone {
        "timestamptz"
    } else {
        "timestamp"
    };
    let tm = decode(value, crate::eval::current_date_order())
        .map_err(|error| error.into_canonical(kind, value))?;
    let days = days_from_civil(tm.year, tm.month, tm.day) - 10_957;
    let local = days
        .checked_mul(86_400_000_000)
        .and_then(|base| {
            base.checked_add(
                ((i64::from(tm.hour) * 60 + i64::from(tm.minute)) * 60 + i64::from(tm.second))
                    * 1_000_000
                    + i64::from(tm.micros),
            )
        })
        .ok_or(PgCanonicalValueError::TemporalOverflow(kind))?;
    // PostgreSQL's range: 4714-11-24 BC through 294276 AD.
    if !(-211_813_488_000_000_000..9_223_371_331_200_000_000).contains(&local) {
        return Err(PgCanonicalValueError::TemporalOverflow(kind));
    }
    if !with_timezone {
        return Ok(crate::PgTimestamp::Finite(local));
    }
    let offset = match (tm.offset, &tm.zone_name) {
        (Some(offset), _) => offset,
        (None, _) if tm.zone_abbreviation.is_some() => {
            let (zone, abbreviation) = tm.zone_abbreviation.as_ref().expect("checked");
            zone_abbreviation_offset(
                zone,
                abbreviation,
                local.div_euclid(1_000_000) + 946_684_800,
            )
            .unwrap_or(0)
        }
        (None, Some(zone)) => {
            crate::eval::local_timestamp_offset_seconds(crate::PgTimestamp::Finite(local), zone)
                .unwrap_or(0)
        }
        (None, None) => crate::eval::local_timestamp_offset_seconds(
            crate::PgTimestamp::Finite(local),
            default_zone,
        )
        .unwrap_or(0),
    };
    local
        .checked_sub(i64::from(offset) * 1_000_000)
        .map(crate::PgTimestamp::Finite)
        .ok_or(PgCanonicalValueError::TemporalOverflow(kind))
}
