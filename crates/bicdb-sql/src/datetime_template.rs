//! `to_timestamp(text, text)` and `to_date(text, text)`: the template
//! parser of PostgreSQL's formatting functions, with its field rules, its
//! separator and whitespace skipping, and its error texts.

use crate::eval::{
    current_timezone_name, local_timestamp_offset_seconds, render_timestamptz,
    timezone_abbreviation_offset,
};
use crate::{PgTimestamp, Result, SqlError};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    None,
    Gregorian,
    IsoWeek,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Key {
    AmDots,
    Am,
    Hh12,
    Hh24,
    Mi,
    Ss,
    Ms,
    Ff(u8),
    Us,
    Ssss,
    Tz,
    Tzh,
    Tzm,
    Of,
    AdDots,
    Ad,
    Month,
    Mon,
    Mm,
    Day,
    Dy,
    Ddd,
    Iddd,
    Dd,
    D,
    Id,
    Ww,
    Iw,
    Q,
    Cc,
    YCommaYyy,
    Yyyy,
    Yyy,
    Yy,
    Y,
    Rm,
    W,
    J,
    Fx,
}

struct KeyInfo {
    name: &'static str,
    key: Key,
    digit: bool,
    mode: Mode,
}

/// Keywords, longest first where one is a prefix of another (the order of
/// PostgreSQL's keyword table).
const KEYS: &[KeyInfo] = &[
    k("A.D.", Key::AdDots, false, Mode::None),
    k("A.M.", Key::AmDots, false, Mode::None),
    k("AD", Key::Ad, false, Mode::None),
    k("AM", Key::Am, false, Mode::None),
    k("B.C.", Key::AdDots, false, Mode::None),
    k("BC", Key::Ad, false, Mode::None),
    k("CC", Key::Cc, true, Mode::None),
    k("DAY", Key::Day, false, Mode::None),
    k("DDD", Key::Ddd, true, Mode::Gregorian),
    k("DD", Key::Dd, true, Mode::Gregorian),
    k("DY", Key::Dy, false, Mode::None),
    k("D", Key::D, true, Mode::Gregorian),
    k("FF1", Key::Ff(1), true, Mode::None),
    k("FF2", Key::Ff(2), true, Mode::None),
    k("FF3", Key::Ff(3), true, Mode::None),
    k("FF4", Key::Ff(4), true, Mode::None),
    k("FF5", Key::Ff(5), true, Mode::None),
    k("FF6", Key::Ff(6), true, Mode::None),
    k("FX", Key::Fx, false, Mode::None),
    k("HH24", Key::Hh24, true, Mode::None),
    k("HH12", Key::Hh12, true, Mode::None),
    k("HH", Key::Hh12, true, Mode::None),
    k("IDDD", Key::Iddd, true, Mode::IsoWeek),
    k("ID", Key::Id, true, Mode::IsoWeek),
    k("IW", Key::Iw, true, Mode::IsoWeek),
    k("IYYY", Key::Yyyy, true, Mode::IsoWeek),
    k("IYY", Key::Yyy, true, Mode::IsoWeek),
    k("IY", Key::Yy, true, Mode::IsoWeek),
    k("I", Key::Y, true, Mode::IsoWeek),
    k("J", Key::J, true, Mode::None),
    k("MI", Key::Mi, true, Mode::None),
    k("MM", Key::Mm, true, Mode::Gregorian),
    k("MONTH", Key::Month, false, Mode::Gregorian),
    k("MON", Key::Mon, false, Mode::Gregorian),
    k("MS", Key::Ms, true, Mode::None),
    k("OF", Key::Of, false, Mode::None),
    k("P.M.", Key::AmDots, false, Mode::None),
    k("PM", Key::Am, false, Mode::None),
    k("Q", Key::Q, true, Mode::None),
    k("RM", Key::Rm, false, Mode::Gregorian),
    k("SSSSS", Key::Ssss, true, Mode::None),
    k("SSSS", Key::Ssss, true, Mode::None),
    k("SS", Key::Ss, true, Mode::None),
    k("TZH", Key::Tzh, false, Mode::None),
    k("TZM", Key::Tzm, true, Mode::None),
    k("TZ", Key::Tz, false, Mode::None),
    k("US", Key::Us, true, Mode::None),
    k("WW", Key::Ww, true, Mode::Gregorian),
    k("W", Key::W, true, Mode::Gregorian),
    k("Y,YYY", Key::YCommaYyy, true, Mode::Gregorian),
    k("YYYY", Key::Yyyy, true, Mode::Gregorian),
    k("YYY", Key::Yyy, true, Mode::Gregorian),
    k("YY", Key::Yy, true, Mode::Gregorian),
    k("Y", Key::Y, true, Mode::Gregorian),
];

const fn k(name: &'static str, key: Key, digit: bool, mode: Mode) -> KeyInfo {
    KeyInfo {
        name,
        key,
        digit,
        mode,
    }
}

enum Node {
    Action {
        info: &'static KeyInfo,
        /// The keyword as spelled in the template, for error texts.
        spelled: String,
        fill: bool,
        th: bool,
    },
    Separator(char),
    Space(char),
    Char(char),
}

/// Characters a standard-mode (SQL/JSON) template may use outside keywords
/// and double quotes.
const STD_SEPARATORS: &str = "-./,':; ";

fn parse_template(format: &str, std: bool) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    let chars = format.char_indices().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        let (offset, ch) = chars[index];
        let rest = &format[offset..];
        // FM prefix
        let mut fill = false;
        let mut body = rest;
        if body.len() >= 2 && body[..2].eq_ignore_ascii_case("FM") {
            fill = true;
            body = &body[2..];
        }
        if let Some(info) = KEYS.iter().find(|info| {
            body.len() >= info.name.len()
                && body.is_char_boundary(info.name.len())
                && body[..info.name.len()].eq_ignore_ascii_case(info.name)
        }) {
            let consumed = (rest.len() - body.len()) + info.name.len();
            let spelled = body[..info.name.len()].to_string();
            let after = &rest[consumed..];
            let th = after.starts_with("TH") || after.starts_with("th");
            let consumed = consumed + if th { 2 } else { 0 };
            nodes.push(Node::Action {
                info,
                spelled,
                fill,
                th,
            });
            let end = offset + consumed;
            while index < chars.len() && chars[index].0 < end {
                index += 1;
            }
            continue;
        }
        if std && ch != '"' {
            if !STD_SEPARATORS.contains(ch) {
                return Err(format_error(
                    format!("invalid datetime format separator: \"{ch}\""),
                    None,
                    None,
                ));
            }
            nodes.push(if ch == ' ' {
                Node::Space(ch)
            } else {
                Node::Separator(ch)
            });
            index += 1;
            continue;
        }
        if ch == '"' {
            index += 1;
            while index < chars.len() && chars[index].1 != '"' {
                let mut inner = chars[index].1;
                if inner == '\\' && index + 1 < chars.len() {
                    index += 1;
                    inner = chars[index].1;
                }
                nodes.push(Node::Char(inner));
                index += 1;
            }
            index += 1;
            continue;
        }
        if ch == '\\' && index + 1 < chars.len() && chars[index + 1].1 == '"' {
            nodes.push(Node::Char('"'));
            index += 2;
            continue;
        }
        if is_separator_char(ch) {
            nodes.push(Node::Separator(ch));
        } else if ch.is_whitespace() {
            nodes.push(Node::Space(ch));
        } else {
            nodes.push(Node::Char(ch));
        }
        index += 1;
    }
    Ok(nodes)
}

fn is_separator_char(ch: char) -> bool {
    (ch as u32) > 0x20 && (ch as u32) < 0x7f && !ch.is_ascii_alphanumeric()
}

fn format_error(message: String, detail: Option<String>, hint: Option<&str>) -> SqlError {
    let mut message = message;
    if let Some(detail) = detail {
        message.push_str(&format!("\nDETAIL:  {detail}"));
    }
    if let Some(hint) = hint {
        message.push_str(&format!("\nHINT:  {hint}"));
    }
    SqlError::data_exception_public("22007", message, None)
}

const FIXED_WIDTH_HINT: &str =
    "If your source string is not fixed-width, try using the \"FM\" modifier.";

#[derive(Default)]
struct Fields {
    mode: Option<Mode>,
    hh: i32,
    pm: i32,
    twelve_hour: bool,
    mi: i32,
    ss: i32,
    ssss: i32,
    ms: i32,
    us: i32,
    ff: i32,
    year: i32,
    year_size: i32,
    bc: i32,
    cc: i32,
    mm: i32,
    dd: i32,
    ddd: i32,
    d: i32,
    ww: i32,
    w: i32,
    j: i32,
    tzsign: i32,
    tzh: i32,
    tzm: i32,
    /// A zone abbreviation's offset (seconds east).
    gmtoffset: Option<i32>,
    /// An abbreviation of the session zone, resolved at the parsed time.
    zone_abbreviation: Option<String>,
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    nodes: &'a [Node],
}

impl Parser<'_> {
    fn rest(&self) -> &str {
        &self.input[self.pos..]
    }

    fn skip_spaces(&mut self) -> usize {
        let start = self.pos;
        while self
            .rest()
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            self.pos += 1;
        }
        self.pos - start
    }

    /// Whether the node after `index` cannot start with a digit.
    fn next_is_separator(&self, index: usize) -> bool {
        if let Node::Action { th: true, .. } = &self.nodes[index] {
            return true;
        }
        match self.nodes.get(index + 1) {
            None => true,
            Some(Node::Action { info, .. }) => !info.digit,
            Some(Node::Char(ch)) => !ch.is_ascii_digit(),
            Some(_) => true,
        }
    }

    /// `from_char_parse_int_len`: the parsed value and the bytes consumed.
    fn parse_int(&mut self, index: usize, len: usize) -> Result<(i32, usize)> {
        let Node::Action { spelled, fill, .. } = &self.nodes[index] else {
            unreachable!("parse_int on an action node");
        };
        let init = self.pos;
        self.skip_spaces();
        let copy = self.rest().chars().take(len).collect::<String>();
        let value: i64;
        if *fill || self.next_is_separator(index) {
            // strtol from the initial position: whitespace, a sign, digits.
            let text = &self.input[init..];
            let trimmed = text.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
            let mut end = 0;
            let bytes = trimmed.as_bytes();
            if bytes
                .first()
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            {
                end = 1;
            }
            let digits_start = end;
            while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                end += 1;
            }
            if end == digits_start {
                self.pos = init;
                value = 0;
            } else {
                value = trimmed[..end].parse::<i64>().unwrap_or(i64::MAX);
                self.pos = init + (text.len() - trimmed.len()) + end;
            }
        } else {
            let used = copy.chars().count();
            if used < len {
                return Err(format_error(
                    format!("source string too short for \"{spelled}\" formatting field"),
                    Some(format!(
                        "Field requires {len} characters, but only {used} remain."
                    )),
                    Some(FIXED_WIDTH_HINT),
                ));
            }
            let bytes = copy.as_bytes();
            let mut end = 0;
            while bytes
                .get(end)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                end += 1;
            }
            if bytes
                .get(end)
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            {
                end += 1;
            }
            let digits_start = end;
            while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                end += 1;
            }
            let used = if end == digits_start { 0 } else { end };
            if used > 0 && used < len {
                return Err(format_error(
                    format!("invalid value \"{copy}\" for \"{spelled}\""),
                    Some(format!(
                        "Field requires {len} characters, but only {used} could be parsed."
                    )),
                    Some(FIXED_WIDTH_HINT),
                ));
            }
            value = if used == 0 {
                0
            } else {
                copy[..used].trim().parse::<i64>().unwrap_or(i64::MAX)
            };
            self.pos += used;
        }
        if self.pos == init {
            return Err(format_error(
                format!("invalid value \"{copy}\" for \"{spelled}\""),
                Some("Value must be an integer.".into()),
                None,
            ));
        }
        if value < i64::from(i32::MIN) || value > i64::from(i32::MAX) {
            return Err(SqlError::data_exception_public(
                "22008",
                format!(
                    "value for \"{spelled}\" in source string is out of range\nDETAIL:  Value must be in the range -2147483648 to 2147483647."
                ),
                None,
            ));
        }
        Ok((value as i32, self.pos - init))
    }

    /// `from_char_seq_search`: the index of the first name the input starts
    /// with, ignoring case.
    fn seq_search(&mut self, index: usize, names: &[&str]) -> Result<usize> {
        let rest = self.rest();
        for (position, name) in names.iter().enumerate() {
            if rest.len() >= name.len()
                && rest.is_char_boundary(name.len())
                && rest[..name.len()].eq_ignore_ascii_case(name)
            {
                self.pos += name.len();
                return Ok(position);
            }
        }
        let Node::Action { spelled, info, .. } = &self.nodes[index] else {
            unreachable!();
        };
        let copy = rest.chars().take(info.name.len()).collect::<String>();
        Err(format_error(
            format!("invalid value \"{copy}\" for \"{spelled}\""),
            Some("The given value did not match any of the allowed values for this field.".into()),
            None,
        ))
    }
}

fn set_int(dest: &mut i32, value: i32, spelled: &str) -> Result<()> {
    if *dest != 0 && *dest != value {
        return Err(format_error(
            format!("conflicting values for \"{spelled}\" field in formatting string"),
            Some("This value contradicts a previous setting for the same field type.".into()),
            None,
        ));
    }
    *dest = value;
    Ok(())
}

fn adjust_partial_year_to_2020(year: i32) -> i32 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

const MONTHS_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS_FULL: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
/// Roman months, December first (PostgreSQL's `rm_months_lower` order).
const ROMAN_MONTHS: [&str; 12] = [
    "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
];

/// `DCH_from_char`. `std` is PostgreSQL's standard (SQL/JSON) mode: fixed
/// format, separators and literal characters must match exactly, and the
/// input and the template must both be used up.
fn parse_fields(input: &str, nodes: &[Node], std: bool) -> Result<Fields> {
    let mut parser = Parser {
        input,
        pos: 0,
        nodes,
    };
    let mut out = Fields::default();
    let mut fx = std;
    let mut stopped_at = nodes.len();
    let mut extra_skip: i32 = 0;
    let months_abbrev = MONTHS_FULL.map(|name| &name[..3]);
    let days_abbrev = DAYS_FULL.map(|name| &name[..3]);
    for index in 0..nodes.len() {
        if parser.pos >= input.len() {
            stopped_at = index;
            break;
        }
        let is_action = matches!(nodes[index], Node::Action { .. });
        let is_fx = matches!(&nodes[index], Node::Action { info, .. } if info.key == Key::Fx);
        if !fx && !is_fx && (is_action || index == 0) {
            extra_skip += parser.skip_spaces() as i32;
        }
        match &nodes[index] {
            Node::Space(separator) | Node::Separator(separator) => {
                if std {
                    if !parser.rest().starts_with(*separator) {
                        return Err(format_error(
                            format!("unmatched format separator \"{separator}\""),
                            None,
                            None,
                        ));
                    }
                    parser.pos += separator.len_utf8();
                } else if !fx {
                    extra_skip -= 1;
                    if parser
                        .rest()
                        .chars()
                        .next()
                        .is_some_and(|ch| ch.is_whitespace() || is_separator_char(ch))
                    {
                        parser.pos += 1;
                        extra_skip += 1;
                    }
                } else if let Some(ch) = parser.rest().chars().next() {
                    parser.pos += ch.len_utf8();
                }
                continue;
            }
            Node::Char(expected) => {
                if !fx {
                    if extra_skip > 0 {
                        extra_skip -= 1;
                    } else if let Some(ch) = parser.rest().chars().next() {
                        parser.pos += ch.len_utf8();
                    }
                } else if let Some(ch) = parser.rest().chars().next() {
                    if std && ch != *expected {
                        return Err(format_error(
                            format!("unmatched format character \"{expected}\""),
                            None,
                            None,
                        ));
                    }
                    parser.pos += ch.len_utf8();
                }
                continue;
            }
            Node::Action { .. } => {}
        }
        let Node::Action {
            info, spelled, th, ..
        } = &nodes[index]
        else {
            unreachable!();
        };
        if info.mode != Mode::None {
            match out.mode {
                None => out.mode = Some(info.mode),
                Some(mode) if mode != info.mode => {
                    return Err(format_error(
                        "invalid combination of date conventions".into(),
                        None,
                        Some(
                            "Do not mix Gregorian and ISO week date conventions in a formatting template.",
                        ),
                    ));
                }
                _ => {}
            }
        }
        let len = info.name.len();
        match info.key {
            Key::Fx => fx = true,
            Key::AmDots | Key::Am => {
                let names: &[&str] = if info.key == Key::AmDots {
                    &["a.m.", "p.m."]
                } else {
                    &["am", "pm"]
                };
                let value = parser.seq_search(index, names)? as i32;
                set_int(&mut out.pm, value % 2 + 1, spelled)?;
                out.twelve_hour = true;
            }
            Key::Hh12 => {
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.hh, value, spelled)?;
                out.twelve_hour = true;
            }
            Key::Hh24 => {
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.hh, value, spelled)?;
            }
            Key::Mi => {
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.mi, value, spelled)?;
            }
            Key::Ss => {
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.ss, value, spelled)?;
            }
            Key::Ms => {
                let (value, used) = parser.parse_int(index, 3)?;
                let scale = match used {
                    1 => 100,
                    2 => 10,
                    _ => 1,
                };
                set_int(&mut out.ms, value * scale, spelled)?;
            }
            Key::Ff(_) | Key::Us => {
                let digits = match info.key {
                    Key::Ff(digits) => {
                        out.ff = i32::from(digits);
                        usize::from(digits)
                    }
                    _ => 6,
                };
                let (value, used) = parser.parse_int(index, digits)?;
                let scale = 10_i32.pow(6_u32.saturating_sub(used.min(6) as u32));
                set_int(&mut out.us, value.saturating_mul(scale), spelled)?;
            }
            Key::Ssss => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.ssss, value, spelled)?;
            }
            Key::Tz | Key::Of | Key::Tzh => {
                if info.key == Key::Tz {
                    let letters = parser
                        .rest()
                        .bytes()
                        .take_while(u8::is_ascii_alphabetic)
                        .count();
                    let session = current_timezone_name();
                    let dynamic = (1..=letters).rev().find(|count| {
                        crate::datetime_input::zone_uses_abbreviation(
                            &session,
                            &parser.rest()[..*count],
                        )
                    });
                    if let Some(count) = dynamic {
                        out.zone_abbreviation = Some(parser.rest()[..count].to_string());
                        out.gmtoffset = None;
                        out.tzsign = 0;
                        parser.pos += count;
                        skip_after_field(&mut parser, fx, &mut extra_skip);
                        continue;
                    }
                    let matched = (1..=letters).rev().find_map(|count| {
                        timezone_abbreviation_offset(&parser.rest()[..count])
                            .map(|offset| (count, offset))
                    });
                    if let Some((count, offset)) = matched {
                        out.gmtoffset = Some(offset);
                        out.tzsign = 0;
                        parser.pos += count;
                        skip_after_field(&mut parser, fx, &mut extra_skip);
                        continue;
                    }
                    if letters > 0 {
                        return Err(format_error(
                            format!("invalid value \"{}\" for \"{spelled}\"", parser.rest()),
                            Some("Time zone abbreviation is not recognized.".into()),
                            None,
                        ));
                    }
                }
                match parser.rest().as_bytes().first() {
                    Some(b'+') | Some(b'-') | Some(b' ') => {
                        out.tzsign = if parser.rest().starts_with('-') {
                            -1
                        } else {
                            1
                        };
                        parser.pos += 1;
                    }
                    _ => {
                        out.tzsign = if extra_skip > 0
                            && parser.pos > 0
                            && input.as_bytes()[parser.pos - 1] == b'-'
                        {
                            -1
                        } else {
                            1
                        };
                    }
                }
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.tzh, value, spelled)?;
                if info.key != Key::Tzh && parser.rest().starts_with(':') {
                    parser.pos += 1;
                    let (value, _) = parser.parse_int(index, 2)?;
                    set_int(&mut out.tzm, value, spelled)?;
                }
            }
            Key::Tzm => {
                if out.tzsign == 0 {
                    out.tzsign = 1;
                }
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.tzm, value, spelled)?;
            }
            Key::AdDots | Key::Ad => {
                let names: &[&str] = if info.key == Key::AdDots {
                    &["a.d.", "b.c."]
                } else {
                    &["ad", "bc"]
                };
                let value = parser.seq_search(index, names)? as i32;
                set_int(&mut out.bc, value % 2 + 1, spelled)?;
            }
            Key::Month => {
                let value = parser.seq_search(index, &MONTHS_FULL)? as i32;
                set_int(&mut out.mm, value + 1, spelled)?;
            }
            Key::Mon => {
                let value = parser.seq_search(index, &months_abbrev)? as i32;
                set_int(&mut out.mm, value + 1, spelled)?;
            }
            Key::Mm => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.mm, value, spelled)?;
            }
            Key::Day => {
                let value = parser.seq_search(index, &DAYS_FULL)? as i32;
                set_int(&mut out.d, value + 1, spelled)?;
            }
            Key::Dy => {
                let value = parser.seq_search(index, &days_abbrev)? as i32;
                set_int(&mut out.d, value + 1, spelled)?;
            }
            Key::Ddd | Key::Iddd => {
                let (value, _) = parser.parse_int(index, 3)?;
                set_int(&mut out.ddd, value, spelled)?;
            }
            Key::Dd => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.dd, value, spelled)?;
            }
            Key::D => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.d, value, spelled)?;
            }
            Key::Id => {
                let (value, _) = parser.parse_int(index, 1)?;
                // Sunday = 1, as the Gregorian numbering.
                let value = if value + 1 > 7 { 1 } else { value + 1 };
                set_int(&mut out.d, value, spelled)?;
            }
            Key::Ww | Key::Iw => {
                let (value, _) = parser.parse_int(index, 2)?;
                set_int(&mut out.ww, value, spelled)?;
            }
            Key::Q => {
                parser.parse_int(index, len)?;
            }
            Key::Cc => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.cc, value, spelled)?;
            }
            Key::YCommaYyy => {
                let rest = parser.rest();
                let parsed = (|| {
                    let trimmed = rest.trim_start();
                    let skipped = rest.len() - trimmed.len();
                    let sign = usize::from(trimmed.starts_with(['+', '-']));
                    let digits = trimmed[sign..]
                        .bytes()
                        .take_while(u8::is_ascii_digit)
                        .count();
                    if digits == 0 {
                        return None;
                    }
                    let millennia = trimmed[..sign + digits].parse::<i32>().ok()?;
                    let after = &trimmed[sign + digits..];
                    let after = after.strip_prefix(',')?;
                    let after_trimmed = after.trim_start();
                    let space = after.len() - after_trimmed.len();
                    let year_sign = usize::from(after_trimmed.starts_with(['+', '-']));
                    let year_digits = after_trimmed[year_sign..]
                        .bytes()
                        .take(3)
                        .take_while(u8::is_ascii_digit)
                        .count();
                    if year_digits == 0 {
                        return None;
                    }
                    let years = after_trimmed[..year_sign + year_digits]
                        .parse::<i32>()
                        .ok()?;
                    Some((
                        millennia.checked_mul(1000)?.checked_add(years)?,
                        skipped + sign + digits + 1 + space + year_sign + year_digits,
                    ))
                })();
                let Some((value, consumed)) = parsed else {
                    return Err(format_error(
                        format!("invalid value \"{rest}\" for \"Y,YYY\""),
                        None,
                        None,
                    ));
                };
                set_int(&mut out.year, value, spelled)?;
                out.year_size = 4;
                parser.pos += consumed;
                if *th {
                    parser.pos = (parser.pos + 2).min(input.len());
                }
            }
            Key::Yyyy => {
                let (value, _) = parser.parse_int(index, 4)?;
                set_int(&mut out.year, value, spelled)?;
                out.year_size = 4;
            }
            Key::Yyy | Key::Yy | Key::Y => {
                let size = match info.key {
                    Key::Yyy => 3,
                    Key::Yy => 2,
                    _ => 1,
                };
                let (value, used) = parser.parse_int(index, size)?;
                let value = if used < 4 {
                    adjust_partial_year_to_2020(value)
                } else {
                    value
                };
                set_int(&mut out.year, value, spelled)?;
                out.year_size = size as i32;
            }
            Key::Rm => {
                let value = parser.seq_search(index, &ROMAN_MONTHS)? as i32;
                set_int(&mut out.mm, 12 - value, spelled)?;
            }
            Key::W => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.w, value, spelled)?;
            }
            Key::J => {
                let (value, _) = parser.parse_int(index, len)?;
                set_int(&mut out.j, value, spelled)?;
            }
        }
        // `TH` after a numeric field skips two characters.
        if *th && info.digit && info.key != Key::YCommaYyy {
            let skip = parser
                .rest()
                .chars()
                .take(2)
                .map(char::len_utf8)
                .sum::<usize>();
            parser.pos += skip;
        }
        skip_after_field(&mut parser, fx, &mut extra_skip);
    }
    if std {
        if stopped_at < nodes.len() {
            return Err(format_error(
                "input string is too short for datetime format".into(),
                None,
                None,
            ));
        }
        parser.skip_spaces();
        if !parser.rest().is_empty() {
            return Err(format_error(
                "trailing characters remain in input string after datetime format".into(),
                None,
                None,
            ));
        }
    }
    Ok(out)
}

fn skip_after_field(parser: &mut Parser<'_>, fx: bool, extra_skip: &mut i32) {
    if !fx {
        *extra_skip = parser.skip_spaces() as i32;
    }
}

/// Days from 1970-01-01 of a proleptic Gregorian date (year 0 = 1 BC).
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days from 1970-01-01 of the Monday starting ISO week 1 of `year`.
fn iso_week1_monday(year: i64) -> i64 {
    let jan4 = days_from_civil(year, 1, 4);
    // 1970-01-01 was a Thursday: weekday with Monday = 0.
    let weekday = (jan4 + 3).rem_euclid(7);
    jan4 - weekday
}

/// The broken-down result of a template parse.
struct Broken {
    year: i64,
    month: i64,
    day: i64,
    micros_of_day: i64,
    /// Seconds east of UTC when the template gave a zone.
    offset: Option<i32>,
}

fn out_of_range(input: &str) -> SqlError {
    SqlError::data_exception_public(
        "22008",
        format!("date/time field value out of range: \"{input}\""),
        None,
    )
}

fn do_to_timestamp(input: &str, format: &str) -> Result<Broken> {
    let nodes = parse_template(format, false)?;
    let fields = parse_fields(input, &nodes, false)?;
    broken_from_fields(input, &fields)
}

fn broken_from_fields(input: &str, fields: &Fields) -> Result<Broken> {
    let mut hour = 0i64;
    let mut minute = 0i64;
    let mut second = 0i64;
    if fields.ssss != 0 {
        let total = i64::from(fields.ssss);
        hour = total / 3600;
        minute = total % 3600 / 60;
        second = total % 60;
    }
    if fields.ss != 0 {
        second = fields.ss.into();
    }
    if fields.mi != 0 {
        minute = fields.mi.into();
    }
    if fields.hh != 0 {
        hour = fields.hh.into();
    }
    if fields.twelve_hour {
        if !(1..=12).contains(&hour) {
            return Err(format_error(
                format!("hour \"{hour}\" is invalid for the 12-hour clock"),
                None,
                Some("Use the 24-hour clock, or give an hour between 1 and 12."),
            ));
        }
        let pm = fields.pm == 2;
        if pm && hour < 12 {
            hour += 12;
        } else if !pm && hour == 12 {
            hour = 0;
        }
    }
    let bc = fields.bc == 2;
    let iso = fields.mode == Some(Mode::IsoWeek);
    let mut year = 0i64;
    let mut month = 1i64;
    let mut day = 1i64;
    let mut have_year = false;
    let mut date_set = false;
    if fields.year != 0 {
        if fields.cc != 0 && fields.year_size <= 2 {
            let cc = i64::from(if bc { -fields.cc } else { fields.cc });
            let mut y = i64::from(fields.year) % 100;
            if y != 0 {
                if cc >= 0 {
                    y += (cc - 1) * 100;
                } else {
                    y = (cc + 1) * 100 - y + 1;
                }
            } else {
                y = cc * 100 + if cc >= 0 { 0 } else { 1 };
            }
            year = y;
        } else {
            year = fields.year.into();
            if bc {
                year = -year;
            }
            if year < 0 {
                year += 1;
            }
        }
        have_year = true;
    } else if fields.cc != 0 {
        let cc = i64::from(if bc { -fields.cc } else { fields.cc });
        year = if cc >= 0 {
            (cc - 1) * 100 + 1
        } else {
            cc * 100 + 1
        };
        have_year = true;
    } else if bc {
        // `BC` alone: year 0 is 1 BC either way.
    }
    let mut ddd = i64::from(fields.ddd);
    let mut dd = i64::from(fields.dd);
    if fields.j != 0 {
        let days = i64::from(fields.j) - 2_440_588;
        let (y, m, d) = civil_from_days(days);
        year = y;
        month = m;
        day = d;
        date_set = true;
    }
    if fields.ww != 0 {
        if iso {
            let monday = iso_week1_monday(year) + (i64::from(fields.ww) - 1) * 7;
            let days = if fields.d != 0 {
                let wday = i64::from(fields.d);
                monday + if wday > 1 { wday - 2 } else { 6 }
            } else {
                monday
            };
            let (y, m, d) = civil_from_days(days);
            year = y;
            month = m;
            day = d;
            date_set = true;
        } else {
            ddd = (i64::from(fields.ww) - 1) * 7 + 1;
        }
    }
    if fields.w != 0 {
        dd = (i64::from(fields.w) - 1) * 7 + 1;
    }
    if dd != 0 {
        day = dd;
    }
    if fields.mm != 0 {
        month = fields.mm.into();
    }
    if ddd != 0 && (month <= 1 || day <= 1) {
        if year == 0 && !bc {
            return Err(format_error(
                "cannot calculate day of year without year information".into(),
                None,
                None,
            ));
        }
        if iso {
            let days = iso_week1_monday(year) - 1 + ddd;
            let (y, m, d) = civil_from_days(days);
            year = y;
            month = m;
            day = d;
        } else {
            let mut month_index = 1;
            let mut before = 0;
            while month_index <= 12 {
                let through = before + days_in_month(year, month_index);
                if ddd <= through {
                    break;
                }
                before = through;
                month_index += 1;
            }
            if month <= 1 {
                month = month_index;
            }
            if day <= 1 {
                day = ddd - before;
            }
        }
        date_set = true;
    }
    let _ = (have_year, date_set);
    let fraction = i64::from(fields.ms) * 1000 + i64::from(fields.us);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return Err(out_of_range(input));
    }
    if !(0..24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..60).contains(&second)
        || !(0..1_000_000).contains(&fraction)
    {
        return Err(out_of_range(input));
    }
    let offset = if fields.tzsign != 0 {
        if !(0..=15).contains(&fields.tzh) || !(0..60).contains(&fields.tzm) {
            return Err(SqlError::data_exception_public(
                "22009",
                format!("time zone displacement out of range: \"{input}\""),
                None,
            ));
        }
        Some(fields.tzsign * (fields.tzh * 3600 + fields.tzm * 60))
    } else if let Some(abbreviation) = &fields.zone_abbreviation {
        let days = days_from_civil(year, month, day);
        let seconds = days * 86_400 + (hour * 60 + minute) * 60 + second;
        crate::datetime_input::zone_abbreviation_offset(
            &current_timezone_name(),
            abbreviation,
            seconds,
        )
    } else {
        fields.gmtoffset
    };
    Ok(Broken {
        year,
        month,
        day,
        micros_of_day: ((hour * 60 + minute) * 60 + second) * 1_000_000 + fraction,
        offset,
    })
}

/// The type `parse_datetime` gives a standard-mode template result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParsedKind {
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

/// A standard-mode template result: `value` is days since 2000-01-01 for a
/// date, microseconds of the day for a time (local for timetz), local
/// microseconds since 2000-01-01 for a timestamp and UTC microseconds for a
/// timestamptz. `offset` is seconds east of UTC for the zoned kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParsedDatetime {
    pub(crate) kind: ParsedKind,
    pub(crate) value: i64,
    pub(crate) offset: i32,
    /// The fractional precision an `FFn` field gave, or -1.
    pub(crate) typmod: i32,
}

const DATED: u8 = 1;
const TIMED: u8 = 2;
const ZONED: u8 = 4;

/// `DCH_datetime_type`.
fn datetime_flags(nodes: &[Node]) -> u8 {
    let mut flags = 0;
    for node in nodes {
        let Node::Action { info, .. } = node else {
            continue;
        };
        flags |= match info.key {
            Key::Fx | Key::Iw => 0,
            Key::AmDots
            | Key::Am
            | Key::Hh12
            | Key::Hh24
            | Key::Mi
            | Key::Ss
            | Key::Ms
            | Key::Us
            | Key::Ff(_)
            | Key::Ssss => TIMED,
            Key::Tz | Key::Of | Key::Tzh | Key::Tzm => ZONED,
            _ => DATED,
        };
    }
    flags
}

/// `AdjustTimestampForTypmod` / `AdjustTimeForTypmod`: round half away from
/// zero to `precision` fractional digits.
pub(crate) fn round_micros_to_precision(value: i64, precision: i32) -> i64 {
    if !(0..6).contains(&precision) {
        return value;
    }
    let scale = 10_i64.pow(6 - precision as u32);
    let offset = scale / 2;
    if value >= 0 {
        value.saturating_add(offset) / scale * scale
    } else {
        -((value.saturating_neg().saturating_add(offset)) / scale * scale)
    }
}

/// PostgreSQL's `parse_datetime` in standard mode (the jsonpath
/// `.datetime()` family): the template decides the result type.
pub(crate) fn parse_datetime_std(input: &str, format: &str) -> Result<ParsedDatetime> {
    let nodes = parse_template(format, true)?;
    let fields = parse_fields(input, &nodes, true)?;
    let flags = datetime_flags(&nodes);
    let broken = broken_from_fields(input, &fields)?;
    let typmod = if fields.ff != 0 { fields.ff } else { -1 };
    let days = days_from_civil(broken.year, broken.month, broken.day) - UNIX_TO_PG_DAYS;
    let local = || {
        days.checked_mul(86_400_000_000)
            .and_then(|micros| micros.checked_add(broken.micros_of_day))
            .filter(|micros| (-211_813_488_000_000_000..9_223_371_331_200_000_000).contains(micros))
    };
    let zone = || {
        broken.offset.ok_or_else(|| {
            format_error(
                "missing time zone in input string for type timestamptz".into(),
                None,
                None,
            )
        })
    };
    let (kind, value, offset) = if flags & DATED != 0 {
        if flags & TIMED != 0 {
            if flags & ZONED != 0 {
                let offset = zone()?;
                let utc = local()
                    .and_then(|local| local.checked_sub(i64::from(offset) * 1_000_000))
                    .ok_or_else(|| {
                        SqlError::data_exception_public(
                            "22008",
                            "timestamptz out of range".to_string(),
                            None,
                        )
                    })?;
                (
                    ParsedKind::TimestampTz,
                    round_micros_to_precision(utc, typmod),
                    offset,
                )
            } else {
                let local = local().ok_or_else(timestamp_out_of_range)?;
                (
                    ParsedKind::Timestamp,
                    round_micros_to_precision(local, typmod),
                    0,
                )
            }
        } else if flags & ZONED != 0 {
            return Err(format_error(
                "datetime format is zoned but not timed".into(),
                None,
                None,
            ));
        } else {
            if !(-2_451_545..=2_145_031_948).contains(&days) {
                return Err(SqlError::data_exception_public(
                    "22008",
                    format!("date out of range: \"{input}\""),
                    None,
                ));
            }
            (ParsedKind::Date, days, 0)
        }
    } else if flags & TIMED != 0 {
        let time = round_micros_to_precision(broken.micros_of_day, typmod);
        if flags & ZONED != 0 {
            let offset = broken.offset.ok_or_else(|| {
                format_error(
                    "missing time zone in input string for type timetz".into(),
                    None,
                    None,
                )
            })?;
            (ParsedKind::TimeTz, time, offset)
        } else {
            (ParsedKind::Time, time, 0)
        }
    } else {
        return Err(format_error(
            "datetime format is not dated and not timed".into(),
            None,
            None,
        ));
    };
    Ok(ParsedDatetime {
        kind,
        value,
        offset,
        typmod,
    })
}

const UNIX_TO_PG_DAYS: i64 = 10_957;

/// `to_timestamp(text, text)`: the timestamptz text the template yields.
pub(crate) fn to_timestamp(input: &str, format: &str) -> Result<String> {
    let broken = do_to_timestamp(input, format)?;
    let days = days_from_civil(broken.year, broken.month, broken.day) - UNIX_TO_PG_DAYS;
    let local = days
        .checked_mul(86_400_000_000)
        .and_then(|micros| micros.checked_add(broken.micros_of_day))
        .ok_or_else(timestamp_out_of_range)?;
    // PostgreSQL's timestamp range: 4714-11-24 BC to 294276 AD.
    if !(-211_813_488_000_000_000..9_223_371_331_200_000_000).contains(&local) {
        return Err(timestamp_out_of_range());
    }
    let offset = match broken.offset {
        Some(offset) => offset,
        None => {
            local_timestamp_offset_seconds(PgTimestamp::Finite(local), &current_timezone_name())
                .unwrap_or(0)
        }
    };
    let utc = local - i64::from(offset) * 1_000_000;
    Ok(render_timestamptz(PgTimestamp::Finite(utc)))
}

/// `to_date(text, text)`: the ISO date text the template yields.
pub(crate) fn to_date(input: &str, format: &str) -> Result<String> {
    let broken = do_to_timestamp(input, format)?;
    let days = days_from_civil(broken.year, broken.month, broken.day) - UNIX_TO_PG_DAYS;
    // Julian day 0 (4714-11-24 BC) through 5874897-12-31.
    if !(-2_451_545..=2_145_031_948).contains(&days) {
        return Err(SqlError::data_exception_public(
            "22008",
            format!("date out of range: \"{input}\""),
            None,
        ));
    }
    let date = crate::PgDate::Finite(days as i32);
    Ok(date.to_iso_text())
}

fn timestamp_out_of_range() -> SqlError {
    SqlError::data_exception_public("22008", "timestamp out of range".to_string(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(input: &str, format: &str) -> String {
        let broken = do_to_timestamp(input, format).unwrap();
        format!(
            "{:04}-{:02}-{:02} {}",
            broken.year,
            broken.month,
            broken.day,
            broken.micros_of_day / 1_000_000
        )
    }

    #[test]
    fn templates_follow_postgres_rules() {
        assert_eq!(
            ts("0097/Feb/16 --> 08:14:30", "YYYY/Mon/DD --> HH:MI:SS"),
            "0097-02-16 29670"
        );
        assert_eq!(
            ts("97/2/16 8:14:30", "FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS"),
            "0097-02-16 29670"
        );
        assert_eq!(
            ts("2011$03!18 23_38_15", "YYYY-MM-DD HH24:MI:SS"),
            "2011-03-18 85095"
        );
        assert_eq!(ts("1985 January 12", "YYYY FMMonth DD"), "1985-01-12 0");
        assert_eq!(ts("05121445482000", "MMDDHH24MISSYYYY"), "2000-05-12 53148");
        assert_eq!(ts("97/Feb/16", "YY:Mon:DD"), "1997-02-16 0");
        assert_eq!(ts("20000-1116", "YYYY-MMDD"), "20000-11-16 0");
        assert_eq!(ts("1997 BC 11 16", "YYYY BC MM DD"), "-1996-11-16 0");
        assert_eq!(ts("9-1116", "Y-MMDD"), "2009-11-16 0");
        assert_eq!(ts("1,582nd VIII 21", "Y,YYYth FMRM DD"), "1582-08-21 0");
        assert_eq!(ts("2005 1 1", "IYYY IW ID"), "2005-01-03 0");
        let error = do_to_timestamp("97/Feb/16", "YYMonDD")
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("invalid value \"/Fe\" for \"Mon\""),
            "{error}"
        );
        let error = do_to_timestamp("2015-02-29", "YYYY-MM-DD")
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("date/time field value out of range"),
            "{error}"
        );
    }
}
