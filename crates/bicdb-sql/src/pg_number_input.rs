//! PostgreSQL text-to-value functions with their own input grammar:
//! `to_number(text, text)` (the number-format picture of `to_char`, read in
//! the C locale) and `parse_ident(text [, strict])`.
use crate::*;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum NumKey {
    Comma,
    Dec,
    Zero,
    Nine,
    B,
    C,
    D,
    E,
    Fm,
    G,
    L,
    Mi,
    Pl,
    Pr,
    RnUpper,
    RnLower,
    Sg,
    Sp,
    S,
    ThUpper,
    ThLower,
    V,
}

/// `NUM_keywords`, in PostgreSQL's search order (longer spellings first
/// where they share a first character).
const NUM_KEYWORDS: &[(&str, NumKey)] = &[
    (",", NumKey::Comma),
    (".", NumKey::Dec),
    ("0", NumKey::Zero),
    ("9", NumKey::Nine),
    ("B", NumKey::B),
    ("C", NumKey::C),
    ("D", NumKey::D),
    ("EEEE", NumKey::E),
    ("FM", NumKey::Fm),
    ("G", NumKey::G),
    ("L", NumKey::L),
    ("MI", NumKey::Mi),
    ("PL", NumKey::Pl),
    ("PR", NumKey::Pr),
    ("RN", NumKey::RnUpper),
    ("SG", NumKey::Sg),
    ("SP", NumKey::Sp),
    ("S", NumKey::S),
    ("TH", NumKey::ThUpper),
    ("V", NumKey::V),
    ("b", NumKey::B),
    ("c", NumKey::C),
    ("d", NumKey::D),
    ("eeee", NumKey::E),
    ("fm", NumKey::Fm),
    ("g", NumKey::G),
    ("l", NumKey::L),
    ("mi", NumKey::Mi),
    ("pl", NumKey::Pl),
    ("pr", NumKey::Pr),
    ("rn", NumKey::RnLower),
    ("sg", NumKey::Sg),
    ("sp", NumKey::Sp),
    ("s", NumKey::S),
    ("th", NumKey::ThLower),
    ("v", NumKey::V),
];

enum FormatNode {
    Action(NumKey),
    /// A literal format character: `to_number` skips one input character.
    Char,
}

const F_DECIMAL: u32 = 1 << 1;
const F_LDECIMAL: u32 = 1 << 2;
const F_ZERO: u32 = 1 << 3;
const F_BLANK: u32 = 1 << 4;
const F_FILLMODE: u32 = 1 << 5;
const F_LSIGN: u32 = 1 << 6;
const F_BRACKET: u32 = 1 << 7;
const F_MINUS: u32 = 1 << 8;
const F_PLUS: u32 = 1 << 9;
const F_ROMAN: u32 = 1 << 10;
const F_MULTI: u32 = 1 << 11;
const F_PLUS_POST: u32 = 1 << 12;
const F_MINUS_POST: u32 = 1 << 13;
const F_EEEE: u32 = 1 << 14;

#[derive(Default)]
struct NumDesc {
    pre: i32,
    post: i32,
    multi: i32,
    flag: u32,
    lsign_pre: Option<bool>,
}

impl NumDesc {
    fn has(&self, flag: u32) -> bool {
        self.flag & flag != 0
    }
}

fn syntax_error(message: &str) -> SqlError {
    SqlError::data_exception_public("42601", message.to_string(), None)
}

/// `NUMDesc_prepare` for one format keyword.
fn prepare(num: &mut NumDesc, key: NumKey) -> Result<()> {
    if num.has(F_EEEE) && key != NumKey::E {
        return Err(syntax_error("\"EEEE\" must be the last pattern used"));
    }
    match key {
        NumKey::Nine => {
            if num.has(F_BRACKET) {
                return Err(syntax_error("\"9\" must be ahead of \"PR\""));
            }
            if num.has(F_MULTI) {
                num.multi += 1;
            } else if num.has(F_DECIMAL) {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        NumKey::Zero => {
            if num.has(F_BRACKET) {
                return Err(syntax_error("\"0\" must be ahead of \"PR\""));
            }
            if !num.has(F_ZERO) && !num.has(F_DECIMAL) {
                num.flag |= F_ZERO;
            }
            if num.has(F_DECIMAL) {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        NumKey::B => {
            if num.pre == 0 && num.post == 0 && !num.has(F_ZERO) {
                num.flag |= F_BLANK;
            }
        }
        NumKey::D | NumKey::Dec => {
            if key == NumKey::D {
                num.flag |= F_LDECIMAL;
            }
            if num.has(F_DECIMAL) {
                return Err(syntax_error("multiple decimal points"));
            }
            if num.has(F_MULTI) {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= F_DECIMAL;
        }
        NumKey::Fm => num.flag |= F_FILLMODE,
        NumKey::S => {
            if num.has(F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" twice"));
            }
            if num.has(F_PLUS) || num.has(F_MINUS) || num.has(F_BRACKET) {
                return Err(syntax_error(
                    "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together",
                ));
            }
            if !num.has(F_DECIMAL) {
                num.lsign_pre = Some(true);
                num.flag |= F_LSIGN;
            } else if num.lsign_pre.is_none() {
                num.lsign_pre = Some(false);
                num.flag |= F_LSIGN;
            }
        }
        NumKey::Mi => {
            if num.has(F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"MI\" together"));
            }
            num.flag |= F_MINUS;
            if num.has(F_DECIMAL) {
                num.flag |= F_MINUS_POST;
            }
        }
        NumKey::Pl => {
            if num.has(F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"PL\" together"));
            }
            num.flag |= F_PLUS;
            if num.has(F_DECIMAL) {
                num.flag |= F_PLUS_POST;
            }
        }
        NumKey::Sg => {
            if num.has(F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"SG\" together"));
            }
            num.flag |= F_MINUS | F_PLUS;
        }
        NumKey::Pr => {
            if num.has(F_LSIGN) || num.has(F_PLUS) || num.has(F_MINUS) {
                return Err(syntax_error(
                    "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                ));
            }
            num.flag |= F_BRACKET;
        }
        NumKey::RnUpper | NumKey::RnLower => {
            if num.has(F_ROMAN) {
                return Err(syntax_error("cannot use \"RN\" twice"));
            }
            num.flag |= F_ROMAN;
        }
        NumKey::V => {
            if num.has(F_DECIMAL) {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= F_MULTI;
        }
        NumKey::E => {
            if num.has(F_EEEE) {
                return Err(syntax_error("cannot use \"EEEE\" twice"));
            }
            if num.has(F_BLANK)
                || num.has(F_FILLMODE)
                || num.has(F_LSIGN)
                || num.has(F_BRACKET)
                || num.has(F_MINUS)
                || num.has(F_PLUS)
                || num.has(F_ROMAN)
                || num.has(F_MULTI)
            {
                return Err(SqlError::with_detail(
                    syntax_error("\"EEEE\" is incompatible with other formats"),
                    "\"EEEE\" may only be used together with digit and decimal point patterns.",
                ));
            }
            num.flag |= F_EEEE;
        }
        NumKey::C | NumKey::G | NumKey::L | NumKey::Sp | NumKey::ThUpper | NumKey::ThLower => {}
        NumKey::Comma => {}
    }
    if num.has(F_ROMAN) && num.flag & !(F_ROMAN | F_FILLMODE) != 0 {
        return Err(SqlError::with_detail(
            syntax_error("\"RN\" is incompatible with other formats"),
            "\"RN\" may only be used together with \"FM\".",
        ));
    }
    Ok(())
}

/// `parse_format` for the number picture.
fn parse_number_format(format: &str) -> Result<(Vec<FormatNode>, NumDesc)> {
    let bytes = format.as_bytes();
    let mut num = NumDesc::default();
    let mut nodes = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if let Some((name, key)) = NUM_KEYWORDS
            .iter()
            .find(|(name, _)| bytes[at..].starts_with(name.as_bytes()))
        {
            prepare(&mut num, *key)?;
            nodes.push(FormatNode::Action(*key));
            at += name.len();
            continue;
        }
        if bytes[at] == b'"' {
            at += 1;
            while at < bytes.len() {
                if bytes[at] == b'"' {
                    at += 1;
                    break;
                }
                if bytes[at] == b'\\' && at + 1 < bytes.len() {
                    at += 1;
                }
                nodes.push(FormatNode::Char);
                at += utf8_len(bytes[at]);
            }
            continue;
        }
        if bytes[at] == b'\\' && bytes.get(at + 1) == Some(&b'"') {
            at += 1;
        }
        nodes.push(FormatNode::Char);
        at += utf8_len(bytes[at]);
    }
    Ok((nodes, num))
}

fn utf8_len(byte: u8) -> usize {
    match byte {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

fn roman_value(c: u8) -> i32 {
    match c {
        b'I' => 1,
        b'V' => 5,
        b'X' => 10,
        b'L' => 50,
        b'C' => 100,
        b'D' => 500,
        b'M' => 1000,
        _ => 0,
    }
}

struct NumProc<'a> {
    input: &'a [u8],
    at: usize,
    number: Vec<u8>,
    read_pre: i32,
    read_post: i32,
    read_dec: bool,
}

impl NumProc<'_> {
    fn overload(&self) -> bool {
        self.at >= self.input.len()
    }

    fn current(&self) -> u8 {
        self.input.get(self.at).copied().unwrap_or(0)
    }

    /// `NUM_eat_non_data_chars`.
    fn eat_non_data_chars(&mut self, mut n: usize) {
        while n > 0 {
            n -= 1;
            if self.overload() || b"0123456789.,+-".contains(&self.current()) {
                break;
            }
            self.at += utf8_len(self.current());
        }
    }

    /// `roman_to_int`; `None` for an invalid numeral.
    fn roman_to_int(&mut self) -> Option<i32> {
        while !self.overload() && self.current().is_ascii_whitespace() {
            self.at += 1;
        }
        let mut chars = Vec::new();
        while chars.len() < 15 && !self.overload() {
            let c = self.current().to_ascii_uppercase();
            if roman_value(c) == 0 {
                break;
            }
            chars.push(c);
            self.at += 1;
        }
        if chars.is_empty() {
            return None;
        }
        let valid_sub = |curr: u8, next: u8| {
            (curr == b'I' && matches!(next, b'V' | b'X'))
                || (curr == b'X' && matches!(next, b'L' | b'C'))
                || (curr == b'C' && matches!(next, b'D' | b'M'))
        };
        let (mut result, mut repeat, mut v, mut l, mut d) = (0, 1, 0, 0, 0);
        let mut subtraction = false;
        let mut last_subtracted = 0;
        let mut i = 0;
        let check_vld = |value: i32, v: i32, l: i32, d: i32| {
            (v > 0 && value >= 5) || (l > 0 && value >= 50) || (d > 0 && value >= 500)
        };
        while i < chars.len() {
            let (c, value) = (chars[i], roman_value(chars[i]));
            if subtraction && value >= last_subtracted {
                return None;
            }
            if check_vld(value, v, l, d) {
                return None;
            }
            match c {
                b'V' => v += 1,
                b'L' => l += 1,
                b'D' => d += 1,
                _ => {}
            }
            if i + 1 < chars.len() {
                let (next, next_value) = (chars[i + 1], roman_value(chars[i + 1]));
                if value < next_value {
                    if !valid_sub(c, next) || repeat > 1 {
                        return None;
                    }
                    if check_vld(next_value, v, l, d) {
                        return None;
                    }
                    match next {
                        b'V' => v += 1,
                        b'L' => l += 1,
                        b'D' => d += 1,
                        _ => {}
                    }
                    i += 1;
                    repeat = 1;
                    subtraction = true;
                    last_subtracted = value;
                    result += next_value - value;
                } else {
                    if c == next {
                        repeat += 1;
                        if repeat > 3 {
                            return None;
                        }
                    } else {
                        repeat = 1;
                    }
                    result += value;
                }
            } else {
                result += value;
            }
            i += 1;
        }
        Some(result)
    }

    /// `NUM_numpart_from_char` (C locale: signs `-`/`+`, point `.`).
    fn numpart_from_char(&mut self, num: &NumDesc, key: NumKey) {
        if self.overload() {
            return;
        }
        if self.current() == b' ' {
            self.at += 1;
        }
        if self.overload() {
            return;
        }
        if self.number[0] == b' '
            && matches!(key, NumKey::Zero | NumKey::Nine)
            && self.read_pre + self.read_post == 0
        {
            let c = self.current();
            if num.has(F_LSIGN) && num.lsign_pre == Some(true) {
                if c == b'-' {
                    self.at += 1;
                    self.number[0] = b'-';
                } else if c == b'+' {
                    self.at += 1;
                    self.number[0] = b'+';
                }
            } else if c == b'-' || (num.has(F_BRACKET) && c == b'<') {
                self.number[0] = b'-';
                self.at += 1;
            } else if c == b'+' {
                self.number[0] = b'+';
                self.at += 1;
            }
        }
        if self.overload() {
            return;
        }
        let mut isread = false;
        let c = self.current();
        if c.is_ascii_digit() {
            if self.read_dec && self.read_post == num.post {
                return;
            }
            self.number.push(c);
            if self.read_dec {
                self.read_post += 1;
            } else {
                self.read_pre += 1;
            }
            isread = true;
        } else if num.has(F_DECIMAL) && !self.read_dec && c == b'.' {
            self.number.push(b'.');
            self.read_dec = true;
            isread = true;
        }
        if self.overload() {
            return;
        }
        if self.number[0] == b' ' && self.read_pre + self.read_post > 0 {
            if num.has(F_LSIGN)
                && isread
                && self.at + 1 < self.input.len()
                && !self.input[self.at + 1].is_ascii_digit()
            {
                let saved = self.at;
                self.at += 1;
                match self.current() {
                    b'-' => self.number[0] = b'-',
                    b'+' => self.number[0] = b'+',
                    _ => {}
                }
                if self.number[0] == b' ' {
                    self.at = saved;
                }
            } else if !isread && !num.has(F_LSIGN) && (num.has(F_PLUS) || num.has(F_MINUS)) {
                let c = self.current();
                if c == b'-' || c == b'+' {
                    self.number[0] = c;
                }
            }
        }
    }
}

/// `to_number(text, text)`.
pub(crate) fn pg_to_number(text: &str, format: &str) -> Result<SqlValue> {
    if format.is_empty() {
        return Ok(SqlValue::Null);
    }
    let (nodes, mut num) = parse_number_format(format)?;
    if num.has(F_EEEE) {
        return Err(SqlError::Unsupported(
            "\"EEEE\" not supported for input".into(),
        ));
    }
    let mut proc = NumProc {
        input: text.as_bytes(),
        at: 0,
        number: vec![b' '],
        read_pre: 0,
        read_post: 0,
        read_dec: false,
    };
    for node in &nodes {
        if proc.overload() {
            break;
        }
        match node {
            FormatNode::Char => {
                proc.at += utf8_len(proc.current());
                continue;
            }
            FormatNode::Action(key) => match key {
                NumKey::Nine | NumKey::Zero | NumKey::Dec | NumKey::D => {
                    proc.numpart_from_char(&num, *key);
                }
                NumKey::Comma | NumKey::G => {
                    if num.has(F_FILLMODE) || proc.current() != b',' {
                        continue;
                    }
                }
                NumKey::L => {
                    proc.eat_non_data_chars(1);
                    continue;
                }
                NumKey::RnUpper | NumKey::RnLower => {
                    let Some(value) = proc.roman_to_int() else {
                        return Err(SqlError::InvalidTextRepresentation(
                            "invalid Roman numeral".to_string(),
                        ));
                    };
                    let digits = value.to_string();
                    proc.number.extend_from_slice(digits.as_bytes());
                    num.pre = digits.len() as i32;
                    num.post = 0;
                    continue;
                }
                NumKey::ThUpper | NumKey::ThLower => {
                    if num.has(F_ROMAN) || proc.number[0] == b'#' || num.has(F_DECIMAL) {
                        continue;
                    }
                    proc.eat_non_data_chars(2);
                    continue;
                }
                NumKey::Mi => {
                    if proc.current() == b'-' {
                        proc.number[0] = b'-';
                    } else {
                        proc.eat_non_data_chars(1);
                        continue;
                    }
                }
                NumKey::Pl => {
                    if proc.current() == b'+' {
                        proc.number[0] = b'+';
                    } else {
                        proc.eat_non_data_chars(1);
                        continue;
                    }
                }
                NumKey::Sg => match proc.current() {
                    c @ (b'-' | b'+') => proc.number[0] = c,
                    _ => {
                        proc.eat_non_data_chars(1);
                        continue;
                    }
                },
                _ => continue,
            },
        }
        proc.at += 1;
    }
    if proc.number.last() == Some(&b'.') {
        proc.number.pop();
    }
    let scale = proc.read_post;
    let precision = num.pre + num.multi + scale;
    let number = String::from_utf8_lossy(&proc.number).into_owned();
    let value = cast_value_to_pg_type(SqlValue::String(number), "numeric")?;
    let value = if (1..=1000).contains(&precision) {
        apply_pg_type_modifier(
            value,
            "numeric",
            Some(&PgTypeModifier::Numeric {
                precision: precision as u16,
                scale: scale as i16,
            }),
        )?
    } else {
        value
    };
    if num.has(F_MULTI) {
        return Ok(SqlValue::String(shift_decimal_left(
            &value.to_cell(),
            num.multi as usize,
        )));
    }
    Ok(value)
}

/// `value * 10^-places` with the scale PostgreSQL's `numeric_power` gives
/// `10^-places` (16 + places) added to the value's own.
fn shift_decimal_left(value: &str, places: usize) -> String {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let scale = fraction.len() + 16 + places;
    let mut all = format!("{whole}{fraction}");
    let point = whole.len() as isize - places as isize;
    let mut whole_part;
    let mut fraction_part;
    if point <= 0 {
        whole_part = "0".to_string();
        fraction_part = "0".repeat((-point) as usize);
        fraction_part.push_str(&all);
    } else {
        fraction_part = all.split_off(point as usize);
        whole_part = all;
    }
    let trimmed = whole_part.trim_start_matches('0');
    whole_part = if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    };
    while fraction_part.len() < scale {
        fraction_part.push('0');
    }
    let zero = whole_part == "0" && fraction_part.bytes().all(|b| b == b'0');
    format!(
        "{}{whole_part}.{fraction_part}",
        if negative && !zero { "-" } else { "" }
    )
}

/// `parse_ident(text, strict)`: the dot-separated identifiers of a qualified
/// name, unquoted names folded to lower case.
pub(crate) fn pg_parse_ident(text: &str, strict: bool) -> Result<SqlValue> {
    let invalid = |detail: Option<&str>| {
        let error = SqlError::data_exception_public(
            "22023",
            format!("string is not a valid identifier: \"{text}\""),
            None,
        );
        match detail {
            Some(detail) => SqlError::with_detail(error, detail),
            None => error,
        }
    };
    let bytes = text.as_bytes();
    let is_space = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b);
    let ident_start = |b: u8| b == b'_' || b.is_ascii_alphabetic() || b >= 0x80;
    let ident_cont = |b: u8| ident_start(b) || b.is_ascii_digit() || b == b'$';
    let mut parts = Vec::new();
    let mut at = 0;
    let mut after_dot = false;
    while at < bytes.len() && is_space(bytes[at]) {
        at += 1;
    }
    loop {
        let mut missing = true;
        if bytes.get(at) == Some(&b'"') {
            let mut name = Vec::new();
            at += 1;
            loop {
                match bytes.get(at) {
                    None => return Err(invalid(Some("String has unclosed double quotes."))),
                    Some(b'"') if bytes.get(at + 1) == Some(&b'"') => {
                        name.push(b'"');
                        at += 2;
                    }
                    Some(b'"') => {
                        at += 1;
                        break;
                    }
                    Some(&byte) => {
                        name.push(byte);
                        at += 1;
                    }
                }
            }
            if name.is_empty() {
                return Err(invalid(Some("Quoted identifier must not be empty.")));
            }
            parts.push(SqlValue::String(
                String::from_utf8_lossy(&name).into_owned(),
            ));
            missing = false;
        } else if bytes.get(at).is_some_and(|&b| ident_start(b)) {
            let start = at;
            at += 1;
            while bytes.get(at).is_some_and(|&b| ident_cont(b)) {
                at += 1;
            }
            let name = String::from_utf8_lossy(&bytes[start..at]).to_ascii_lowercase();
            parts.push(SqlValue::String(name));
            missing = false;
        }
        if missing {
            return Err(if bytes.get(at) == Some(&b'.') {
                invalid(Some("No valid identifier before \".\"."))
            } else if after_dot {
                invalid(Some("No valid identifier after \".\"."))
            } else {
                invalid(None)
            });
        }
        while at < bytes.len() && is_space(bytes[at]) {
            at += 1;
        }
        match bytes.get(at) {
            Some(b'.') => {
                after_dot = true;
                at += 1;
                while at < bytes.len() && is_space(bytes[at]) {
                    at += 1;
                }
            }
            None => break,
            Some(_) => {
                if strict {
                    return Err(invalid(None));
                }
                break;
            }
        }
    }
    Ok(array_value(parts))
}

/// Result type of a function implemented here.
pub(crate) fn pg_number_input_result_type(name: &str) -> Option<String> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "to_number" => Some("numeric".to_string()),
        "parse_ident" => Some("text[]".to_string()),
        _ => None,
    }
}

/// Evaluates `to_number` and `parse_ident`; `None` for other functions.
pub(crate) fn eval_pg_number_input(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    match (name, args) {
        ("to_number", [value, format]) => {
            if matches!(value, SqlValue::Null) || matches!(format, SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            pg_to_number(&value.to_cell(), &format.to_cell()).map(Some)
        }
        ("parse_ident", [text]) | ("parse_ident", [text, _]) => {
            let strict = match args.get(1) {
                None => true,
                Some(SqlValue::Null) => return Ok(Some(SqlValue::Null)),
                Some(SqlValue::Bool(strict)) => *strict,
                Some(other) => parse_pg_bool_text(&other.to_cell()).unwrap_or(true),
            };
            if matches!(text, SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            pg_parse_ident(&text.to_cell(), strict).map(Some)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(text: &str, format: &str) -> String {
        pg_to_number(text, format).unwrap().to_cell()
    }

    #[test]
    fn to_number_matches_postgres() {
        for (text, format, expected) in [
            ("-34,338,492", "99G999G999", "-34338492"),
            (
                "-34,338,492.654,878",
                "99G999G999D999G999",
                "-34338492.654878",
            ),
            ("<564646.654564>", "999999.999999PR", "-564646.654564"),
            ("0.00001-", "9.999999S", "-0.00001"),
            ("5.01-", "FM9.999999S", "-5.01"),
            ("5.01-", "FM9.999999MI", "-5.01"),
            ("5 4 4 4 4 8 . 7 8", "9 9 9 9 9 9 . 9 9", "544448.78"),
            (".01", "FM9.99", "0.01"),
            (".0", "99999999.99999999", "0.0"),
            ("0", "99.99", "0"),
            (".-01", "S99.99", "-0.01"),
            (".01-", "99.99S", "-0.01"),
            (" . 0 1-", " 9 9 . 9 9 S", "-0.01"),
            ("34,50", "999,99", "3450"),
            ("123,000", "999G", "123"),
            ("123456", "999G999", "123456"),
            ("$1234.56", "L9,999.99", "1234.56"),
            ("$1234.56", "L99,999.99", "1234.56"),
            ("$1,234.56", "L99,999.99", "1234.56"),
            ("1234.56", "L99,999.99", "1234.56"),
            ("1,234.56", "L99,999.99", "1234.56"),
            ("42nd", "99th", "42"),
            ("123456", "99999V99", "1234.560000000000000000"),
            ("CvIiI", "rn", "108"),
            ("MMXX  ", "RN", "2020"),
            ("  XIV", "  RN", "14"),
            ("M CC", "RN", "1000"),
        ] {
            assert_eq!(number(text, format), expected, "{text:?} {format:?}");
        }
        for (text, format) in [("viv", "RN"), ("DCCCD", "RN"), ("MMMM", "RN"), ("IL", "RN")] {
            assert_eq!(
                pg_to_number(text, format).unwrap_err().to_string(),
                "invalid Roman numeral"
            );
        }
    }

    #[test]
    fn parse_ident_matches_postgres() {
        assert_eq!(
            pg_parse_ident("\"SchemaX\".\"TableY\"", true)
                .unwrap()
                .to_cell(),
            "[\"SchemaX\",\"TableY\"]"
        );
        assert_eq!(
            pg_parse_ident("foo.boo[]", false).unwrap().to_cell(),
            "[\"foo\",\"boo\"]"
        );
        assert!(pg_parse_ident("foo.boo[]", true).is_err());
    }
}
