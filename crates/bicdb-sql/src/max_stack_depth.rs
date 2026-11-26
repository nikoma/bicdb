//! The `max_stack_depth` setting: PostgreSQL's units, limits and display,
//! and the value its "stack depth limit exceeded" HINT reports.
use crate::*;

/// PostgreSQL's default, 2MB.
pub(crate) const DEFAULT_KB: i64 = 2048;
const MIN_KB: i64 = 100;
/// PostgreSQL's limit with the usual 8MB platform stack: the stack size
/// less its 512kB safety margin.
const PLATFORM_MAX_KB: i64 = 8192 - 512;

thread_local! {
    static CURRENT_KB: std::cell::Cell<i64> = const { std::cell::Cell::new(DEFAULT_KB) };
}

/// Publishes the session's `max_stack_depth` for one statement.
pub(crate) struct Scope(i64);

impl Scope {
    pub(crate) fn enter(gucs: &HashMap<String, String>) -> Self {
        let kb = gucs
            .get("max_stack_depth")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(DEFAULT_KB);
        Self(CURRENT_KB.with(|current| current.replace(kb)))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT_KB.with(|current| current.set(self.0));
    }
}

/// The statement's `max_stack_depth` in kB.
pub(crate) fn current_kb() -> i64 {
    CURRENT_KB.with(std::cell::Cell::get)
}

/// The HINT of PostgreSQL's "stack depth limit exceeded".
pub(crate) fn stack_depth_hint() -> String {
    format!(
        "Increase the configuration parameter \"max_stack_depth\" (currently {}kB), after ensuring the platform's stack depth limit is adequate.",
        current_kb()
    )
}

/// A `max_stack_depth` value in kB, as `SET` reads it: an integer (kB) or
/// a number with a memory unit.
pub(crate) fn parse_setting(value: &str) -> Result<i64> {
    let invalid = || {
        SqlError::with_detail_fields(
            SqlError::invalid_parameter_value(format!(
                "invalid value for parameter \"max_stack_depth\": \"{value}\""
            )),
            None,
            Some(
                "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\"."
                    .into(),
            ),
            None,
        )
    };
    let text = value.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number = number.parse::<f64>().map_err(|_| invalid())?;
    let factor = match unit.trim() {
        "" | "kB" => 1.0,
        "B" => 1.0 / 1024.0,
        "MB" => 1024.0,
        "GB" => 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(invalid()),
    };
    let kb = (number * factor).round();
    if !kb.is_finite() || kb.abs() > i32::MAX as f64 {
        return Err(invalid());
    }
    let kb = kb as i64;
    if kb < MIN_KB {
        return Err(SqlError::invalid_parameter_value(format!(
            "{kb} kB is outside the valid range for parameter \"max_stack_depth\" ({MIN_KB} kB .. {} kB)",
            i32::MAX
        )));
    }
    if kb > PLATFORM_MAX_KB {
        return Err(SqlError::with_detail_fields(
            SqlError::invalid_parameter_value(format!(
                "invalid value for parameter \"max_stack_depth\": {kb}"
            )),
            Some(format!(
                "\"max_stack_depth\" must not exceed {PLATFORM_MAX_KB}kB."
            )),
            Some(
                "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent."
                    .into(),
            ),
            None,
        ));
    }
    Ok(kb)
}

/// `SHOW max_stack_depth`: the value in the largest unit that divides it.
pub(crate) fn display(kb: i64) -> String {
    for (unit, size) in [("TB", 1i64 << 30), ("GB", 1 << 20), ("MB", 1 << 10)] {
        if kb != 0 && kb % size == 0 {
            return format!("{}{unit}", kb / size);
        }
    }
    format!("{kb}kB")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_limits_and_display_match_postgres() {
        assert_eq!(parse_setting("100kB").unwrap(), 100);
        assert_eq!(parse_setting("2MB").unwrap(), 2048);
        assert_eq!(parse_setting("1536").unwrap(), 1536);
        assert!(parse_setting("50").is_err());
        assert!(parse_setting("10MB").is_err());
        assert_eq!(display(2048), "2MB");
        assert_eq!(display(100), "100kB");
    }
}
