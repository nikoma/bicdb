//! Shared canonical numeric ordering for SQL storage and commit-time repair.
/// The canonical decimal text `-?(0|[1-9][0-9]*)(\.[0-9]+)?` split into
/// (negative, integer digits, fraction digits) — `None` for anything else
/// (leading zeros, a bare `.5`, `1.`, exponents, NaN/Infinity, negative
/// zero), which the full parser then handles. Canonical text is exactly
/// what `PgNumeric::to_decimal_text` renders, so a value in this form
/// parses and re-renders to itself.
pub fn canonical_numeric_parts(text: &str) -> Option<(bool, &str, &str)> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (unsigned, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if whole.len() > 1 && whole.starts_with('0') {
        return None;
    }
    if unsigned.contains('.') && fraction.is_empty() {
        return None;
    }
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if negative
        && whole.bytes().all(|byte| byte == b'0')
        && fraction.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some((negative, whole, fraction))
}

/// Ordered storage key for a `numeric` value given as canonical text,
/// byte-identical to parsing it into a `PgNumeric` first (see the
/// differential test) and without the parse, the coefficient String and
/// the re-render that path costs.
pub fn numeric_index_key_from_canonical_text(text: &str) -> Option<Vec<u8>> {
    let (negative, whole, fraction) = canonical_numeric_parts(text)?;
    // PgNumeric's coefficient: whole ++ fraction without leading zeros.
    let whole_digits = whole.trim_start_matches('0');
    let (lead, tail): (&str, &str) = if whole_digits.is_empty() {
        (fraction.trim_start_matches('0'), "")
    } else {
        (whole_digits, fraction)
    };
    let coefficient_len = lead.len() + tail.len();
    if coefficient_len == 0 {
        return Some(vec![1, 2]);
    }
    // Trailing zeros are dropped from the significant digits; the exponent
    // stays relative to the full coefficient length.
    let display_scale = fraction.len() as i64;
    let exponent = coefficient_len as i64 - display_scale;
    let trimmed_tail = tail.trim_end_matches('0');
    let significant_len = if trimmed_tail.is_empty() {
        lead.trim_end_matches('0').len()
    } else {
        lead.len() + trimmed_tail.len()
    };
    // Write directly into the result. The old path allocated separate
    // significant-digit and magnitude buffers for every numeric cell.
    let mut key = Vec::with_capacity(2 + 8 + significant_len + 1);
    key.push(1);
    key.push(if negative { 1 } else { 3 });
    key.extend((exponent as u64 ^ (1 << 63)).to_be_bytes());
    key.extend_from_slice(&lead.as_bytes()[..significant_len.min(lead.len())]);
    if significant_len > lead.len() {
        key.extend_from_slice(&tail.as_bytes()[..significant_len - lead.len()]);
    }
    key.push(0);
    if negative {
        for byte in &mut key[2..] {
            *byte = !*byte;
        }
    }
    Some(key)
}
