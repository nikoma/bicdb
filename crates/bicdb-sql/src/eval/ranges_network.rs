//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn render_range_for_session(range: &PgRange, _range_type: &str) -> String {
    if range.subtype != "timestamptz" || range.empty {
        return range.to_postgres_text();
    }
    let render_bound = |bound: &PgRangeBound| match bound {
        PgRangeBound::Unbounded => String::new(),
        PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => match value.as_ref() {
            PgCanonicalValue::TimestampTz(value) => {
                format!("\"{}\"", render_timestamptz(*value).replace('"', "\\\""))
            }
            value => canonical_range_scalar_value(value).to_cell(),
        },
    };
    let lower_delimiter = if matches!(range.lower, PgRangeBound::Inclusive(_)) {
        '['
    } else {
        '('
    };
    let upper_delimiter = if matches!(range.upper, PgRangeBound::Inclusive(_)) {
        ']'
    } else {
        ')'
    };
    format!(
        "{lower_delimiter}{},{}{upper_delimiter}",
        render_bound(&range.lower),
        render_bound(&range.upper)
    )
}

pub(crate) fn render_multirange_for_session(ranges: &[PgRange], multirange_type: &str) -> String {
    let range_type = range_type_from_multirange(multirange_type).unwrap_or("int4range");
    format!(
        "{{{}}}",
        ranges
            .iter()
            .map(|range| render_range_for_session(range, range_type))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(crate) fn range_constructor_bound(value: &SqlValue) -> String {
    if matches!(value, SqlValue::Null) {
        return String::new();
    }
    format!(
        "\"{}\"",
        value.to_cell().replace('\\', "\\\\").replace('"', "\\\"")
    )
}

pub(crate) fn construct_range(range_type: &str, args: &[SqlValue]) -> Result<SqlValue> {
    if !(2..=3).contains(&args.len()) {
        return Err(SqlError::undefined_function(format!(
            "function {range_type} with {} arguments does not exist",
            args.len()
        )));
    }
    let bounds = args
        .get(2)
        .map(SqlValue::to_cell)
        .unwrap_or_else(|| "[)".to_string());
    if !matches!(bounds.as_str(), "[]" | "[)" | "(]" | "()") {
        return Err(SqlError::InvalidSql(
            "invalid range bound flags".to_string(),
        ));
    }
    let input = format!(
        "{}{},{}{}",
        &bounds[..1],
        range_constructor_bound(&args[0]),
        range_constructor_bound(&args[1]),
        &bounds[1..]
    );
    range_from_sql_value(&SqlValue::String(input), range_type)?.map_or(
        Ok(SqlValue::Null),
        |range| {
            Ok(SqlValue::String(render_range_for_session(
                &range, range_type,
            )))
        },
    )
}

pub(crate) fn construct_multirange(
    multirange_type: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<SqlValue> {
    let range_type = range_type_from_multirange(multirange_type)
        .expect("built-in multiranges have registered range types");
    if args.is_empty() {
        return Ok(SqlValue::String("{}".to_string()));
    }
    if arg_types
        .iter()
        .any(|arg_type| arg_type.as_deref() != Some(range_type))
    {
        return Err(SqlError::undefined_function(format!(
            "function {multirange_type} with the supplied arguments does not exist"
        )));
    }
    if args.len() == 1 && matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Err(SqlError::data_exception_public(
            "22004",
            "multirange values cannot contain null members",
            None,
        ));
    }
    let ranges = args
        .iter()
        .map(|value| range_from_sql_value(value, range_type).map(Option::unwrap))
        .collect::<Result<Vec<_>>>()?;
    let ranges = canonicalize_pg_multirange(ranges)
        .map_err(|error| postgres_range_input_error(multirange_type, "multirange", error))?;
    Ok(SqlValue::String(render_multirange_for_session(
        &ranges,
        multirange_type,
    )))
}

pub(crate) fn lower_bound_inclusive(bound: &PgRangeBound) -> bool {
    matches!(bound, PgRangeBound::Inclusive(_))
}

pub(crate) fn upper_bound_order(
    left: &PgRangeBound,
    right: &PgRangeBound,
    subtype: &str,
) -> Ordering {
    match (left, right) {
        (PgRangeBound::Unbounded, PgRangeBound::Unbounded) => Ordering::Equal,
        (PgRangeBound::Unbounded, _) => Ordering::Greater,
        (_, PgRangeBound::Unbounded) => Ordering::Less,
        (
            PgRangeBound::Inclusive(left_value) | PgRangeBound::Exclusive(left_value),
            PgRangeBound::Inclusive(right_value) | PgRangeBound::Exclusive(right_value),
        ) => {
            let ordering =
                range_scalar_order(subtype, left_value, right_value).unwrap_or(Ordering::Equal);
            if ordering != Ordering::Equal {
                ordering
            } else {
                lower_bound_inclusive(left).cmp(&lower_bound_inclusive(right))
            }
        }
    }
}

pub(crate) fn range_lower_order(
    left: &PgRangeBound,
    right: &PgRangeBound,
    subtype: &str,
) -> Ordering {
    match (left, right) {
        (PgRangeBound::Unbounded, PgRangeBound::Unbounded) => Ordering::Equal,
        (PgRangeBound::Unbounded, _) => Ordering::Less,
        (_, PgRangeBound::Unbounded) => Ordering::Greater,
        (
            PgRangeBound::Inclusive(left_value) | PgRangeBound::Exclusive(left_value),
            PgRangeBound::Inclusive(right_value) | PgRangeBound::Exclusive(right_value),
        ) => {
            let ordering =
                range_scalar_order(subtype, left_value, right_value).unwrap_or(Ordering::Equal);
            if ordering != Ordering::Equal {
                ordering
            } else {
                lower_bound_inclusive(right).cmp(&lower_bound_inclusive(left))
            }
        }
    }
}

pub(crate) fn upper_ends_before_lower(
    upper: &PgRangeBound,
    lower: &PgRangeBound,
    subtype: &str,
) -> bool {
    match (upper, lower) {
        (PgRangeBound::Unbounded, _) | (_, PgRangeBound::Unbounded) => false,
        (
            PgRangeBound::Inclusive(upper_value) | PgRangeBound::Exclusive(upper_value),
            PgRangeBound::Inclusive(lower_value) | PgRangeBound::Exclusive(lower_value),
        ) => match range_scalar_order(subtype, upper_value, lower_value) {
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => {
                !(lower_bound_inclusive(upper) && lower_bound_inclusive(lower))
            }
            _ => false,
        },
    }
}

pub(crate) fn bounds_are_adjacent(
    upper: &PgRangeBound,
    lower: &PgRangeBound,
    subtype: &str,
) -> bool {
    match (upper, lower) {
        (
            PgRangeBound::Inclusive(upper_value) | PgRangeBound::Exclusive(upper_value),
            PgRangeBound::Inclusive(lower_value) | PgRangeBound::Exclusive(lower_value),
        ) => {
            range_scalar_order(subtype, upper_value, lower_value) == Some(Ordering::Equal)
                && lower_bound_inclusive(upper) != lower_bound_inclusive(lower)
        }
        _ => false,
    }
}

pub(crate) fn ranges_overlap_typed(left: &PgRange, right: &PgRange) -> bool {
    !left.empty
        && !right.empty
        && !upper_ends_before_lower(&left.upper, &right.lower, &left.subtype)
        && !upper_ends_before_lower(&right.upper, &left.lower, &left.subtype)
}

pub(crate) fn range_contains_range(left: &PgRange, right: &PgRange) -> bool {
    if right.empty {
        return true;
    }
    !left.empty
        && range_lower_order(&left.lower, &right.lower, &left.subtype) != Ordering::Greater
        && upper_bound_order(&left.upper, &right.upper, &left.subtype) != Ordering::Less
}

pub(crate) fn range_contains_element(range: &PgRange, element: &PgCanonicalValue) -> bool {
    if range.empty {
        return false;
    }
    let lower_matches = match &range.lower {
        PgRangeBound::Unbounded => true,
        PgRangeBound::Inclusive(lower) => {
            range_scalar_order(&range.subtype, lower, element) != Some(Ordering::Greater)
        }
        PgRangeBound::Exclusive(lower) => {
            range_scalar_order(&range.subtype, lower, element) == Some(Ordering::Less)
        }
    };
    let upper_matches = match &range.upper {
        PgRangeBound::Unbounded => true,
        PgRangeBound::Inclusive(upper) => {
            range_scalar_order(&range.subtype, upper, element) != Some(Ordering::Less)
        }
        PgRangeBound::Exclusive(upper) => {
            range_scalar_order(&range.subtype, upper, element) == Some(Ordering::Greater)
        }
    };
    lower_matches && upper_matches
}

pub(crate) fn canonicalize_range_result(range: PgRange, range_type: &str) -> Result<SqlValue> {
    range
        .canonicalized()
        .map(|range| SqlValue::String(render_range_for_session(&range, range_type)))
        .map_err(|error| postgres_range_input_error(range_type, "range result", error))
}

pub(crate) fn range_intersection(
    left: &PgRange,
    right: &PgRange,
    range_type: &str,
) -> Result<SqlValue> {
    if !ranges_overlap_typed(left, right) {
        return Ok(SqlValue::String("empty".to_string()));
    }
    canonicalize_range_result(
        PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: if range_lower_order(&left.lower, &right.lower, &left.subtype) == Ordering::Less
            {
                right.lower.clone()
            } else {
                left.lower.clone()
            },
            upper: if upper_bound_order(&left.upper, &right.upper, &left.subtype)
                == Ordering::Greater
            {
                right.upper.clone()
            } else {
                left.upper.clone()
            },
        },
        range_type,
    )
}

pub(crate) fn range_hull(left: &PgRange, right: &PgRange, range_type: &str) -> Result<SqlValue> {
    if left.empty {
        return Ok(SqlValue::String(render_range_for_session(
            right, range_type,
        )));
    }
    if right.empty {
        return Ok(SqlValue::String(render_range_for_session(left, range_type)));
    }
    canonicalize_range_result(
        PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: if range_lower_order(&left.lower, &right.lower, &left.subtype)
                == Ordering::Greater
            {
                right.lower.clone()
            } else {
                left.lower.clone()
            },
            upper: if upper_bound_order(&left.upper, &right.upper, &left.subtype) == Ordering::Less
            {
                right.upper.clone()
            } else {
                left.upper.clone()
            },
        },
        range_type,
    )
}

pub(crate) fn complement_as_lower(bound: &PgRangeBound) -> PgRangeBound {
    match bound {
        PgRangeBound::Inclusive(value) => PgRangeBound::Exclusive(value.clone()),
        PgRangeBound::Exclusive(value) => PgRangeBound::Inclusive(value.clone()),
        PgRangeBound::Unbounded => PgRangeBound::Unbounded,
    }
}

pub(crate) fn complement_as_upper(bound: &PgRangeBound) -> PgRangeBound {
    complement_as_lower(bound)
}

pub(crate) fn range_difference(
    left: &PgRange,
    right: &PgRange,
    range_type: &str,
) -> Result<SqlValue> {
    if left.empty || !ranges_overlap_typed(left, right) {
        return Ok(SqlValue::String(render_range_for_session(left, range_type)));
    }
    if range_contains_range(right, left) {
        return Ok(SqlValue::String("empty".to_string()));
    }
    let cuts_lower =
        range_lower_order(&right.lower, &left.lower, &left.subtype) != Ordering::Greater;
    let cuts_upper = upper_bound_order(&right.upper, &left.upper, &left.subtype) != Ordering::Less;
    if !cuts_lower && !cuts_upper {
        return Err(SqlError::data_exception_public(
            "22000",
            "result of range difference would not be contiguous",
            None,
        ));
    }
    let result = if cuts_lower {
        PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: complement_as_lower(&right.upper),
            upper: left.upper.clone(),
        }
    } else {
        PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: left.lower.clone(),
            upper: complement_as_upper(&right.lower),
        }
    };
    canonicalize_range_result(result, range_type)
}

pub(crate) fn network_function_pg_type(name: &str, arg_types: &[Option<String>]) -> Option<String> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let first = arg_types.first().and_then(Option::as_deref);
    let unary = arg_types.len() == 1;
    let binary = arg_types.len() == 2;
    let second = arg_types.get(1).and_then(Option::as_deref);
    match name {
        "abbrev" | "host" | "text" if unary && matches!(first, Some("inet" | "cidr")) => {
            Some("text".to_string())
        }
        "family" | "masklen" if unary && matches!(first, Some("inet" | "cidr")) => {
            Some("int4".to_string())
        }
        "broadcast" | "hostmask" | "netmask" if unary && matches!(first, Some("inet" | "cidr")) => {
            Some("inet".to_string())
        }
        "network" if unary && matches!(first, Some("inet" | "cidr")) => Some("cidr".to_string()),
        "inet_merge"
            if binary
                && matches!(first, Some("inet" | "cidr"))
                && matches!(second, Some("inet" | "cidr")) =>
        {
            Some("cidr".to_string())
        }
        "inet_same_family"
            if binary
                && matches!(first, Some("inet" | "cidr"))
                && matches!(second, Some("inet" | "cidr")) =>
        {
            Some("bool".to_string())
        }
        "set_masklen"
            if binary
                && matches!(first, Some("inet" | "cidr"))
                && matches!(second, None | Some("int2" | "int4")) =>
        {
            first.map(str::to_string)
        }
        "cidr" if unary && matches!(first, Some("inet" | "cidr")) => Some("cidr".to_string()),
        "trunc" if unary && matches!(first, Some("macaddr" | "macaddr8")) => {
            first.map(str::to_string)
        }
        "macaddr8_set7bit" if unary && first == Some("macaddr8") => Some("macaddr8".to_string()),
        _ => None,
    }
}

pub(crate) fn cidr_abbrev(network: PgNetwork) -> String {
    let prefix = network.prefix;
    match network.address {
        PgIpAddress::V4(octets) => {
            let count = usize::from(prefix.div_ceil(8)).max(1);
            let address = octets[..count]
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(".");
            format!("{address}/{prefix}")
        }
        PgIpAddress::V6(octets) => {
            let full = Ipv6Addr::from(octets).to_string();
            let required_groups = usize::from(prefix.div_ceil(16));
            let last_required_nonzero = required_groups > 0
                && u16::from_be_bytes([
                    octets[(required_groups - 1) * 2],
                    octets[(required_groups - 1) * 2 + 1],
                ]) != 0;
            let address = if last_required_nonzero {
                full.strip_suffix("::").unwrap_or(&full)
            } else {
                &full
            };
            format!("{address}/{prefix}")
        }
    }
}

pub(crate) fn common_network_prefix(left: PgNetwork, right: PgNetwork) -> Result<u8> {
    if !same_ip_family(left.address, right.address) {
        return Err(SqlError::invalid_parameter_value(
            "cannot merge addresses from different families",
        ));
    }
    let max = left.prefix.min(right.prefix);
    let different = ip_address_value(left.address) ^ ip_address_value(right.address);
    let family_padding = 128 - u32::from(left.address.bit_len());
    let common = different.leading_zeros().saturating_sub(family_padding) as u8;
    Ok(common.min(max))
}

pub(crate) fn eval_network_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if network_function_pg_type(name, arg_types).is_none() {
        let first = arg_types.first().and_then(Option::as_deref);
        let belongs_to_network_family = matches!(
            name,
            "abbrev"
                | "broadcast"
                | "cidr"
                | "family"
                | "host"
                | "hostmask"
                | "inet_merge"
                | "inet_same_family"
                | "masklen"
                | "netmask"
                | "network"
                | "set_masklen"
                | "text"
        ) && matches!(first, Some("inet" | "cidr"));
        let belongs_to_mac_family = (name == "trunc"
            && matches!(first, Some("macaddr" | "macaddr8")))
            || name == "macaddr8_set7bit";
        if belongs_to_network_family || belongs_to_mac_family {
            let signature = arg_types
                .iter()
                .map(|pg_type| pg_type.as_deref().unwrap_or("unknown"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(SqlError::undefined_function(format!(
                "function {name}({signature}) does not exist"
            )));
        }
        return Ok(None);
    }
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }
    let first_type = arg_types.first().and_then(Option::as_deref).unwrap();
    if matches!(first_type, "macaddr" | "macaddr8") {
        let address = mac_argument(&args[0], first_type)?;
        let value = match name {
            "trunc" => match address {
                PgMacAddress::Mac48(mut octets) => {
                    octets[3..].fill(0);
                    PgMacAddress::Mac48(octets)
                }
                PgMacAddress::Mac64(mut octets) => {
                    octets[3..].fill(0);
                    PgMacAddress::Mac64(octets)
                }
            },
            "macaddr8_set7bit" => match address {
                PgMacAddress::Mac64(mut octets) => {
                    octets[0] |= 0x02;
                    PgMacAddress::Mac64(octets)
                }
                _ => unreachable!("function type dispatch requires macaddr8"),
            },
            _ => return Ok(None),
        };
        return Ok(Some(mac_value(value)));
    }
    let network = network_argument(&args[0], first_type)?;
    let value = match name {
        "abbrev" => SqlValue::String(if first_type == "cidr" {
            cidr_abbrev(network)
        } else {
            network.to_postgres_text()
        }),
        "host" => {
            let text = PgNetwork {
                prefix: network.address.bit_len(),
                ..network
            }
            .to_postgres_output_text();
            SqlValue::String(text)
        }
        "text" => SqlValue::String(network.to_postgres_text()),
        "family" => SqlValue::Int(if matches!(network.address, PgIpAddress::V4(_)) {
            4
        } else {
            6
        }),
        "masklen" => SqlValue::Int(i64::from(network.prefix)),
        "network" | "cidr" => network_value(PgNetwork {
            kind: PgNetworkKind::Cidr,
            address: masked_ip_address(network.address, network.prefix),
            prefix: network.prefix,
        }),
        "broadcast" => {
            let mask = address_mask(network.address.bit_len(), network.prefix);
            let width_mask = if network.address.bit_len() == 128 {
                u128::MAX
            } else {
                u128::from(u32::MAX)
            };
            network_value(PgNetwork {
                kind: PgNetworkKind::Inet,
                address: ip_address_from_value(
                    network.address,
                    ip_address_value(network.address) | (!mask & width_mask),
                ),
                prefix: network.prefix,
            })
        }
        "netmask" | "hostmask" => {
            let mask = address_mask(network.address.bit_len(), network.prefix);
            let width_mask = if network.address.bit_len() == 128 {
                u128::MAX
            } else {
                u128::from(u32::MAX)
            };
            network_value(PgNetwork {
                kind: PgNetworkKind::Inet,
                address: ip_address_from_value(
                    network.address,
                    if name == "netmask" {
                        mask
                    } else {
                        !mask & width_mask
                    },
                ),
                prefix: network.address.bit_len(),
            })
        }
        "set_masklen" => {
            let Some(prefix) = args.get(1).and_then(sql_value_i64) else {
                return Err(SqlError::invalid_parameter_value(
                    "set_masklen requires an integer mask length",
                ));
            };
            let prefix = if prefix == -1 {
                network.address.bit_len()
            } else {
                u8::try_from(prefix)
                    .ok()
                    .filter(|prefix| *prefix <= network.address.bit_len())
                    .ok_or_else(|| {
                        SqlError::invalid_parameter_value(format!("invalid mask length: {prefix}"))
                    })?
            };
            network_value(PgNetwork {
                kind: network.kind,
                address: if network.kind == PgNetworkKind::Cidr {
                    masked_ip_address(network.address, prefix)
                } else {
                    network.address
                },
                prefix,
            })
        }
        "inet_same_family" => {
            let second_type = arg_types
                .get(1)
                .and_then(Option::as_deref)
                .unwrap_or("inet");
            let right = network_argument(&args[1], second_type)?;
            SqlValue::Bool(same_ip_family(network.address, right.address))
        }
        "inet_merge" => {
            let second_type = arg_types
                .get(1)
                .and_then(Option::as_deref)
                .unwrap_or("inet");
            let right = network_argument(&args[1], second_type)?;
            let prefix = common_network_prefix(network, right)?;
            network_value(PgNetwork {
                kind: PgNetworkKind::Cidr,
                address: masked_ip_address(network.address, prefix),
                prefix,
            })
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn eval_range_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if is_builtin_range_type(name) {
        return construct_range(name, args).map(Some);
    }
    if is_builtin_multirange_type(name) {
        return construct_multirange(name, args, arg_types).map(Some);
    }
    if !matches!(
        name,
        "isempty"
            | "lower"
            | "upper"
            | "lower_inc"
            | "upper_inc"
            | "lower_inf"
            | "upper_inf"
            | "range_merge"
    ) {
        return Ok(None);
    }
    let Some(arg_type) = arg_types.first().and_then(Option::as_deref) else {
        return Ok(None);
    };
    if name == "range_merge" && args.len() == 1 {
        let Some(range_type) = range_type_from_multirange(arg_type) else {
            return Ok(None);
        };
        let Some(ranges) = multirange_from_sql_value(&args[0], arg_type)? else {
            return Ok(Some(SqlValue::Null));
        };
        let Some(first) = ranges.first() else {
            return Ok(Some(SqlValue::String("empty".to_string())));
        };
        let mut merged = SqlValue::String(render_range_for_session(first, range_type));
        for range in &ranges[1..] {
            let current = range_from_sql_value(&merged, range_type)?.unwrap();
            merged = range_hull(&current, range, range_type)?;
        }
        return Ok(Some(merged));
    }
    if name == "range_merge" && args.len() == 2 {
        if !is_builtin_range_type(arg_type)
            || arg_types.get(1).and_then(Option::as_deref) != Some(arg_type)
        {
            return Ok(None);
        }
        let (Some(left), Some(right)) = (
            range_from_sql_value(&args[0], arg_type)?,
            range_from_sql_value(&args[1], arg_type)?,
        ) else {
            return Ok(Some(SqlValue::Null));
        };
        return range_hull(&left, &right, arg_type).map(Some);
    }
    if !is_builtin_range_type(arg_type) || args.len() != 1 {
        return Ok(None);
    }
    let Some(range) = range_from_sql_value(&args[0], arg_type)? else {
        return Ok(Some(SqlValue::Null));
    };
    let value = match name {
        "isempty" => SqlValue::Bool(range.empty),
        "lower" | "upper" => {
            let bound = if name == "lower" {
                &range.lower
            } else {
                &range.upper
            };
            match bound {
                PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) if !range.empty => {
                    canonical_range_scalar_value(value)
                }
                _ => SqlValue::Null,
            }
        }
        "lower_inc" => SqlValue::Bool(!range.empty && lower_bound_inclusive(&range.lower)),
        "upper_inc" => SqlValue::Bool(!range.empty && lower_bound_inclusive(&range.upper)),
        "lower_inf" => {
            SqlValue::Bool(!range.empty && matches!(range.lower, PgRangeBound::Unbounded))
        }
        "upper_inf" => {
            SqlValue::Bool(!range.empty && matches!(range.upper, PgRangeBound::Unbounded))
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn eval_user_range_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if matches!(
        name,
        "isempty"
            | "lower"
            | "upper"
            | "lower_inc"
            | "upper_inc"
            | "lower_inf"
            | "upper_inf"
            | "range_merge"
    ) {
        let Some(arg_type) = arg_types.first().and_then(Option::as_deref) else {
            return Ok(None);
        };
        let (schema_name, type_name) = arg_type
            .rsplit_once('.')
            .map(|(schema, name)| (schema, name))
            .unwrap_or(("public", arg_type));
        let Some(user_type) = load_user_type(db, schema_name, type_name)? else {
            return Ok(None);
        };
        let range_value = match &user_type.kind {
            UserTypeKind::Range { value, .. } => value,
            UserTypeKind::Multirange { value, .. } if name == "range_merge" => value,
            _ => return Ok(None),
        };
        if name == "range_merge" {
            let ranges = pg_user_range_values(
                db,
                i32::try_from(user_type.oid).map_err(|_| {
                    SqlError::numeric_value_out_of_range("user range OID exceeds int4")
                })?,
                args.first().unwrap_or(&SqlValue::Null),
            )?
            .unwrap_or_default();
            if matches!(args.first(), Some(SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let Some(first) = ranges.first() else {
                return Ok(Some(SqlValue::String("empty".to_string())));
            };
            let hull = ranges
                .iter()
                .skip(1)
                .fold(first.clone(), |left, right| user_range_hull(&left, right));
            let output_type = match &user_type.kind {
                UserTypeKind::Range { .. } => user_type.column_type(false),
                UserTypeKind::Multirange {
                    range_schema_name,
                    range_name,
                    ..
                } => load_user_type(db, range_schema_name, range_name)?
                    .ok_or_else(|| {
                        SqlError::undefined_type(format!("{range_schema_name}.{range_name}"))
                    })?
                    .column_type(false),
                _ => unreachable!(),
            };
            return cast_value_to_user_range(
                SqlValue::String(render_range_for_session(
                    &hull,
                    &output_type.formatted_name(),
                )),
                &output_type,
                range_value,
                false,
            )
            .map(Some);
        }
        if args.len() != 1 {
            return Ok(None);
        }
        let Some(range) = pg_user_range_values(
            db,
            i32::try_from(user_type.oid)
                .map_err(|_| SqlError::numeric_value_out_of_range("user range OID exceeds int4"))?,
            &args[0],
        )?
        .and_then(|ranges| ranges.into_iter().next()) else {
            return Ok(Some(SqlValue::Null));
        };
        let value = match name {
            "isempty" => SqlValue::Bool(range.empty),
            "lower" | "upper" => {
                let bound = if name == "lower" {
                    &range.lower
                } else {
                    &range.upper
                };
                match bound {
                    PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value)
                        if !range.empty =>
                    {
                        canonical_range_scalar_value(value)
                    }
                    _ => SqlValue::Null,
                }
            }
            "lower_inc" => SqlValue::Bool(!range.empty && lower_bound_inclusive(&range.lower)),
            "upper_inc" => SqlValue::Bool(!range.empty && lower_bound_inclusive(&range.upper)),
            "lower_inf" => {
                SqlValue::Bool(!range.empty && matches!(range.lower, PgRangeBound::Unbounded))
            }
            "upper_inf" => {
                SqlValue::Bool(!range.empty && matches!(range.upper, PgRangeBound::Unbounded))
            }
            _ => unreachable!(),
        };
        return Ok(Some(value));
    }
    let (schema_name, type_name) = name
        .rsplit_once('.')
        .map(|(schema, name)| (schema, name))
        .unwrap_or(("public", name));
    let Some(user_type) = load_user_type(db, schema_name, type_name)? else {
        return Ok(None);
    };
    let scalar_type = user_type.column_type(false);
    match &user_type.kind {
        UserTypeKind::Range { value, .. } => {
            if !(2..=3).contains(&args.len()) {
                return Err(SqlError::undefined_function(format!(
                    "function {name} with {} arguments does not exist",
                    args.len()
                )));
            }
            let bounds = args
                .get(2)
                .map(SqlValue::to_cell)
                .unwrap_or_else(|| "[)".to_string());
            if !matches!(bounds.as_str(), "[]" | "[)" | "(]" | "()") {
                return Err(SqlError::InvalidSql(
                    "invalid range bound flags".to_string(),
                ));
            }
            let input = format!(
                "{}{},{}{}",
                &bounds[..1],
                range_constructor_bound(&args[0]),
                range_constructor_bound(&args[1]),
                &bounds[1..]
            );
            cast_value_to_user_range(SqlValue::String(input), &scalar_type, value, false).map(Some)
        }
        UserTypeKind::Multirange {
            value,
            range_schema_name,
            range_name,
            ..
        } => {
            if args.is_empty() {
                return Ok(Some(SqlValue::String("{}".to_string())));
            }
            if args.len() != 1 {
                return Err(SqlError::undefined_function(format!(
                    "function {name} with {} arguments does not exist",
                    args.len()
                )));
            }
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let expected_range = if range_schema_name == "public" {
                range_name.clone()
            } else {
                format!("{range_schema_name}.{range_name}")
            };
            let arg_type = arg_types.first().and_then(Option::as_deref);
            if arg_type.is_none() || arg_type == Some(expected_range.as_str()) {
                let range = load_user_type(db, range_schema_name, range_name)?
                    .ok_or_else(|| SqlError::undefined_type(expected_range.clone()))?
                    .column_type(false);
                let range_value = cast_value_to_user_range(args[0].clone(), &range, value, false)?;
                return cast_value_to_user_range(
                    SqlValue::String(format!("{{{}}}", range_value.to_cell())),
                    &scalar_type,
                    value,
                    true,
                )
                .map(Some);
            }
            if arg_type == Some(format!("{expected_range}[]").as_str()) {
                let SqlValue::Json(array) = &args[0] else {
                    return Err(SqlError::invalid_text_representation(
                        format!("{expected_range}[]"),
                        "invalid range array",
                    ));
                };
                let array = array
                    .get("$bicdb_array_input")
                    .and_then(|input| input.get("value"))
                    .unwrap_or(array);
                let mut elements = Vec::new();
                flatten_array_json(array, &mut elements);
                if elements.iter().any(|value| value.is_null()) {
                    return Err(SqlError::data_exception_public(
                        "22004",
                        "multirange values cannot contain null members",
                        None,
                    ));
                }
                let members = elements
                    .into_iter()
                    .map(|value| {
                        value.as_str().map(str::to_string).ok_or_else(|| {
                            SqlError::invalid_text_representation(
                                format!("{expected_range}[]"),
                                "invalid range array",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return cast_value_to_user_range(
                    SqlValue::String(format!("{{{}}}", members.join(","))),
                    &scalar_type,
                    value,
                    true,
                )
                .map(Some);
            }
            Err(SqlError::undefined_function(format!(
                "function {name} with the supplied argument does not exist"
            )))
        }
        _ => Ok(None),
    }
}

pub(crate) fn user_range_hull(left: &PgRange, right: &PgRange) -> PgRange {
    if left.empty {
        return right.clone();
    }
    if right.empty {
        return left.clone();
    }
    PgRange {
        subtype: left.subtype.clone(),
        empty: false,
        lower: if range_lower_order(&left.lower, &right.lower, &left.subtype) == Ordering::Greater {
            right.lower.clone()
        } else {
            left.lower.clone()
        },
        upper: if upper_bound_order(&left.upper, &right.upper, &left.subtype) == Ordering::Less {
            right.upper.clone()
        } else {
            left.upper.clone()
        },
    }
}

pub(crate) fn range_binary_result_pg_type(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Option<String> {
    let left_range = left_type.filter(|pg_type| is_builtin_range_type(pg_type));
    let right_range = right_type.filter(|pg_type| is_builtin_range_type(pg_type));
    let left_multirange = left_type.filter(|pg_type| is_builtin_multirange_type(pg_type));
    let right_multirange = right_type.filter(|pg_type| is_builtin_multirange_type(pg_type));
    let left_set = left_range.or(left_multirange);
    let right_set = right_range.or(right_multirange);
    let range_pair = left_range.is_some() && left_range == right_range;
    let multirange_pair = left_multirange.is_some() && left_multirange == right_multirange;
    let set_pair = left_set
        .zip(right_set)
        .is_some_and(|(left, right)| range_sets_have_same_family(left, right));
    let positional_predicate = matches!(
        op,
        BinaryOperator::PGOverlap
            | BinaryOperator::PGBitwiseShiftLeft
            | BinaryOperator::PGBitwiseShiftRight
            | BinaryOperator::AndLt
            | BinaryOperator::AndGt
    ) || matches!(op, BinaryOperator::Custom(operator) if operator == "-|-");
    if matches!(op, BinaryOperator::AtArrow) && left_set.is_some()
        || matches!(op, BinaryOperator::ArrowAt) && right_set.is_some()
        || set_pair && positional_predicate
    {
        return Some("bool".to_string());
    }
    if range_pair
        && matches!(
            op,
            BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
        )
    {
        return left_range.map(str::to_string);
    }
    if multirange_pair
        && matches!(
            op,
            BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
        )
    {
        return left_multirange.map(str::to_string);
    }
    None
}

pub(crate) fn range_element_from_value(
    range_type: &str,
    value: &SqlValue,
) -> Result<PgCanonicalValue> {
    let subtype = range_subtype_name(range_type).expect("range types have registered subtypes");
    let spec = pg_type_spec(subtype).expect("range subtypes are registered");
    crate::type_codec::canonical_value(spec, value)
}

pub(crate) fn ranges_are_adjacent(left: &PgRange, right: &PgRange) -> bool {
    !left.empty
        && !right.empty
        && (bounds_are_adjacent(&left.upper, &right.lower, &left.subtype)
            || bounds_are_adjacent(&right.upper, &left.lower, &left.subtype))
}

pub(crate) fn range_set_from_sql_value(
    value: &SqlValue,
    pg_type: &str,
) -> Result<Option<Vec<PgRange>>> {
    if is_builtin_range_type(pg_type) {
        return range_from_sql_value(value, pg_type).map(|range| range.map(|range| vec![range]));
    }
    if is_builtin_multirange_type(pg_type) {
        return multirange_from_sql_value(value, pg_type);
    }
    Ok(None)
}

pub(crate) fn range_sets_have_same_family(left_type: &str, right_type: &str) -> bool {
    range_family(left_type).is_some_and(|family| Some(family) == range_family(right_type))
}

pub(crate) fn range_set_contains(left: &[PgRange], right: &[PgRange]) -> bool {
    right
        .iter()
        .all(|right| right.empty || left.iter().any(|left| range_contains_range(left, right)))
}

pub(crate) fn range_sets_overlap(left: &[PgRange], right: &[PgRange]) -> bool {
    left.iter()
        .any(|left| right.iter().any(|right| ranges_overlap_typed(left, right)))
}

pub(crate) fn range_sets_are_adjacent(left: &[PgRange], right: &[PgRange]) -> bool {
    nonempty_range_bounds(left)
        .zip(nonempty_range_bounds(right))
        .is_some_and(|((left_first, left_last), (right_first, right_last))| {
            bounds_are_adjacent(&left_last.upper, &right_first.lower, &left_last.subtype)
                || bounds_are_adjacent(&right_last.upper, &left_first.lower, &left_first.subtype)
        })
}

pub(crate) fn range_statistics_length(range: &PgRange) -> Result<Option<String>> {
    if range.empty {
        return Ok(None);
    }
    pub(crate) fn bound_value(bound: &PgRangeBound) -> Option<&PgCanonicalValue> {
        match bound {
            PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => Some(value.as_ref()),
            PgRangeBound::Unbounded => None,
        }
    }
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return Ok(None);
    };
    let length = match (lower, upper) {
        (PgCanonicalValue::Int4(lower), PgCanonicalValue::Int4(upper)) => i64::from(*upper)
            .saturating_sub(i64::from(*lower))
            .to_string(),
        (PgCanonicalValue::Int8(lower), PgCanonicalValue::Int8(upper)) => i128::from(*upper)
            .saturating_sub(i128::from(*lower))
            .to_string(),
        (PgCanonicalValue::Numeric(lower), PgCanonicalValue::Numeric(upper)) => {
            eval_pg_numeric_arithmetic(
                SqlValue::String(upper.to_decimal_text()),
                &BinaryOperator::Minus,
                SqlValue::String(lower.to_decimal_text()),
            )?
            .to_cell()
        }
        (
            PgCanonicalValue::Date(PgDate::Finite(lower)),
            PgCanonicalValue::Date(PgDate::Finite(upper)),
        ) => i64::from(*upper)
            .saturating_sub(i64::from(*lower))
            .to_string(),
        (
            PgCanonicalValue::Timestamp(PgTimestamp::Finite(lower))
            | PgCanonicalValue::TimestampTz(PgTimestamp::Finite(lower)),
            PgCanonicalValue::Timestamp(PgTimestamp::Finite(upper))
            | PgCanonicalValue::TimestampTz(PgTimestamp::Finite(upper)),
        ) => format_range_microseconds(i128::from(*upper) - i128::from(*lower)),
        _ => return Ok(None),
    };
    Ok(Some(length))
}

pub(crate) fn format_range_microseconds(micros: i128) -> String {
    let negative = micros < 0;
    let magnitude = micros.abs();
    let seconds = magnitude / 1_000_000;
    let fraction = magnitude % 1_000_000;
    let sign = if negative { "-" } else { "" };
    if fraction == 0 {
        format!("{sign}{seconds}")
    } else {
        format!(
            "{sign}{seconds}.{}",
            format!("{fraction:06}").trim_end_matches('0')
        )
    }
}

pub(crate) fn nonempty_range_bounds(ranges: &[PgRange]) -> Option<(&PgRange, &PgRange)> {
    Some((
        ranges.iter().find(|range| !range.empty)?,
        ranges.iter().rev().find(|range| !range.empty)?,
    ))
}

pub(crate) fn empty_range(subtype: &str) -> PgRange {
    PgRange {
        subtype: subtype.to_string(),
        empty: true,
        lower: PgRangeBound::Unbounded,
        upper: PgRangeBound::Unbounded,
    }
}

pub(crate) fn intersect_range_values(left: &PgRange, right: &PgRange) -> Result<PgRange> {
    if !ranges_overlap_typed(left, right) {
        return Ok(empty_range(&left.subtype));
    }
    PgRange {
        subtype: left.subtype.clone(),
        empty: false,
        lower: if range_lower_order(&left.lower, &right.lower, &left.subtype) == Ordering::Less {
            right.lower.clone()
        } else {
            left.lower.clone()
        },
        upper: if upper_bound_order(&left.upper, &right.upper, &left.subtype) == Ordering::Greater {
            right.upper.clone()
        } else {
            left.upper.clone()
        },
    }
    .canonicalized()
    .map_err(|error| postgres_range_input_error("range", "intersection", error))
}

pub(crate) fn intersect_range_sets(left: &[PgRange], right: &[PgRange]) -> Result<Vec<PgRange>> {
    let mut intersections = Vec::new();
    for left in left {
        for right in right {
            let intersection = intersect_range_values(left, right)?;
            if !intersection.empty {
                intersections.push(intersection);
            }
        }
    }
    canonicalize_pg_multirange(intersections)
        .map_err(|error| postgres_range_input_error("multirange", "intersection", error))
}

pub(crate) fn subtract_range_value(left: &PgRange, right: &PgRange) -> Result<Vec<PgRange>> {
    if left.empty || !ranges_overlap_typed(left, right) {
        return Ok((!left.empty).then(|| left.clone()).into_iter().collect());
    }
    if range_contains_range(right, left) {
        return Ok(Vec::new());
    }
    let mut remaining = Vec::with_capacity(2);
    if range_lower_order(&right.lower, &left.lower, &left.subtype) == Ordering::Greater {
        remaining.push(PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: left.lower.clone(),
            upper: complement_as_upper(&right.lower),
        });
    }
    if upper_bound_order(&right.upper, &left.upper, &left.subtype) == Ordering::Less {
        remaining.push(PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: complement_as_lower(&right.upper),
            upper: left.upper.clone(),
        });
    }
    remaining
        .into_iter()
        .map(PgRange::canonicalized)
        .filter_map(|range| match range {
            Ok(range) if range.empty => None,
            result => Some(result),
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| postgres_range_input_error("multirange", "difference", error))
}

pub(crate) fn subtract_range_sets(left: &[PgRange], right: &[PgRange]) -> Result<Vec<PgRange>> {
    let mut remaining = left
        .iter()
        .filter(|range| !range.empty)
        .cloned()
        .collect::<Vec<_>>();
    for remove in right {
        let mut next = Vec::new();
        for candidate in &remaining {
            next.extend(subtract_range_value(candidate, remove)?);
        }
        remaining = next;
        if remaining.is_empty() {
            break;
        }
    }
    canonicalize_pg_multirange(remaining)
        .map_err(|error| postgres_range_input_error("multirange", "difference", error))
}

pub(crate) fn render_range_set(ranges: &[PgRange], multirange_type: &str) -> SqlValue {
    SqlValue::String(render_multirange_for_session(ranges, multirange_type))
}

pub(crate) fn inferred_range_type_from_value(value: &SqlValue) -> Option<&'static str> {
    let SqlValue::String(text) = value else {
        return None;
    };
    if text != "empty" && !matches!(text.as_bytes().first(), Some(b'[' | b'(')) {
        return None;
    }
    [
        "int4range",
        "int8range",
        "numrange",
        "daterange",
        "tsrange",
        "tstzrange",
    ]
    .into_iter()
    .find(|range_type| {
        matches!(
            parse_pg_canonical_special(range_type, text),
            Ok(Some(PgCanonicalValue::Range(_)))
        )
    })
}

pub(crate) fn range_operator_error(
    op: &BinaryOperator,
    left_type: &str,
    right_type: &str,
) -> SqlError {
    SqlError::undefined_function(format!(
        "operator does not exist: {left_type} {op} {right_type}"
    ))
}

pub(crate) fn is_range_set_operator(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::AtArrow
            | BinaryOperator::ArrowAt
            | BinaryOperator::PGOverlap
            | BinaryOperator::PGBitwiseShiftLeft
            | BinaryOperator::PGBitwiseShiftRight
            | BinaryOperator::AndLt
            | BinaryOperator::AndGt
            | BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
    ) || matches!(op, BinaryOperator::Custom(operator) if operator == "-|-")
}

pub(crate) fn eval_range_binary_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let mut left_set_type = match left_type {
        Some(pg_type) if is_builtin_range_type(pg_type) || is_builtin_multirange_type(pg_type) => {
            Some(pg_type)
        }
        Some(_) => None,
        None => inferred_range_type_from_value(&left),
    };
    let mut right_set_type = match right_type {
        Some(pg_type) if is_builtin_range_type(pg_type) || is_builtin_multirange_type(pg_type) => {
            Some(pg_type)
        }
        Some(_) => None,
        None => inferred_range_type_from_value(&right),
    };
    if left.to_cell() == "empty" && right_set_type.is_some_and(is_builtin_range_type) {
        left_set_type = right_set_type;
    }
    if right.to_cell() == "empty" && left_set_type.is_some_and(is_builtin_range_type) {
        right_set_type = left_set_type;
    }
    if left_set_type.is_none() && right_set_type.is_none() {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(range_binary_result_pg_type(op, left_type, right_type).map(|_| SqlValue::Null));
    }
    if matches!(op, BinaryOperator::AtArrow) {
        let Some(left_type) = left_set_type else {
            return Ok(None);
        };
        let (range_type, _) = range_family(left_type).expect("range set types have a family");
        let left = range_set_from_sql_value(&left, left_type)?.unwrap();
        let contained = if let Some(right_type) = right_set_type {
            if !range_sets_have_same_family(left_type, right_type) {
                return Err(range_operator_error(op, left_type, right_type));
            }
            let right = range_set_from_sql_value(&right, right_type)?.unwrap();
            range_set_contains(&left, &right)
        } else {
            let element = range_element_from_value(range_type, &right)?;
            left.iter()
                .any(|range| range_contains_element(range, &element))
        };
        return Ok(Some(SqlValue::Bool(contained)));
    }
    if matches!(op, BinaryOperator::ArrowAt) {
        let Some(right_type) = right_set_type else {
            return Ok(None);
        };
        let (range_type, _) = range_family(right_type).expect("range set types have a family");
        let right = range_set_from_sql_value(&right, right_type)?.unwrap();
        let contained = if let Some(left_type) = left_set_type {
            if !range_sets_have_same_family(left_type, right_type) {
                return Err(range_operator_error(op, left_type, right_type));
            }
            let left = range_set_from_sql_value(&left, left_type)?.unwrap();
            range_set_contains(&right, &left)
        } else {
            let element = range_element_from_value(range_type, &left)?;
            right
                .iter()
                .any(|range| range_contains_element(range, &element))
        };
        return Ok(Some(SqlValue::Bool(contained)));
    }
    let Some((left_type, right_type)) = left_set_type
        .zip(right_set_type)
        .filter(|(left, right)| range_sets_have_same_family(left, right))
    else {
        if let (Some(left_type), Some(right_type)) = (left_set_type, right_set_type) {
            if is_range_set_operator(op) {
                return Err(range_operator_error(op, left_type, right_type));
            }
        }
        return Ok(None);
    };
    let (range_type, multirange_type) =
        range_family(left_type).expect("range set types have a family");
    if left_type == right_type
        && matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
        )
    {
        let ordering = pg_typed_compare(left_type, &left, &right)?;
        return Ok(Some(SqlValue::Bool(comparison_from_ordering(op, ordering))));
    }
    if left_type != right_type
        && matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
        )
    {
        return Err(range_operator_error(op, left_type, right_type));
    }
    let left = range_set_from_sql_value(&left, left_type)?.unwrap();
    let right = range_set_from_sql_value(&right, right_type)?.unwrap();
    let left_bounds = nonempty_range_bounds(&left);
    let right_bounds = nonempty_range_bounds(&right);
    let result = match op {
        BinaryOperator::PGOverlap => SqlValue::Bool(range_sets_overlap(&left, &right)),
        BinaryOperator::Custom(operator) if operator == "-|-" => {
            SqlValue::Bool(range_sets_are_adjacent(&left, &right))
        }
        BinaryOperator::PGBitwiseShiftLeft => {
            SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
                |((_, left_last), (right_first, _))| {
                    upper_ends_before_lower(
                        &left_last.upper,
                        &right_first.lower,
                        &left_last.subtype,
                    )
                },
            ))
        }
        BinaryOperator::PGBitwiseShiftRight => {
            SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
                |((left_first, _), (_, right_last))| {
                    upper_ends_before_lower(
                        &right_last.upper,
                        &left_first.lower,
                        &left_first.subtype,
                    )
                },
            ))
        }
        BinaryOperator::AndLt => SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
            |((_, left_last), (_, right_last))| {
                upper_bound_order(&left_last.upper, &right_last.upper, &left_last.subtype)
                    != Ordering::Greater
            },
        )),
        BinaryOperator::AndGt => SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
            |((left_first, _), (right_first, _))| {
                range_lower_order(&left_first.lower, &right_first.lower, &left_first.subtype)
                    != Ordering::Less
            },
        )),
        BinaryOperator::Multiply if left_type == right_type && is_builtin_range_type(left_type) => {
            return range_intersection(&left[0], &right[0], range_type).map(Some);
        }
        BinaryOperator::Plus if left_type == right_type && is_builtin_range_type(left_type) => {
            if !left[0].empty
                && !right[0].empty
                && !ranges_overlap_typed(&left[0], &right[0])
                && !ranges_are_adjacent(&left[0], &right[0])
            {
                return Err(SqlError::data_exception_public(
                    "22000",
                    "result of range union would not be contiguous",
                    None,
                ));
            }
            return range_hull(&left[0], &right[0], range_type).map(Some);
        }
        BinaryOperator::Minus if left_type == right_type && is_builtin_range_type(left_type) => {
            return range_difference(&left[0], &right[0], range_type).map(Some);
        }
        BinaryOperator::Plus if left_type == right_type => {
            let ranges =
                canonicalize_pg_multirange(left.iter().chain(&right).cloned().collect::<Vec<_>>())
                    .map_err(|error| postgres_range_input_error(multirange_type, "union", error))?;
            return Ok(Some(render_range_set(&ranges, multirange_type)));
        }
        BinaryOperator::Multiply if left_type == right_type => {
            let ranges = intersect_range_sets(&left, &right)?;
            return Ok(Some(render_range_set(&ranges, multirange_type)));
        }
        BinaryOperator::Minus if left_type == right_type => {
            let ranges = subtract_range_sets(&left, &right)?;
            return Ok(Some(render_range_set(&ranges, multirange_type)));
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

#[derive(Clone)]
pub(crate) struct UserRangeSetType {
    user_type: UserTypeSchema,
    value: UserRangeValueSchema,
    range_oid: i64,
    multirange: bool,
}

pub(crate) fn user_range_set_type(
    db: &BicDb,
    pg_type: Option<&str>,
) -> Result<Option<UserRangeSetType>> {
    let Some(pg_type) = pg_type else {
        return Ok(None);
    };
    if pg_type.ends_with("[]") {
        return Ok(None);
    }
    let (schema_name, type_name) = pg_type
        .rsplit_once('.')
        .map(|(schema, name)| (schema, name))
        .unwrap_or(("public", pg_type));
    let Some(user_type) = load_user_type(db, schema_name, type_name)? else {
        return Ok(None);
    };
    let (value, range_oid, multirange) = match &user_type.kind {
        UserTypeKind::Range { value, .. } => (value.clone(), user_type.oid, false),
        UserTypeKind::Multirange {
            value, range_oid, ..
        } => (value.clone(), *range_oid, true),
        _ => return Ok(None),
    };
    Ok(Some(UserRangeSetType {
        user_type,
        value,
        range_oid,
        multirange,
    }))
}

pub(crate) fn user_range_set_values(
    db: &BicDb,
    descriptor: &UserRangeSetType,
    value: &SqlValue,
) -> Result<Vec<PgRange>> {
    pg_user_range_values(
        db,
        i32::try_from(descriptor.user_type.oid)
            .map_err(|_| SqlError::numeric_value_out_of_range("user range OID exceeds int4"))?,
        value,
    )?
    .ok_or_else(|| {
        SqlError::undefined_type(descriptor.user_type.column_type(false).formatted_name())
    })
}

pub(crate) fn user_range_result(
    descriptor: &UserRangeSetType,
    ranges: Vec<PgRange>,
) -> Result<SqlValue> {
    let pg_type = descriptor.user_type.column_type(false);
    let text = if descriptor.multirange {
        let ranges =
            canonicalize_pg_multirange_with_policy(ranges, descriptor.value.canonical_discrete)
                .map_err(|error| {
                    postgres_range_input_error(
                        &pg_type.formatted_name(),
                        "multirange result",
                        error,
                    )
                })?;
        render_multirange_for_session(&ranges, &pg_type.formatted_name())
    } else {
        let range = ranges.into_iter().next().unwrap_or(PgRange {
            subtype: descriptor.value.subtype.clone(),
            empty: true,
            lower: PgRangeBound::Unbounded,
            upper: PgRangeBound::Unbounded,
        });
        render_range_for_session(&range, &pg_type.formatted_name())
    };
    cast_value_to_user_range(
        SqlValue::String(text),
        &pg_type,
        &descriptor.value,
        descriptor.multirange,
    )
}

pub(crate) fn user_range_intersection(left: &PgRange, right: &PgRange) -> PgRange {
    if !ranges_overlap_typed(left, right) {
        return PgRange {
            subtype: left.subtype.clone(),
            empty: true,
            lower: PgRangeBound::Unbounded,
            upper: PgRangeBound::Unbounded,
        };
    }
    PgRange {
        subtype: left.subtype.clone(),
        empty: false,
        lower: if range_lower_order(&left.lower, &right.lower, &left.subtype) == Ordering::Less {
            right.lower.clone()
        } else {
            left.lower.clone()
        },
        upper: if upper_bound_order(&left.upper, &right.upper, &left.subtype) == Ordering::Greater {
            right.upper.clone()
        } else {
            left.upper.clone()
        },
    }
}

pub(crate) fn subtract_user_range_value(
    left: &PgRange,
    right: &PgRange,
    canonicalize_discrete: bool,
) -> Result<Vec<PgRange>> {
    if left.empty || !ranges_overlap_typed(left, right) {
        return Ok((!left.empty).then(|| left.clone()).into_iter().collect());
    }
    if range_contains_range(right, left) {
        return Ok(Vec::new());
    }
    let mut remaining = Vec::with_capacity(2);
    if range_lower_order(&right.lower, &left.lower, &left.subtype) == Ordering::Greater {
        remaining.push(PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: left.lower.clone(),
            upper: complement_as_upper(&right.lower),
        });
    }
    if upper_bound_order(&right.upper, &left.upper, &left.subtype) == Ordering::Less {
        remaining.push(PgRange {
            subtype: left.subtype.clone(),
            empty: false,
            lower: complement_as_lower(&right.upper),
            upper: left.upper.clone(),
        });
    }
    remaining
        .into_iter()
        .map(|range| range.canonicalized_with_policy(canonicalize_discrete))
        .filter_map(|range| match range {
            Ok(range) if range.empty => None,
            result => Some(result),
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| postgres_range_input_error("range", "difference", error))
}

pub(crate) fn eval_range_binary_value_with_db(
    db: &BicDb,
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_user = user_range_set_type(db, left_type)?;
    let right_user = user_range_set_type(db, right_type)?;
    if left_user.is_none() && right_user.is_none() {
        return eval_range_binary_value(left, op, right, left_type, right_type);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(is_range_set_operator(op).then_some(SqlValue::Null));
    }
    if matches!(op, BinaryOperator::AtArrow) {
        let Some(left_descriptor) = left_user.as_ref() else {
            return Err(range_operator_error(
                op,
                left_type.unwrap_or("unknown"),
                right_type.unwrap_or("unknown"),
            ));
        };
        let left_ranges = user_range_set_values(db, left_descriptor, &left)?;
        let contained = if let Some(right_descriptor) = right_user.as_ref() {
            if left_descriptor.range_oid != right_descriptor.range_oid {
                return Err(range_operator_error(
                    op,
                    left_type.unwrap_or("unknown"),
                    right_type.unwrap_or("unknown"),
                ));
            }
            range_set_contains(
                &left_ranges,
                &user_range_set_values(db, right_descriptor, &right)?,
            )
        } else {
            let spec = pg_type_spec(&left_descriptor.value.subtype)
                .ok_or_else(|| SqlError::undefined_type(left_descriptor.value.subtype.clone()))?;
            let element = crate::type_codec::canonical_value(spec, &right)?;
            left_ranges
                .iter()
                .any(|range| range_contains_element(range, &element))
        };
        return Ok(Some(SqlValue::Bool(contained)));
    }
    if matches!(op, BinaryOperator::ArrowAt) {
        let Some(right_descriptor) = right_user.as_ref() else {
            return Err(range_operator_error(
                op,
                left_type.unwrap_or("unknown"),
                right_type.unwrap_or("unknown"),
            ));
        };
        let right_ranges = user_range_set_values(db, right_descriptor, &right)?;
        let contained = if let Some(left_descriptor) = left_user.as_ref() {
            if left_descriptor.range_oid != right_descriptor.range_oid {
                return Err(range_operator_error(
                    op,
                    left_type.unwrap_or("unknown"),
                    right_type.unwrap_or("unknown"),
                ));
            }
            range_set_contains(
                &right_ranges,
                &user_range_set_values(db, left_descriptor, &left)?,
            )
        } else {
            let spec = pg_type_spec(&right_descriptor.value.subtype)
                .ok_or_else(|| SqlError::undefined_type(right_descriptor.value.subtype.clone()))?;
            let element = crate::type_codec::canonical_value(spec, &left)?;
            right_ranges
                .iter()
                .any(|range| range_contains_element(range, &element))
        };
        return Ok(Some(SqlValue::Bool(contained)));
    }
    let (Some(left_descriptor), Some(right_descriptor)) = (left_user.as_ref(), right_user.as_ref())
    else {
        return Err(range_operator_error(
            op,
            left_type.unwrap_or("unknown"),
            right_type.unwrap_or("unknown"),
        ));
    };
    if left_descriptor.range_oid != right_descriptor.range_oid {
        return Err(range_operator_error(
            op,
            left_type.unwrap_or("unknown"),
            right_type.unwrap_or("unknown"),
        ));
    }
    if left_descriptor.user_type.oid == right_descriptor.user_type.oid
        && matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
        )
    {
        let ordering = pg_typed_compare_for_db(db, left_type.unwrap(), &left, &right)?;
        return Ok(Some(SqlValue::Bool(comparison_from_ordering(op, ordering))));
    }
    if left_descriptor.multirange != right_descriptor.multirange
        && matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
        )
    {
        return Err(range_operator_error(
            op,
            left_type.unwrap(),
            right_type.unwrap(),
        ));
    }
    let left_ranges = user_range_set_values(db, left_descriptor, &left)?;
    let right_ranges = user_range_set_values(db, right_descriptor, &right)?;
    let left_bounds = nonempty_range_bounds(&left_ranges);
    let right_bounds = nonempty_range_bounds(&right_ranges);
    let value = match op {
        BinaryOperator::PGOverlap => {
            SqlValue::Bool(range_sets_overlap(&left_ranges, &right_ranges))
        }
        BinaryOperator::Custom(operator) if operator == "-|-" => {
            SqlValue::Bool(range_sets_are_adjacent(&left_ranges, &right_ranges))
        }
        BinaryOperator::PGBitwiseShiftLeft => {
            SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
                |((_, left_last), (right_first, _))| {
                    upper_ends_before_lower(
                        &left_last.upper,
                        &right_first.lower,
                        &left_last.subtype,
                    )
                },
            ))
        }
        BinaryOperator::PGBitwiseShiftRight => {
            SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
                |((left_first, _), (_, right_last))| {
                    upper_ends_before_lower(
                        &right_last.upper,
                        &left_first.lower,
                        &left_first.subtype,
                    )
                },
            ))
        }
        BinaryOperator::AndLt => SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
            |((_, left_last), (_, right_last))| {
                upper_bound_order(&left_last.upper, &right_last.upper, &left_last.subtype)
                    != Ordering::Greater
            },
        )),
        BinaryOperator::AndGt => SqlValue::Bool(left_bounds.zip(right_bounds).is_some_and(
            |((left_first, _), (right_first, _))| {
                range_lower_order(&left_first.lower, &right_first.lower, &left_first.subtype)
                    != Ordering::Less
            },
        )),
        BinaryOperator::Plus if !left_descriptor.multirange => {
            if !left_ranges[0].empty
                && !right_ranges[0].empty
                && !ranges_overlap_typed(&left_ranges[0], &right_ranges[0])
                && !ranges_are_adjacent(&left_ranges[0], &right_ranges[0])
            {
                return Err(SqlError::data_exception_public(
                    "22000",
                    "result of range union would not be contiguous",
                    None,
                ));
            }
            return user_range_result(
                left_descriptor,
                vec![user_range_hull(&left_ranges[0], &right_ranges[0])],
            )
            .map(Some);
        }
        BinaryOperator::Multiply if !left_descriptor.multirange => {
            return user_range_result(
                left_descriptor,
                vec![user_range_intersection(&left_ranges[0], &right_ranges[0])],
            )
            .map(Some);
        }
        BinaryOperator::Minus if !left_descriptor.multirange => {
            let ranges = subtract_user_range_value(
                &left_ranges[0],
                &right_ranges[0],
                left_descriptor.value.canonical_discrete,
            )?;
            if ranges.len() > 1 {
                return Err(SqlError::data_exception_public(
                    "22000",
                    "result of range difference would not be contiguous",
                    None,
                ));
            }
            return user_range_result(left_descriptor, ranges).map(Some);
        }
        BinaryOperator::Plus if left_descriptor.multirange => {
            return user_range_result(
                left_descriptor,
                left_ranges.into_iter().chain(right_ranges).collect(),
            )
            .map(Some);
        }
        BinaryOperator::Multiply if left_descriptor.multirange => {
            let mut intersections = Vec::new();
            for left in &left_ranges {
                for right in &right_ranges {
                    let intersection = user_range_intersection(left, right);
                    if !intersection.empty {
                        intersections.push(intersection);
                    }
                }
            }
            return user_range_result(left_descriptor, intersections).map(Some);
        }
        BinaryOperator::Minus if left_descriptor.multirange => {
            let mut remaining = left_ranges;
            for remove in &right_ranges {
                let mut next = Vec::new();
                for candidate in &remaining {
                    next.extend(subtract_user_range_value(
                        candidate,
                        remove,
                        left_descriptor.value.canonical_discrete,
                    )?);
                }
                remaining = next;
            }
            return user_range_result(left_descriptor, remaining).map(Some);
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn eval_unary_bit_not_expr_value(
    op: &UnaryOperator,
    expr: &Expr,
    value: SqlValue,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let pg_type = projected_expr_pg_type(expr, schema);
    if let Some(value) = eval_geometric_unary_value(op, &value, pg_type.as_deref())? {
        return Ok(value);
    }
    if matches!(op, UnaryOperator::PGPrefixFactorial) && pg_type.as_deref() == Some("tsquery") {
        return tsquery_from_sql_value(&value)
            .map(PgTsQuery::not)
            .map(SqlValue::TsQuery);
    }
    if matches!(op, UnaryOperator::BitwiseNot) {
        if let Some(pg_type @ ("inet" | "cidr")) = pg_type.as_deref() {
            let network = network_argument(&value, pg_type)?;
            return Ok(network_value(PgNetwork {
                kind: PgNetworkKind::Inet,
                address: map_ip_octets(network.address, |octet| !octet),
                prefix: network.prefix,
            }));
        }
        if let Some(pg_type @ ("macaddr" | "macaddr8")) = pg_type.as_deref() {
            return Ok(mac_value(map_mac_octets(
                mac_argument(&value, pg_type)?,
                |octet| !octet,
            )));
        }
    }
    if !matches!(op, UnaryOperator::BitwiseNot)
        || (!matches!(pg_type.as_deref(), Some("bit" | "varbit"))
            && PgBitString::from_bit_text(&value.to_cell()).is_err())
    {
        return Err(SqlError::undefined_function(
            "operator does not exist: ~ unknown",
        ));
    }
    let bits = bit_argument(&value)?.unwrap();
    Ok(SqlValue::String(
        bits.to_bit_text()
            .chars()
            .map(|bit| if bit == '0' { '1' } else { '0' })
            .collect(),
    ))
}

pub(crate) fn network_argument(value: &SqlValue, pg_type: &str) -> Result<PgNetwork> {
    let kind = if pg_type == "cidr" {
        PgNetworkKind::Cidr
    } else {
        PgNetworkKind::Inet
    };
    PgNetwork::from_postgres_text(&value.to_cell(), kind).map_err(|_| {
        SqlError::invalid_text_representation(pg_type, format!("\"{}\"", value.to_cell()))
    })
}

pub(crate) fn mac_argument(value: &SqlValue, pg_type: &str) -> Result<PgMacAddress> {
    PgMacAddress::from_postgres_text(&value.to_cell(), pg_type == "macaddr8").map_err(|_| {
        SqlError::invalid_text_representation(pg_type, format!("\"{}\"", value.to_cell()))
    })
}

pub(crate) fn network_value(network: PgNetwork) -> SqlValue {
    SqlValue::String(network.to_postgres_text())
}

pub(crate) fn mac_value(address: PgMacAddress) -> SqlValue {
    SqlValue::String(address.to_postgres_text())
}

pub(crate) fn map_ip_octets(address: PgIpAddress, mut map: impl FnMut(u8) -> u8) -> PgIpAddress {
    match address {
        PgIpAddress::V4(mut octets) => {
            octets.iter_mut().for_each(|octet| *octet = map(*octet));
            PgIpAddress::V4(octets)
        }
        PgIpAddress::V6(mut octets) => {
            octets.iter_mut().for_each(|octet| *octet = map(*octet));
            PgIpAddress::V6(octets)
        }
    }
}

pub(crate) fn map_mac_octets(address: PgMacAddress, mut map: impl FnMut(u8) -> u8) -> PgMacAddress {
    match address {
        PgMacAddress::Mac48(mut octets) => {
            octets.iter_mut().for_each(|octet| *octet = map(*octet));
            PgMacAddress::Mac48(octets)
        }
        PgMacAddress::Mac64(mut octets) => {
            octets.iter_mut().for_each(|octet| *octet = map(*octet));
            PgMacAddress::Mac64(octets)
        }
    }
}

pub(crate) fn combine_ip_octets(
    left: PgIpAddress,
    right: PgIpAddress,
    mut combine: impl FnMut(u8, u8) -> u8,
) -> Option<PgIpAddress> {
    match (left, right) {
        (PgIpAddress::V4(mut left), PgIpAddress::V4(right)) => {
            left.iter_mut()
                .zip(right)
                .for_each(|(left, right)| *left = combine(*left, right));
            Some(PgIpAddress::V4(left))
        }
        (PgIpAddress::V6(mut left), PgIpAddress::V6(right)) => {
            left.iter_mut()
                .zip(right)
                .for_each(|(left, right)| *left = combine(*left, right));
            Some(PgIpAddress::V6(left))
        }
        _ => None,
    }
}

pub(crate) fn combine_mac_octets(
    left: PgMacAddress,
    right: PgMacAddress,
    mut combine: impl FnMut(u8, u8) -> u8,
) -> Option<PgMacAddress> {
    match (left, right) {
        (PgMacAddress::Mac48(mut left), PgMacAddress::Mac48(right)) => {
            left.iter_mut()
                .zip(right)
                .for_each(|(left, right)| *left = combine(*left, right));
            Some(PgMacAddress::Mac48(left))
        }
        (PgMacAddress::Mac64(mut left), PgMacAddress::Mac64(right)) => {
            left.iter_mut()
                .zip(right)
                .for_each(|(left, right)| *left = combine(*left, right));
            Some(PgMacAddress::Mac64(left))
        }
        _ => None,
    }
}

pub(crate) fn ip_address_value(address: PgIpAddress) -> u128 {
    match address {
        PgIpAddress::V4(octets) => u128::from(u32::from_be_bytes(octets)),
        PgIpAddress::V6(octets) => u128::from_be_bytes(octets),
    }
}

pub(crate) fn ip_address_from_value(template: PgIpAddress, value: u128) -> PgIpAddress {
    match template {
        PgIpAddress::V4(_) => PgIpAddress::V4((value as u32).to_be_bytes()),
        PgIpAddress::V6(_) => PgIpAddress::V6(value.to_be_bytes()),
    }
}

pub(crate) fn same_ip_family(left: PgIpAddress, right: PgIpAddress) -> bool {
    matches!(
        (left, right),
        (PgIpAddress::V4(_), PgIpAddress::V4(_)) | (PgIpAddress::V6(_), PgIpAddress::V6(_))
    )
}

pub(crate) fn address_mask(bit_len: u8, prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else if bit_len == 128 {
        u128::MAX << (128 - prefix)
    } else {
        u128::from(u32::MAX << (32 - prefix))
    }
}

pub(crate) fn masked_ip_address(address: PgIpAddress, prefix: u8) -> PgIpAddress {
    let value = ip_address_value(address) & address_mask(address.bit_len(), prefix);
    ip_address_from_value(address, value)
}

pub(crate) fn network_contains(left: PgNetwork, right: PgNetwork, strict: bool) -> bool {
    same_ip_family(left.address, right.address)
        && if strict {
            left.prefix < right.prefix
        } else {
            left.prefix <= right.prefix
        }
        && masked_ip_address(left.address, left.prefix)
            == masked_ip_address(right.address, left.prefix)
}

pub(crate) fn network_overlap(left: PgNetwork, right: PgNetwork) -> bool {
    if !same_ip_family(left.address, right.address) {
        return false;
    }
    let prefix = left.prefix.min(right.prefix);
    masked_ip_address(left.address, prefix) == masked_ip_address(right.address, prefix)
}

pub(crate) fn network_masklen_inclusion_cmp(left: PgNetwork, right: PgNetwork, code: i8) -> i8 {
    let order = i16::from(left.prefix) - i16::from(right.prefix);
    if (order > 0 && code >= 0)
        || (order == 0 && (-1..=1).contains(&code))
        || (order < 0 && code <= 0)
    {
        0
    } else {
        code
    }
}

pub(crate) fn network_common_prefix_bits(left: PgNetwork, right: PgNetwork, limit: u8) -> u8 {
    if !same_ip_family(left.address, right.address) || limit == 0 {
        return 0;
    }
    let family_padding = 128 - u32::from(left.address.bit_len());
    let common = (ip_address_value(left.address) ^ ip_address_value(right.address))
        .leading_zeros()
        .saturating_sub(family_padding) as u8;
    common.min(limit)
}

pub(crate) fn network_inclusion_cmp(left: PgNetwork, right: PgNetwork, code: i8) -> i8 {
    if !same_ip_family(left.address, right.address) {
        return if matches!(left.address, PgIpAddress::V4(_)) {
            -1
        } else {
            1
        };
    }
    let common_bits = left.prefix.min(right.prefix);
    let common = network_common_prefix_bits(left, right, common_bits);
    if common < common_bits {
        let shift = u32::from(left.address.bit_len() - common - 1);
        let left_bit = (ip_address_value(left.address) >> shift) & 1;
        let right_bit = (ip_address_value(right.address) >> shift) & 1;
        return if left_bit < right_bit { -1 } else { 1 };
    }
    network_masklen_inclusion_cmp(left, right, code)
}

pub(crate) fn network_histogram_match_divider(
    boundary: PgNetwork,
    query: PgNetwork,
    code: i8,
) -> i16 {
    if !same_ip_family(boundary.address, query.address)
        || network_masklen_inclusion_cmp(boundary, query, code) != 0
    {
        return -1;
    }
    let min_bits = boundary.prefix.min(query.prefix);
    let decisive_bits = if code < 0 {
        boundary.prefix
    } else if code > 0 {
        query.prefix
    } else {
        min_bits
    };
    i16::from(decisive_bits) - i16::from(network_common_prefix_bits(boundary, query, min_bits))
}

pub(crate) fn network_histogram_selectivity(
    samples: &[String],
    query: &SqlValue,
    op: &BinaryOperator,
    sample_type: &str,
    query_type: &str,
) -> Result<Option<f64>> {
    let code = match op.to_string().as_str() {
        ">>" => -2,
        ">>=" => -1,
        "&&" => 0,
        "<<=" => 1,
        "<<" => 2,
        _ => return Ok(None),
    };
    if samples.len() <= 1 {
        return Ok(Some(0.0));
    }
    let query = network_argument(query, query_type)?;
    let mut matched = 0.0;
    for bucket in samples.windows(2) {
        let left = network_argument(&SqlValue::String(bucket[0].clone()), sample_type)?;
        let right = network_argument(&SqlValue::String(bucket[1].clone()), sample_type)?;
        let left_order = network_inclusion_cmp(left, query, code);
        let right_order = network_inclusion_cmp(right, query, code);
        if left_order == 0 && right_order == 0 {
            matched += 1.0;
        } else if (left_order <= 0 && right_order >= 0) || (left_order >= 0 && right_order <= 0) {
            let divider = network_histogram_match_divider(left, query, code)
                .max(network_histogram_match_divider(right, query, code));
            if divider >= 0 {
                matched += 2_f64.powi(-i32::from(divider));
            }
        }
    }
    Ok(Some(matched / (samples.len() - 1) as f64))
}

pub(crate) fn network_add(network: PgNetwork, addend: i64) -> Result<PgNetwork> {
    let value = ip_address_value(network.address);
    let value = if addend >= 0 {
        value.checked_add(addend as u128)
    } else {
        value.checked_sub(u128::from(addend.unsigned_abs()))
    }
    .filter(|value| network.address.bit_len() == 128 || *value <= u128::from(u32::MAX))
    .ok_or_else(|| SqlError::numeric_value_out_of_range("result is out of range"))?;
    Ok(PgNetwork {
        kind: PgNetworkKind::Inet,
        address: ip_address_from_value(network.address, value),
        prefix: network.prefix,
    })
}

pub(crate) fn network_difference(left: PgNetwork, right: PgNetwork) -> Result<i64> {
    if !same_ip_family(left.address, right.address) {
        return Err(SqlError::invalid_parameter_value(
            "cannot subtract inet values of different sizes",
        ));
    }
    let left = ip_address_value(left.address);
    let right = ip_address_value(right.address);
    if left >= right {
        i64::try_from(left - right)
            .map_err(|_| SqlError::numeric_value_out_of_range("result is out of range"))
    } else {
        let magnitude = right - left;
        if magnitude == (1_u128 << 63) {
            Ok(i64::MIN)
        } else {
            i64::try_from(magnitude)
                .map(|value| -value)
                .map_err(|_| SqlError::numeric_value_out_of_range("result is out of range"))
        }
    }
}

pub(crate) fn pg_lsn_argument(value: &SqlValue) -> Result<u64> {
    match parse_pg_canonical_special("pg_lsn", &value.to_cell()) {
        Ok(Some(PgCanonicalValue::Lsn(value))) => Ok(value),
        _ => Err(SqlError::invalid_text_representation(
            "pg_lsn",
            format!(
                "invalid input syntax for type pg_lsn: \"{}\"",
                value.to_cell()
            ),
        )),
    }
}

pub(crate) fn pg_lsn_with_numeric_offset(lsn: u64, offset: Decimal, subtract: bool) -> Result<u64> {
    let divisor = pow10_bigint(offset.scale);
    let base = BigInt::from(lsn) * &divisor;
    let scaled = if subtract {
        base - offset.mantissa
    } else {
        base + offset.mantissa
    };
    if scaled.is_negative() {
        return Err(SqlError::invalid_parameter_value("pg_lsn out of range"));
    }
    let mut rounded = &scaled / &divisor;
    let remainder = &scaled % &divisor;
    if remainder * 2 >= divisor {
        rounded += 1;
    }
    rounded
        .to_u64()
        .ok_or_else(|| SqlError::invalid_parameter_value("pg_lsn out of range"))
}

pub(crate) fn is_numeric_pg_type(pg_type: &str) -> bool {
    matches!(pg_type, "int2" | "int4" | "int8" | "numeric")
}

pub(crate) fn pg_lsn_binary_result_pg_type(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Option<&'static str> {
    let left_lsn = left_type == Some("pg_lsn");
    let right_lsn = right_type == Some("pg_lsn");
    let left_numeric = left_type.is_some_and(is_numeric_pg_type);
    let right_numeric = right_type.is_some_and(is_numeric_pg_type);
    match op {
        BinaryOperator::Minus if left_lsn && right_lsn => Some("numeric"),
        BinaryOperator::Plus if (left_lsn && right_numeric) || (left_numeric && right_lsn) => {
            Some("pg_lsn")
        }
        BinaryOperator::Minus if left_lsn && right_numeric => Some("pg_lsn"),
        _ => None,
    }
}

pub(crate) fn eval_pg_lsn_binary_value(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let Some(result_type) = pg_lsn_binary_result_pg_type(op, left_type, right_type) else {
        return Ok(None);
    };
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    if result_type == "numeric" {
        return Ok(Some(SqlValue::String(
            (BigInt::from(pg_lsn_argument(left)?) - BigInt::from(pg_lsn_argument(right)?))
                .to_string(),
        )));
    }

    let (lsn, offset, subtract) = if left_type == Some("pg_lsn") {
        (
            pg_lsn_argument(left)?,
            decimal_value(right).transpose()?.ok_or_else(|| {
                SqlError::invalid_text_representation(
                    "numeric",
                    format!(
                        "invalid input syntax for type numeric: \"{}\"",
                        right.to_cell()
                    ),
                )
            })?,
            matches!(op, BinaryOperator::Minus),
        )
    } else {
        (
            pg_lsn_argument(right)?,
            decimal_value(left).transpose()?.ok_or_else(|| {
                SqlError::invalid_text_representation(
                    "numeric",
                    format!(
                        "invalid input syntax for type numeric: \"{}\"",
                        left.to_cell()
                    ),
                )
            })?,
            false,
        )
    };
    let result = pg_lsn_with_numeric_offset(lsn, offset, subtract)?;
    Ok(Some(SqlValue::String(format_pg_lsn(result))))
}

pub(crate) fn network_binary_result_pg_type(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Option<String> {
    let left_network = matches!(left_type, Some("inet" | "cidr"));
    let right_network = matches!(right_type, Some("inet" | "cidr"));
    let left_integer = matches!(left_type, Some("int2" | "int4" | "int8"));
    let right_integer = matches!(right_type, Some("int2" | "int4" | "int8"));
    let left_mac = matches!(left_type, Some("macaddr" | "macaddr8"));
    let right_mac = left_type == right_type && left_mac;
    let operator = op.to_string();
    if (left_network && right_network)
        && matches!(
            operator.as_str(),
            "<<" | "<<=" | ">>" | ">>=" | "&&" | "=" | "<>" | "<" | "<=" | ">" | ">="
        )
    {
        return Some("bool".to_string());
    }
    if left_network
        && right_network
        && matches!(op, BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseOr)
    {
        return Some("inet".to_string());
    }
    if left_mac && right_mac && matches!(op, BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseOr)
    {
        return left_type.map(str::to_string);
    }
    if matches!(op, BinaryOperator::Plus)
        && ((left_network && right_integer) || (left_integer && right_network))
    {
        return Some("inet".to_string());
    }
    if matches!(op, BinaryOperator::Minus) && left_network {
        if right_network {
            return Some("int8".to_string());
        }
        if right_integer {
            return Some("inet".to_string());
        }
    }
    None
}

pub(crate) fn eval_network_binary_value(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let Some(result_type) = network_binary_result_pg_type(op, left_type, right_type) else {
        return Ok(None);
    };
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    if matches!(left_type, Some("macaddr" | "macaddr8")) {
        let left = mac_argument(left, left_type.unwrap())?;
        let right = mac_argument(right, right_type.unwrap())?;
        let address = match op {
            BinaryOperator::BitwiseAnd => combine_mac_octets(left, right, |a, b| a & b),
            BinaryOperator::BitwiseOr => combine_mac_octets(left, right, |a, b| a | b),
            _ => None,
        }
        .ok_or_else(|| {
            SqlError::undefined_function("operator does not exist for MAC address widths")
        })?;
        return Ok(Some(mac_value(address)));
    }
    if matches!(left_type, Some("int2" | "int4" | "int8")) {
        let network = network_argument(right, right_type.unwrap())?;
        let SqlValue::Int(addend) = left else {
            return Err(SqlError::invalid_parameter_value(
                "inet arithmetic requires bigint",
            ));
        };
        return network_add(network, *addend).map(network_value).map(Some);
    }
    let left_network = network_argument(left, left_type.unwrap())?;
    if matches!(right_type, Some("int2" | "int4" | "int8")) {
        let SqlValue::Int(addend) = right else {
            return Err(SqlError::invalid_parameter_value(
                "inet arithmetic requires bigint",
            ));
        };
        let addend = if matches!(op, BinaryOperator::Minus) {
            addend
                .checked_neg()
                .ok_or_else(|| SqlError::numeric_value_out_of_range("result is out of range"))?
        } else {
            *addend
        };
        return network_add(left_network, addend)
            .map(network_value)
            .map(Some);
    }
    let right_network = network_argument(right, right_type.unwrap())?;
    let operator = op.to_string();
    let value = match operator.as_str() {
        "<<" => SqlValue::Bool(network_contains(right_network, left_network, true)),
        "<<=" => SqlValue::Bool(network_contains(right_network, left_network, false)),
        ">>" => SqlValue::Bool(network_contains(left_network, right_network, true)),
        ">>=" => SqlValue::Bool(network_contains(left_network, right_network, false)),
        "&&" => SqlValue::Bool(network_overlap(left_network, right_network)),
        _ if result_type == "int8" => {
            SqlValue::Int(network_difference(left_network, right_network)?)
        }
        _ if result_type == "inet" => {
            if !same_ip_family(left_network.address, right_network.address) {
                let verb = if matches!(op, BinaryOperator::BitwiseAnd) {
                    "AND"
                } else {
                    "OR"
                };
                return Err(SqlError::invalid_parameter_value(format!(
                    "cannot {verb} inet values of different sizes"
                )));
            }
            let address = combine_ip_octets(left_network.address, right_network.address, |a, b| {
                if matches!(op, BinaryOperator::BitwiseAnd) {
                    a & b
                } else {
                    a | b
                }
            })
            .expect("matching IP families");
            network_value(PgNetwork {
                kind: PgNetworkKind::Inet,
                address,
                prefix: left_network.prefix.max(right_network.prefix),
            })
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn eval_binary_borrowed(
    left: std::borrow::Cow<'_, SqlValue>,
    op: &BinaryOperator,
    right: std::borrow::Cow<'_, SqlValue>,
) -> Result<SqlValue> {
    match op {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo
            if !matches!(
                (op, left.as_ref()),
                (BinaryOperator::Minus, SqlValue::Json(_))
            ) =>
        {
            eval_arithmetic_value_ref(&left, op, &right)
        }
        _ => eval_binary_value(left.into_owned(), op, right.into_owned()),
    }
}

pub(crate) fn eval_binary_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    if let Some(metric) = vector_distance_operator(op) {
        if metric == "<->" {
            if let Some(value) = eval_untyped_geometric_distance(&left, &right)? {
                return Ok(value);
            }
        }
        return eval_vector_distance_value(left, metric, right);
    }
    match op {
        BinaryOperator::Minus if matches!(left, SqlValue::Json(_)) => {
            eval_json_delete_value(left, right)
        }
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => eval_arithmetic_value(left, op, right),
        BinaryOperator::StringConcat => {
            if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                Ok(SqlValue::Null)
            } else if let Some(value) = eval_json_concat(&left, &right) {
                Ok(value)
            } else {
                Ok(SqlValue::String(format!(
                    "{}{}",
                    left.to_cell(),
                    right.to_cell()
                )))
            }
        }
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq => compare_values(&left, op, &right)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        BinaryOperator::PGLikeMatch
        | BinaryOperator::PGILikeMatch
        | BinaryOperator::PGNotLikeMatch
        | BinaryOperator::PGNotILikeMatch
        | BinaryOperator::PGRegexMatch
        | BinaryOperator::PGRegexIMatch
        | BinaryOperator::PGRegexNotMatch
        | BinaryOperator::PGRegexNotIMatch => eval_pg_pattern_operator(&left, op, &right)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        BinaryOperator::AtArrow | BinaryOperator::ArrowAt => {
            eval_containment_truth(left, op, right)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
        }
        BinaryOperator::Arrow | BinaryOperator::LongArrow => {
            eval_json_operator_value(left, op, right)
        }
        BinaryOperator::HashArrow | BinaryOperator::HashLongArrow => {
            eval_json_hash_operator_value(left, op, right)
        }
        BinaryOperator::Question | BinaryOperator::QuestionAnd | BinaryOperator::QuestionPipe => {
            eval_json_existence_value(left, op, right)
        }
        BinaryOperator::AtQuestion => eval_jsonpath_operator_value(left, right, false),
        BinaryOperator::HashMinus => eval_json_delete_path_value(left, right),
        BinaryOperator::BitwiseAnd => match (left, right) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            (SqlValue::Int(left), SqlValue::Int(right)) => Ok(SqlValue::Int(left & right)),
            _ => Err(SqlError::Unsupported(format!(
                "unsupported value operator {op}"
            ))),
        },
        other => Err(SqlError::Unsupported(format!(
            "unsupported value operator {other}"
        ))),
    }
}

pub(crate) fn vector_distance_operator(op: &BinaryOperator) -> Option<&'static str> {
    // Formatting every ordinary +/* operator just to rule out a vector
    // distance allocated a String on the scalar arithmetic hot path.
    let spelling = match op {
        BinaryOperator::Spaceship => return Some("<=>"),
        BinaryOperator::LtDashGt => return Some("<->"),
        BinaryOperator::LtCaret => return Some("<+>"),
        BinaryOperator::Custom(spelling) => spelling.as_str(),
        _ => return None,
    };
    match spelling {
        "<=>" => Some("<=>"),
        "<->" => Some("<->"),
        "<#>" => Some("<#>"),
        "<+>" | "<^" => Some("<+>"),
        _ => None,
    }
}

pub(crate) fn eval_vector_distance_value(
    left: SqlValue,
    metric: &str,
    right: SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let left = sql_value_to_vector(&left)?;
    let right = sql_value_to_vector(&right)?;
    if left.len() != right.len() {
        return Err(SqlError::data_exception(
            "22000",
            format!(
                "different vector dimensions {} and {}",
                left.len(),
                right.len()
            ),
            Some("vector".to_string()),
        ));
    }
    let distance = match metric {
        "<=>" => {
            let dot = dot_product(&left, &right)?;
            let left_norm = dot_product(&left, &left)?;
            let right_norm = dot_product(&right, &right)?;
            if left_norm == 0.0 || right_norm == 0.0 {
                f64::NAN
            } else {
                1.0 - f64::from(dot) / (f64::from(left_norm).sqrt() * f64::from(right_norm).sqrt())
            }
        }
        "<->" => f64::from(
            left.iter()
                .zip(&right)
                .map(|(left, right)| (left - right).powi(2))
                .sum::<f32>(),
        )
        .sqrt(),
        "<#>" => -f64::from(dot_product(&left, &right)?),
        "<+>" => f64::from(
            left.iter()
                .zip(&right)
                .map(|(left, right)| (left - right).abs())
                .sum::<f32>(),
        ),
        _ => unreachable!("vector operator dispatch is validated"),
    };
    Ok(SqlValue::Float(distance))
}

pub(crate) fn eval_bit_binary_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let left = bit_argument(&left)?.unwrap();
    if matches!(
        op,
        BinaryOperator::PGBitwiseShiftLeft | BinaryOperator::PGBitwiseShiftRight
    ) {
        let shift = sql_value_i64(&right)
            .ok_or_else(|| SqlError::undefined_function("bit shift requires an integer"))?;
        let left_shift = if shift < 0 {
            matches!(op, BinaryOperator::PGBitwiseShiftRight)
        } else {
            matches!(op, BinaryOperator::PGBitwiseShiftLeft)
        };
        let count = usize::try_from(shift.unsigned_abs()).unwrap_or(usize::MAX);
        let text = left.to_bit_text();
        let output = if count >= text.len() {
            "0".repeat(text.len())
        } else if left_shift {
            format!("{}{}", &text[count..], "0".repeat(count))
        } else {
            format!("{}{}", "0".repeat(count), &text[..text.len() - count])
        };
        return Ok(SqlValue::String(output));
    }
    let right = bit_argument(&right)?.unwrap();
    if matches!(op, BinaryOperator::StringConcat) {
        return Ok(SqlValue::String(format!(
            "{}{}",
            left.to_bit_text(),
            right.to_bit_text()
        )));
    }
    if left.bit_len() != right.bit_len() {
        let operation = match op {
            BinaryOperator::BitwiseAnd => "AND",
            BinaryOperator::BitwiseOr => "OR",
            BinaryOperator::PGBitwiseXor => "XOR",
            _ => "operate on",
        };
        return Err(SqlError::data_exception(
            "22026",
            format!("cannot {operation} bit strings of different sizes"),
            None,
        ));
    }
    let output = left
        .to_bit_text()
        .bytes()
        .zip(right.to_bit_text().bytes())
        .map(|(left, right)| {
            let bit = match op {
                BinaryOperator::BitwiseAnd => left == b'1' && right == b'1',
                BinaryOperator::BitwiseOr => left == b'1' || right == b'1',
                BinaryOperator::PGBitwiseXor => left != right,
                _ => unreachable!("bit operator dispatch is validated"),
            };
            if bit {
                '1'
            } else {
                '0'
            }
        })
        .collect();
    Ok(SqlValue::String(output))
}

/// The three inferred types a binary expression's evaluation depends on:
/// both operand types and the result type. Pure in (exprs, schema), so
/// callers that evaluate the same AST node repeatedly (PL/pgSQL loops, per
/// row predicates) may compute them once and pass them to
/// `eval_binary_expr_value_typed`.
#[derive(Clone, Debug, Default)]
pub(crate) struct BinaryExprTypes {
    pub(crate) left: Option<String>,
    pub(crate) right: Option<String>,
    pub(crate) result: Option<String>,
}

pub(crate) fn binary_expr_types(
    left_expr: &Expr,
    op: &BinaryOperator,
    right_expr: &Expr,
    schema: Option<&TableSchema>,
) -> BinaryExprTypes {
    BinaryExprTypes {
        left: projected_arithmetic_operand_pg_type(left_expr, schema),
        right: projected_arithmetic_operand_pg_type(right_expr, schema),
        result: projected_binary_expr_pg_type(left_expr, op, right_expr, schema),
    }
}

pub(crate) fn eval_binary_expr_value(
    left_expr: &Expr,
    op: &BinaryOperator,
    right_expr: &Expr,
    left: SqlValue,
    right: SqlValue,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    let types = binary_expr_types(left_expr, op, right_expr, schema);
    eval_binary_expr_value_typed(left_expr, op, right_expr, left, right, schema, &types)
}

pub(crate) fn eval_binary_expr_value_typed(
    left_expr: &Expr,
    op: &BinaryOperator,
    right_expr: &Expr,
    left: SqlValue,
    right: SqlValue,
    schema: Option<&TableSchema>,
    types: &BinaryExprTypes,
) -> Result<SqlValue> {
    let left_type = &types.left;
    let right_type = &types.right;
    let pg_type = &types.result;
    let left = enforce_integer_value_type(left, left_type.as_deref())?;
    let right = enforce_integer_value_type(right, right_type.as_deref())?;
    if let Some(value) = eval_geometric_binary_value(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    reject_unsupported_geometric_binary(op, left_type.as_deref(), right_type.as_deref())?;
    if let Some(value) = eval_network_binary_value(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_pg_lsn_binary_value(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_range_binary_value(
        left.clone(),
        op,
        right.clone(),
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_array_binary_value(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
    ) {
        return compare_expr_values(left_expr, op, right_expr, &left, &right, schema)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null));
    }
    if left_type.as_deref() == Some("oid") || right_type.as_deref() == Some("oid") {
        return Err(SqlError::undefined_function(format!(
            "operator does not exist: {} {op} {}",
            left_type.as_deref().unwrap_or("unknown"),
            right_type.as_deref().unwrap_or("unknown")
        )));
    }
    if let Some(value) = eval_typed_interval_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_typed_date_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_typed_time_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_typed_timetz_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_typed_timestamptz_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    if let Some(value) = eval_typed_timestamp_arithmetic(
        &left,
        op,
        &right,
        left_type.as_deref(),
        right_type.as_deref(),
    )? {
        return Ok(value);
    }
    let left_is_bit = matches!(left_type.as_deref(), Some("bit" | "varbit"));
    let right_is_bit = matches!(right_type.as_deref(), Some("bit" | "varbit"));
    let bit_operator = match op {
        BinaryOperator::PGBitwiseShiftLeft | BinaryOperator::PGBitwiseShiftRight => {
            left_is_bit
                || (left_type.is_none()
                    && matches!(&left, SqlValue::String(value) if PgBitString::from_bit_text(value).is_ok()))
        }
        _ => {
            (left_is_bit && (right_is_bit || right_type.is_none()))
                || (right_is_bit && (left_is_bit || left_type.is_none()))
        }
    };
    let left_is_vector =
        left_type.as_deref() == Some("tsvector") || is_explicit_tsvector_expr(left_expr);
    let right_is_vector =
        right_type.as_deref() == Some("tsvector") || is_explicit_tsvector_expr(right_expr);
    let left_is_query =
        left_type.as_deref() == Some("tsquery") || matches!(left, SqlValue::TsQuery(_));
    let right_is_query =
        right_type.as_deref() == Some("tsquery") || matches!(right, SqlValue::TsQuery(_));
    let value = if matches!(op, BinaryOperator::AtAt)
        && left_type.as_deref() == Some("jsonb")
        && right_type.as_deref() == Some("jsonpath")
    {
        eval_jsonpath_operator_value(left, right, true)?
    } else if matches!(op, BinaryOperator::AtAt)
        && ((left_is_vector && right_is_query) || (left_is_query && right_is_vector))
    {
        eval_text_search_match_value(
            left,
            right,
            if left_is_vector {
                Some("tsvector")
            } else {
                Some("tsquery")
            },
            if right_is_vector {
                Some("tsvector")
            } else {
                Some("tsquery")
            },
        )?
    } else if matches!(op, BinaryOperator::AtAt) {
        return Err(SqlError::undefined_function(format!(
            "operator does not exist: {} @@ {}",
            left_type.as_deref().unwrap_or("unknown"),
            right_type.as_deref().unwrap_or("unknown")
        )));
    } else if matches!(op, BinaryOperator::PGOverlap | BinaryOperator::StringConcat)
        && left_type.as_deref() == Some("tsquery")
        && right_type.as_deref() == Some("tsquery")
    {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            SqlValue::Null
        } else {
            let left = tsquery_from_sql_value(&left)?;
            let right = tsquery_from_sql_value(&right)?;
            SqlValue::TsQuery(if matches!(op, BinaryOperator::PGOverlap) {
                left.and(right)
            } else {
                left.or(right)
            })
        }
    } else if bit_operator
        && matches!(
            op,
            BinaryOperator::BitwiseAnd
                | BinaryOperator::BitwiseOr
                | BinaryOperator::PGBitwiseXor
                | BinaryOperator::PGBitwiseShiftLeft
                | BinaryOperator::PGBitwiseShiftRight
                | BinaryOperator::StringConcat
        )
    {
        eval_bit_binary_value(left, op, right)?
    } else if matches!(op, BinaryOperator::StringConcat)
        && matches!(left_type.as_deref(), Some("tsvector"))
        && matches!(right_type.as_deref(), Some("tsvector"))
    {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            SqlValue::Null
        } else {
            let left = PgTsVector::from_postgres_text(&left.to_cell())?;
            let right = PgTsVector::from_postgres_text(&right.to_cell())?;
            SqlValue::String(left.concat(&right).to_postgres_text())
        }
    } else if matches!(op, BinaryOperator::StringConcat)
        && matches!(left_type.as_deref(), Some("bytea"))
        && matches!(right_type.as_deref(), Some("bytea"))
    {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            SqlValue::Null
        } else {
            let mut bytes = bytea_argument(&left)?.unwrap();
            bytes.extend(bytea_argument(&right)?.unwrap());
            SqlValue::String(format_bytea_hex(&bytes))
        }
    } else if matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    ) && (matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null))
        && (matches!(left_type.as_deref(), Some("money"))
            || matches!(right_type.as_deref(), Some("money"))
            || matches!(pg_type.as_deref(), Some("numeric" | "float4" | "float8")))
    {
        // SQL three-valued arithmetic: a NULL operand yields NULL. The typed
        // money/numeric/float branches below parse the operand's text, and a
        // NULL rendered as "" was failing with "invalid input syntax for type
        // numeric" instead — which killed HammerDB's NEWORD on the 1% path
        // where an invalid item leaves a NULL price in the amount array.
        SqlValue::Null
    } else if matches!(left_type.as_deref(), Some("money"))
        || matches!(right_type.as_deref(), Some("money"))
    {
        eval_money_arithmetic(left, op, right, left_type.as_deref(), right_type.as_deref())?
    } else if matches!(pg_type.as_deref(), Some("numeric"))
        && matches!(
            op,
            BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
        )
    {
        eval_pg_numeric_arithmetic(left, op, right)?
    } else if matches!(pg_type.as_deref(), Some("float4" | "float8"))
        && matches!(
            op,
            BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
        )
    {
        eval_float_arithmetic_value(left, op, right, pg_type.as_deref().unwrap())?
    } else {
        eval_binary_value(left, op, right)
            .map_err(|error| remap_integer_overflow(error, pg_type.as_deref()))?
    };
    enforce_integer_value_type(value, pg_type.as_deref())
}

pub(crate) fn is_array_pg_type(pg_type: Option<&str>) -> bool {
    pg_type.is_some_and(|pg_type| pg_type.ends_with("[]"))
}

pub(crate) fn flatten_array_json<'a>(value: &'a JsonValue, output: &mut Vec<&'a JsonValue>) {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                flatten_array_json(value, output);
            }
        }
        value => output.push(value),
    }
}

pub(crate) fn array_index_terms(value: &SqlValue) -> Result<Vec<String>> {
    let (array, _) = array_json_parts(value, "array GIN index")?.ok_or_else(|| {
        SqlError::InvalidSql("array GIN index expression must return an array".to_string())
    })?;
    let mut values = Vec::new();
    flatten_array_json(array, &mut values);
    Ok(values
        .into_iter()
        .map(|value| format!("a{}", canonical_json_value_key(value)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(crate) fn collect_array_slices(value: &JsonValue, depth: usize, output: &mut Vec<JsonValue>) {
    if depth == 0 {
        output.push(value.clone());
        return;
    }
    if let JsonValue::Array(values) = value {
        for value in values {
            collect_array_slices(value, depth - 1, output);
        }
    }
}

pub(crate) fn foreach_array_values(value: &SqlValue, slice: usize) -> Result<Vec<SqlValue>> {
    let (array, lower_bounds) = array_json_parts(value, "FOREACH")?.ok_or_else(|| {
        SqlError::data_exception("22004", "FOREACH expression must not be null", None)
    })?;
    let dimensions = array_dimensions(array);
    if slice > dimensions.len() {
        return Err(SqlError::data_exception(
            "2202E",
            format!(
                "slice dimension ({slice}) is out of the valid range 0..{}",
                dimensions.len()
            ),
            None,
        ));
    }
    if slice == 0 {
        let mut values = Vec::new();
        flatten_array_json(array, &mut values);
        return Ok(values.into_iter().map(json_to_sql_value).collect());
    }
    let mut slices = Vec::new();
    collect_array_slices(array, dimensions.len() - slice, &mut slices);
    let slice_bounds = lower_bounds[dimensions.len() - slice..].to_vec();
    Ok(slices
        .into_iter()
        .map(|value| array_json_value_with_lower_bounds(value, slice_bounds.clone()))
        .collect())
}

pub(crate) fn array_elements_not_distinct(left: &JsonValue, right: &JsonValue) -> bool {
    values_not_distinct(&json_to_sql_value(left), &json_to_sql_value(right))
}

pub(crate) fn eval_array_binary_value(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    if !matches!(
        op,
        BinaryOperator::AtArrow
            | BinaryOperator::ArrowAt
            | BinaryOperator::PGOverlap
            | BinaryOperator::StringConcat
    ) {
        return Ok(None);
    }
    let mut left_array = is_array_pg_type(left_type);
    let mut right_array = is_array_pg_type(right_type);
    if left_array && !right_array && right_type.is_none() {
        right_array =
            matches!(right, SqlValue::Null) || array_json_parts(right, "array operator")?.is_some();
    }
    if right_array && !left_array && left_type.is_none() {
        left_array =
            matches!(left, SqlValue::Null) || array_json_parts(left, "array operator")?.is_some();
    }
    if matches!(
        op,
        BinaryOperator::AtArrow | BinaryOperator::ArrowAt | BinaryOperator::PGOverlap
    ) && left_array
        && right_array
    {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            return Ok(Some(SqlValue::Null));
        }
        let (left, _) = array_json_parts(left, "array operator")?.expect("typed array");
        let (right, _) = array_json_parts(right, "array operator")?.expect("typed array");
        let mut left_values = Vec::new();
        let mut right_values = Vec::new();
        flatten_array_json(left, &mut left_values);
        flatten_array_json(right, &mut right_values);
        let contains = |container: &[&JsonValue], values: &[&JsonValue]| {
            values.iter().all(|value| {
                container
                    .iter()
                    .any(|candidate| array_elements_not_distinct(candidate, value))
            })
        };
        let result = match op {
            BinaryOperator::AtArrow => contains(&left_values, &right_values),
            BinaryOperator::ArrowAt => contains(&right_values, &left_values),
            BinaryOperator::PGOverlap => left_values.iter().any(|left| {
                right_values
                    .iter()
                    .any(|right| array_elements_not_distinct(left, right))
            }),
            _ => unreachable!(),
        };
        return Ok(Some(SqlValue::Bool(result)));
    }
    if !matches!(op, BinaryOperator::StringConcat) || !left_array && !right_array {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) && matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    if left_array && right_array {
        return concat_array_values(left, right).map(Some);
    }
    if left_array {
        require_one_dimensional_array(
            left,
            "22000",
            "argument must be empty or one-dimensional array",
        )?;
        let Some((mut values, bounds)) = nullable_array_parts(left, "array concatenation")? else {
            return Ok(Some(array_value(vec![right.clone()])));
        };
        values.push(right.clone());
        return Ok(Some(array_value_with_lower_bounds(values, bounds)));
    }
    require_one_dimensional_array(
        right,
        "22000",
        "argument must be empty or one-dimensional array",
    )?;
    let Some((mut values, bounds)) = nullable_array_parts(right, "array concatenation")? else {
        return Ok(Some(array_value(vec![left.clone()])));
    };
    values.insert(0, left.clone());
    Ok(Some(array_value_with_lower_bounds(values, bounds)))
}

pub(crate) fn is_explicit_tsvector_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => is_explicit_tsvector_expr(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } => is_explicit_tsvector_expr(left) && is_explicit_tsvector_expr(right),
        Expr::Function(function) => {
            function_name_is(function, "to_tsvector")
                || function_name_is(function, "setweight")
                || function_name_is(function, "strip")
                || function_name_is(function, "array_to_tsvector")
        }
        _ => false,
    }
}

pub(crate) fn is_explicit_tsquery_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => is_explicit_tsquery_expr(inner),
        Expr::Function(function) => {
            function_name_is(function, "to_tsquery")
                || function_name_is(function, "plainto_tsquery")
                || function_name_is(function, "phraseto_tsquery")
                || function_name_is(function, "websearch_to_tsquery")
                || function_name_is(function, "tsquery_phrase")
                || function_name_is(function, "ts_rewrite")
        }
        _ => false,
    }
}

pub(crate) fn eval_text_search_match_value(
    left: SqlValue,
    right: SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let (vector, query) = match (left_type, right_type) {
        (Some("tsvector"), Some("tsquery")) => (left.to_cell(), tsquery_from_sql_value(&right)?),
        (Some("tsquery"), Some("tsvector")) => (right.to_cell(), tsquery_from_sql_value(&left)?),
        _ => {
            return Err(SqlError::undefined_function(format!(
                "operator does not exist: {} @@ {}",
                left_type.unwrap_or("unknown"),
                right_type.unwrap_or("unknown")
            )));
        }
    };
    Ok(SqlValue::Bool(
        query.matches(&PgTsVector::from_postgres_text(&vector)?),
    ))
}

pub(crate) fn eval_typed_date_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_date = left_type == Some("date");
    let right_date = right_type == Some("date");
    let left_integer = left_type.is_some_and(is_date_integer_type);
    let right_integer = right_type.is_some_and(is_date_integer_type);
    let supported = (left_date && right_date && matches!(op, BinaryOperator::Minus))
        || (left_date
            && right_integer
            && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || (left_integer && right_date && matches!(op, BinaryOperator::Plus));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }

    if left_date && right_date {
        let left = PgDate::from_postgres_text(&left.to_cell())
            .map_err(|error| postgres_date_input_error(&left.to_cell(), error))?;
        let right = PgDate::from_postgres_text(&right.to_cell())
            .map_err(|error| postgres_date_input_error(&right.to_cell(), error))?;
        let (Some(left), Some(right)) = (left.epoch_days(), right.epoch_days()) else {
            return Err(SqlError::data_exception(
                "22008",
                "cannot subtract infinite dates",
                Some("date".to_string()),
            ));
        };
        return Ok(Some(SqlValue::Int(i64::from(left - right))));
    }

    let (date_value, day_value, subtract) = if left_date {
        (left, right, matches!(op, BinaryOperator::Minus))
    } else {
        (right, left, false)
    };
    let date = PgDate::from_postgres_text(&date_value.to_cell())
        .map_err(|error| postgres_date_input_error(&date_value.to_cell(), error))?;
    let days = sql_value_i64(day_value)
        .and_then(|days| i32::try_from(days).ok())
        .ok_or_else(|| SqlError::data_exception("22008", "date out of range", None))?;
    let days = if subtract {
        days.checked_neg()
            .ok_or_else(|| SqlError::data_exception("22008", "date out of range", None))?
    } else {
        days
    };
    let date = date
        .checked_add_days(days)
        .map_err(|_| SqlError::data_exception("22008", "date out of range", None))?;
    Ok(Some(SqlValue::String(date.to_iso_text())))
}

pub(crate) fn eval_typed_time_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_time = left_type == Some("time");
    let right_time = right_type == Some("time");
    let left_interval = left_type == Some("interval");
    let right_interval = right_type == Some("interval");
    let supported = (left_time && right_time && matches!(op, BinaryOperator::Minus))
        || (left_time
            && right_interval
            && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || (left_interval && right_time && matches!(op, BinaryOperator::Plus));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }

    if left_time && right_time {
        let left = PgTime::from_postgres_text(&left.to_cell())
            .map_err(|error| postgres_time_input_error(&left.to_cell(), error))?;
        let right = PgTime::from_postgres_text(&right.to_cell())
            .map_err(|error| postgres_time_input_error(&right.to_cell(), error))?;
        return Ok(Some(SqlValue::String(
            PgInterval {
                months: 0,
                days: 0,
                micros: left.micros_since_midnight() - right.micros_since_midnight(),
            }
            .to_postgres_text(),
        )));
    }

    let (time, interval, subtract) = if left_time {
        (left, right, matches!(op, BinaryOperator::Minus))
    } else {
        (right, left, false)
    };
    let time = PgTime::from_postgres_text(&time.to_cell())
        .map_err(|error| postgres_time_input_error(&time.to_cell(), error))?;
    let interval_text = interval.to_cell();
    let interval = PgInterval::from_postgres_text(&interval_text).map_err(|_| {
        SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{interval_text}\""
        ))
    })?;
    let delta = if subtract {
        interval
            .micros
            .checked_neg()
            .ok_or_else(|| SqlError::data_exception("22008", "time out of range", None))?
    } else {
        interval.micros
    };
    let micros = time
        .micros_since_midnight()
        .checked_add(delta)
        .ok_or_else(|| SqlError::data_exception("22008", "time out of range", None))?
        .rem_euclid(MICROS_PER_DAY);
    Ok(Some(SqlValue::String(
        PgTime::from_micros_since_midnight(micros)
            .expect("reduced time is in range")
            .to_iso_text(),
    )))
}

pub(crate) fn eval_typed_timetz_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_timetz = left_type == Some("timetz");
    let right_timetz = right_type == Some("timetz");
    let left_interval = left_type == Some("interval");
    let right_interval = right_type == Some("interval");
    let supported = (left_timetz
        && right_interval
        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || (left_interval && right_timetz && matches!(op, BinaryOperator::Plus));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    let (time, interval, subtract) = if left_timetz {
        (left, right, matches!(op, BinaryOperator::Minus))
    } else {
        (right, left, false)
    };
    let time_text = time.to_cell();
    let time = PgTimeTz::from_postgres_text(&time_text, current_timezone_offset_seconds())
        .map_err(|error| postgres_timetz_input_error(&time_text, error))?;
    let interval_text = interval.to_cell();
    let interval = PgInterval::from_postgres_text(&interval_text).map_err(|_| {
        SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{interval_text}\""
        ))
    })?;
    let delta = if subtract {
        interval
            .micros
            .checked_neg()
            .ok_or_else(|| SqlError::data_exception("22008", "timetz out of range", None))?
    } else {
        interval.micros
    };
    let micros = time
        .time
        .micros_since_midnight()
        .checked_add(delta)
        .ok_or_else(|| SqlError::data_exception("22008", "timetz out of range", None))?
        .rem_euclid(MICROS_PER_DAY);
    Ok(Some(SqlValue::String(
        PgTimeTz::new(
            PgTime::from_micros_since_midnight(micros).expect("reduced time is in range"),
            time.utc_offset_seconds,
        )
        .expect("existing timezone offset is valid")
        .to_iso_text(),
    )))
}

pub(crate) fn eval_typed_interval_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_interval = left_type == Some("interval");
    let right_interval = right_type == Some("interval");
    let supported = (left_interval
        && right_interval
        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || ((left_interval || right_interval) && matches!(op, BinaryOperator::Multiply))
        || (left_interval && !right_interval && matches!(op, BinaryOperator::Divide));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    let result = if left_interval && right_interval {
        let left_text = left.to_cell();
        let right_text = right.to_cell();
        let left = PgInterval::from_postgres_text(&left_text)
            .map_err(|error| postgres_interval_input_error(&left_text, error))?;
        let right = PgInterval::from_postgres_text(&right_text)
            .map_err(|error| postgres_interval_input_error(&right_text, error))?;
        if matches!(op, BinaryOperator::Plus) {
            left.checked_add(right)
        } else {
            left.checked_sub(right)
        }
    } else {
        let (interval, scalar) = if left_interval {
            (left, right)
        } else {
            (right, left)
        };
        let interval_text = interval.to_cell();
        let interval = PgInterval::from_postgres_text(&interval_text)
            .map_err(|error| postgres_interval_input_error(&interval_text, error))?;
        let scalar = sql_value_f64(scalar).ok_or_else(|| {
            SqlError::undefined_function("interval arithmetic requires a double precision scalar")
        })?;
        if matches!(op, BinaryOperator::Divide) {
            if scalar == 0.0 {
                return Err(division_by_zero());
            }
            interval.checked_div(scalar)
        } else {
            interval.checked_scale(scalar)
        }
    }
    .map_err(|_| {
        SqlError::data_exception(
            "22015",
            "interval out of range",
            Some("interval".to_string()),
        )
    })?;
    Ok(Some(SqlValue::String(render_interval(result))))
}

pub(crate) fn eval_typed_timestamp_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_timestamp = left_type == Some("timestamp");
    let right_timestamp = right_type == Some("timestamp");
    let left_interval = left_type == Some("interval");
    let right_interval = right_type == Some("interval");
    let supported = (left_timestamp
        && right_interval
        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || (left_interval && right_timestamp && matches!(op, BinaryOperator::Plus))
        || (left_timestamp && right_timestamp && matches!(op, BinaryOperator::Minus));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    if left_timestamp && right_timestamp {
        let left_text = left.to_cell();
        let right_text = right.to_cell();
        let left = PgTimestamp::from_postgres_text(&left_text, false)
            .map_err(|error| postgres_timestamp_input_error(&left_text, error))?;
        let right = PgTimestamp::from_postgres_text(&right_text, false)
            .map_err(|error| postgres_timestamp_input_error(&right_text, error))?;
        let interval = left.checked_difference(right).map_err(|_| {
            SqlError::data_exception(
                "22008",
                "cannot subtract infinite timestamps",
                Some("timestamp".to_string()),
            )
        })?;
        return Ok(Some(SqlValue::String(interval.to_postgres_text())));
    }
    let (timestamp, interval, subtract) = if left_timestamp {
        (left, right, matches!(op, BinaryOperator::Minus))
    } else {
        (right, left, false)
    };
    let timestamp_text = timestamp.to_cell();
    let timestamp = PgTimestamp::from_postgres_text(&timestamp_text, false)
        .map_err(|error| postgres_timestamp_input_error(&timestamp_text, error))?;
    let interval_text = interval.to_cell();
    let interval = PgInterval::from_postgres_text(&interval_text).map_err(|_| {
        SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{interval_text}\""
        ))
    })?;
    let result = if subtract {
        timestamp.checked_sub_interval(interval)
    } else {
        timestamp.checked_add_interval(interval)
    }
    .map_err(|_| {
        SqlError::data_exception(
            "22008",
            "timestamp out of range",
            Some("timestamp".to_string()),
        )
    })?;
    Ok(Some(SqlValue::String(result.to_iso_text(false))))
}

pub(crate) fn eval_typed_timestamptz_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let left_timestamp = left_type == Some("timestamptz");
    let right_timestamp = right_type == Some("timestamptz");
    let left_interval = left_type == Some("interval");
    let right_interval = right_type == Some("interval");
    let supported = (left_timestamp
        && right_interval
        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
        || (left_interval && right_timestamp && matches!(op, BinaryOperator::Plus))
        || (left_timestamp && right_timestamp && matches!(op, BinaryOperator::Minus));
    if !supported {
        return Ok(None);
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    if left_timestamp && right_timestamp {
        let left_text = left.to_cell();
        let right_text = right.to_cell();
        let left = parse_timestamptz(&left_text)
            .map_err(|error| postgres_timestamptz_input_error(&left_text, error))?;
        let right = parse_timestamptz(&right_text)
            .map_err(|error| postgres_timestamptz_input_error(&right_text, error))?;
        let interval = left.checked_difference(right).map_err(|_| {
            SqlError::data_exception(
                "22008",
                "cannot subtract infinite timestamps",
                Some("timestamptz".to_string()),
            )
        })?;
        return Ok(Some(SqlValue::String(interval.to_postgres_text())));
    }
    let (timestamp, interval, subtract) = if left_timestamp {
        (left, right, matches!(op, BinaryOperator::Minus))
    } else {
        (right, left, false)
    };
    let timestamp_text = timestamp.to_cell();
    let mut timestamp = parse_timestamptz(&timestamp_text)
        .map_err(|error| postgres_timestamptz_input_error(&timestamp_text, error))?;
    let interval_text = interval.to_cell();
    let mut interval = PgInterval::from_postgres_text(&interval_text).map_err(|_| {
        SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{interval_text}\""
        ))
    })?;
    if subtract {
        interval = PgInterval {
            months: interval.months.checked_neg().ok_or_else(|| {
                SqlError::data_exception("22008", "timestamptz out of range", None)
            })?,
            days: interval.days.checked_neg().ok_or_else(|| {
                SqlError::data_exception("22008", "timestamptz out of range", None)
            })?,
            micros: interval.micros.checked_neg().ok_or_else(|| {
                SqlError::data_exception("22008", "timestamptz out of range", None)
            })?,
        };
    }
    let Some(mut utc_micros) = timestamp.finite_micros() else {
        return Ok(Some(SqlValue::String(render_timestamptz(timestamp))));
    };
    let timezone = current_timezone_name();
    if interval.months != 0 || interval.days != 0 {
        let offset = timezone_offset_at_timestamp(&timezone, timestamp).ok_or_else(|| {
            SqlError::invalid_parameter_value(format!("time zone \"{timezone}\" not recognized"))
        })?;
        let local = PgTimestamp::Finite(
            utc_micros
                .checked_add(i64::from(offset) * 1_000_000)
                .ok_or_else(|| {
                    SqlError::data_exception("22008", "timestamptz out of range", None)
                })?,
        );
        let local = local
            .checked_add_interval(PgInterval {
                months: interval.months,
                days: interval.days,
                micros: 0,
            })
            .map_err(|_| SqlError::data_exception("22008", "timestamptz out of range", None))?;
        timestamp = parse_timestamptz_in_zone(&local.to_iso_text(false), &timezone)
            .map_err(|error| postgres_timestamptz_input_error(&timestamp_text, error))?;
        utc_micros = timestamp
            .finite_micros()
            .ok_or_else(|| SqlError::data_exception("22008", "timestamptz out of range", None))?;
    }
    let result = utc_micros
        .checked_add(interval.micros)
        .map(PgTimestamp::Finite)
        .ok_or_else(|| SqlError::data_exception("22008", "timestamptz out of range", None))?;
    Ok(Some(SqlValue::String(render_timestamptz(result))))
}

pub(crate) fn is_date_integer_type(pg_type: &str) -> bool {
    matches!(pg_type, "int2" | "smallint" | "int4" | "int" | "integer")
}

pub(crate) fn is_interval_scalar_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "int2" | "int4" | "int8" | "float4" | "float8" | "numeric"
    )
}

pub(crate) fn compare_expr_values(
    left_expr: &Expr,
    op: &BinaryOperator,
    right_expr: &Expr,
    left: &SqlValue,
    right: &SqlValue,
    schema: Option<&TableSchema>,
) -> Result<Option<bool>> {
    let left_expr_type = projected_expr_pg_type(left_expr, schema);
    let right_expr_type = projected_expr_pg_type(right_expr, schema);
    reject_undefined_comparison(left_expr_type.as_deref(), op, right_expr_type.as_deref())?;
    if matches!(
        op,
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq
    ) && left_expr_type == right_expr_type
        && matches!(left_expr_type.as_deref(), Some("xid" | "cid"))
    {
        let pg_type = left_expr_type.as_deref().expect("transaction type checked");
        return Err(SqlError::undefined_function(format!(
            "operator does not exist: {pg_type} {op} {pg_type}"
        )));
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(None);
    }
    if left_expr_type.as_deref() == Some("pg_lsn") && right_expr_type.as_deref() == Some("pg_lsn") {
        return Ok(Some(comparison_from_ordering(
            op,
            pg_typed_compare("pg_lsn", left, right)?,
        )));
    }
    if let Some(SqlValue::Bool(value)) = eval_geometric_binary_value(
        left,
        op,
        right,
        left_expr_type.as_deref(),
        right_expr_type.as_deref(),
    )? {
        return Ok(Some(value));
    }
    reject_unsupported_geometric_binary(op, left_expr_type.as_deref(), right_expr_type.as_deref())?;
    if let Some(collation) = comparison_collation(left_expr, right_expr, schema)? {
        if let (SqlValue::String(left), SqlValue::String(right)) = (left, right) {
            if let Some(ordering) = locale_text_ordering(&collation, left, right) {
                return Ok(Some(comparison_from_ordering(op, ordering)));
            }
        }
    }
    let left_type = projected_arithmetic_operand_pg_type(left_expr, schema);
    let right_type = projected_arithmetic_operand_pg_type(right_expr, schema);
    if left_type.as_deref() == Some("oid") || right_type.as_deref() == Some("oid") {
        let left = oid_comparison_value(left, left_type.as_deref(), right_type.as_deref())?;
        let right = oid_comparison_value(right, right_type.as_deref(), left_type.as_deref())?;
        return compare_values(&left, op, &right);
    }
    if matches!(left_type.as_deref(), Some("money"))
        || matches!(right_type.as_deref(), Some("money"))
    {
        if !matches!(left_type.as_deref(), Some("money"))
            || !matches!(right_type.as_deref(), Some("money"))
        {
            return Err(money_operator_error(
                op,
                left_type.as_deref(),
                right_type.as_deref(),
            ));
        }
        let left = crate::pg_money_cents_from_text(&left.to_cell())
            .map_err(|_| SqlError::money_out_of_range())?;
        let right = crate::pg_money_cents_from_text(&right.to_cell())
            .map_err(|_| SqlError::money_out_of_range())?;
        let ordering = left.cmp(&right);
        return Ok(Some(match op {
            BinaryOperator::Eq => ordering == Ordering::Equal,
            BinaryOperator::NotEq => ordering != Ordering::Equal,
            BinaryOperator::Gt => ordering == Ordering::Greater,
            BinaryOperator::GtEq => ordering != Ordering::Less,
            BinaryOperator::Lt => ordering == Ordering::Less,
            BinaryOperator::LtEq => ordering != Ordering::Greater,
            _ => unreachable!(),
        }));
    }
    let internal_char_comparison = (matches!(left_expr_type.as_deref(), Some("char"))
        && matches!(right_expr_type.as_deref(), Some("char") | None))
        || (matches!(right_expr_type.as_deref(), Some("char"))
            && matches!(left_expr_type.as_deref(), None));
    if internal_char_comparison {
        let ordering = pg_typed_compare("char", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let bytea_comparison = (matches!(left_expr_type.as_deref(), Some("bytea"))
        && matches!(right_expr_type.as_deref(), Some("bytea") | None))
        || (matches!(right_expr_type.as_deref(), Some("bytea"))
            && matches!(left_expr_type.as_deref(), Some("bytea") | None));
    if bytea_comparison {
        let left = bytea_argument(left)?.unwrap();
        let right = bytea_argument(right)?.unwrap();
        return Ok(Some(comparison_from_ordering(op, left.cmp(&right))));
    }
    let bpchar_comparison = (matches!(left_expr_type.as_deref(), Some("bpchar"))
        && matches!(right_expr_type.as_deref(), Some("bpchar") | None))
        || (matches!(right_expr_type.as_deref(), Some("bpchar"))
            && matches!(left_expr_type.as_deref(), None));
    if bpchar_comparison {
        let ordering = pg_typed_compare("bpchar", left, right)?;
        return Ok(Some(match op {
            BinaryOperator::Eq => ordering == Ordering::Equal,
            BinaryOperator::NotEq => ordering != Ordering::Equal,
            BinaryOperator::Gt => ordering == Ordering::Greater,
            BinaryOperator::GtEq => ordering != Ordering::Less,
            BinaryOperator::Lt => ordering == Ordering::Less,
            BinaryOperator::LtEq => ordering != Ordering::Greater,
            _ => unreachable!(),
        }));
    }
    let date_comparison = (matches!(left_expr_type.as_deref(), Some("date"))
        && matches!(right_expr_type.as_deref(), Some("date") | None))
        || (matches!(right_expr_type.as_deref(), Some("date"))
            && matches!(left_expr_type.as_deref(), Some("date") | None));
    if date_comparison {
        let ordering = pg_typed_compare("date", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let timetz_comparison = (matches!(left_expr_type.as_deref(), Some("timetz"))
        && matches!(right_expr_type.as_deref(), Some("timetz") | None))
        || (matches!(right_expr_type.as_deref(), Some("timetz"))
            && matches!(left_expr_type.as_deref(), Some("timetz") | None));
    if timetz_comparison {
        let ordering = pg_typed_compare("timetz", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let timestamp_comparison = (matches!(left_expr_type.as_deref(), Some("timestamp"))
        && matches!(right_expr_type.as_deref(), Some("timestamp") | None))
        || (matches!(right_expr_type.as_deref(), Some("timestamp"))
            && matches!(left_expr_type.as_deref(), Some("timestamp") | None));
    if timestamp_comparison {
        let ordering = pg_typed_compare("timestamp", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let timestamptz_comparison = (matches!(left_expr_type.as_deref(), Some("timestamptz"))
        && matches!(right_expr_type.as_deref(), Some("timestamptz") | None))
        || (matches!(right_expr_type.as_deref(), Some("timestamptz"))
            && matches!(left_expr_type.as_deref(), Some("timestamptz") | None));
    if timestamptz_comparison {
        let ordering = pg_typed_compare("timestamptz", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let interval_comparison = (matches!(left_expr_type.as_deref(), Some("interval"))
        && matches!(right_expr_type.as_deref(), Some("interval") | None))
        || (matches!(right_expr_type.as_deref(), Some("interval"))
            && matches!(left_expr_type.as_deref(), Some("interval") | None));
    if interval_comparison {
        let ordering = pg_typed_compare("interval", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let range_comparison = left_expr_type
        .as_deref()
        .filter(|pg_type| is_builtin_range_type(pg_type))
        .filter(|pg_type| Some(*pg_type) == right_expr_type.as_deref());
    if let Some(range_type) = range_comparison {
        let ordering = pg_typed_compare(range_type, left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let multirange_comparison = left_expr_type
        .as_deref()
        .filter(|pg_type| is_builtin_multirange_type(pg_type))
        .filter(|pg_type| Some(*pg_type) == right_expr_type.as_deref());
    if let Some(multirange_type) = multirange_comparison {
        let ordering = pg_typed_compare(multirange_type, left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let tsvector_comparison = (matches!(left_expr_type.as_deref(), Some("tsvector"))
        && matches!(right_expr_type.as_deref(), Some("tsvector") | None))
        || (matches!(right_expr_type.as_deref(), Some("tsvector"))
            && matches!(left_expr_type.as_deref(), Some("tsvector") | None));
    if tsvector_comparison {
        let ordering = pg_typed_compare("tsvector", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let tsquery_comparison = (matches!(left_expr_type.as_deref(), Some("tsquery"))
        && matches!(right_expr_type.as_deref(), Some("tsquery") | None))
        || (matches!(right_expr_type.as_deref(), Some("tsquery"))
            && matches!(left_expr_type.as_deref(), Some("tsquery") | None));
    if tsquery_comparison {
        let ordering = pg_typed_compare("tsquery", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let network_comparison = matches!(left_expr_type.as_deref(), Some("inet" | "cidr"))
        && matches!(right_expr_type.as_deref(), Some("inet" | "cidr"));
    if network_comparison {
        let ordering = pg_typed_compare("inet", left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let mac_comparison = left_expr_type == right_expr_type
        && matches!(left_expr_type.as_deref(), Some("macaddr" | "macaddr8"));
    if mac_comparison {
        let ordering = pg_typed_compare(left_expr_type.as_deref().unwrap(), left, right)?;
        return Ok(Some(comparison_from_ordering(op, ordering)));
    }
    let operand_type =
        numeric_combine_pg_type(left_expr_type.as_deref(), right_expr_type.as_deref());
    if matches!(operand_type.as_deref(), Some("numeric")) {
        let ordering = pg_typed_compare("numeric", left, right)?;
        return Ok(Some(match op {
            BinaryOperator::Eq => ordering == Ordering::Equal,
            BinaryOperator::NotEq => ordering != Ordering::Equal,
            BinaryOperator::Gt => ordering == Ordering::Greater,
            BinaryOperator::GtEq => ordering != Ordering::Less,
            BinaryOperator::Lt => ordering == Ordering::Less,
            BinaryOperator::LtEq => ordering != Ordering::Greater,
            _ => unreachable!(),
        }));
    }
    compare_values(left, op, right)
}
