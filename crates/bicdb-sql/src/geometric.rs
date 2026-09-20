use crate::*;

const GEOMETRIC_EPSILON: f64 = 1.0e-6;

pub(crate) fn is_geometric_type(pg_type: Option<&str>) -> bool {
    matches!(
        pg_type,
        Some("point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle")
    )
}

pub(crate) fn geometric_index_default_opclass(
    access_method: &str,
    pg_type: &str,
) -> Option<&'static str> {
    match (access_method, pg_type) {
        ("gist", "point") => Some("point_ops"),
        ("gist", "box") => Some("box_ops"),
        ("gist", "polygon") => Some("poly_ops"),
        ("gist", "circle") => Some("circle_ops"),
        ("spgist", "point") => Some("quad_point_ops"),
        ("spgist", "box") => Some("box_ops"),
        ("spgist", "polygon") => Some("poly_ops"),
        ("brin", "box") => Some("box_inclusion_ops"),
        _ => None,
    }
}

pub(crate) fn geometric_index_opclass_supported(
    access_method: &str,
    pg_type: &str,
    opclass: &str,
) -> bool {
    let opclass = opclass.rsplit('.').next().unwrap_or(opclass);
    geometric_index_default_opclass(access_method, pg_type) == Some(opclass)
        || matches!(
            (access_method, pg_type, opclass),
            ("spgist", "point", "kd_point_ops")
        )
}

pub(crate) fn geometric_index_operator_supported(
    access_method: &str,
    indexed_type: &str,
    operator: &str,
    other_type: &str,
) -> bool {
    match (access_method, indexed_type) {
        ("gist", "point") => match operator {
            "<<" | ">>" | "~=" | "<<|" | "|>>" | "<^" | ">^" => other_type == "point",
            "<@" => matches!(other_type, "box" | "polygon" | "circle"),
            "<->" => other_type == "point",
            _ => false,
        },
        ("spgist", "point") => match operator {
            "<<" | ">>" | "~=" | "<<|" | "|>>" | "<^" | ">^" | "<->" => other_type == "point",
            "<@" => other_type == "box",
            _ => false,
        },
        ("gist" | "spgist", "box" | "polygon") => {
            if operator == "<->" {
                other_type == "point"
            } else {
                other_type == indexed_type
                    && matches!(
                        operator,
                        "<<" | "&<"
                            | "&&"
                            | "&>"
                            | ">>"
                            | "~="
                            | "@>"
                            | "<@"
                            | "&<|"
                            | "<<|"
                            | "|>>"
                            | "|&>"
                    )
            }
        }
        ("gist", "circle") => {
            if operator == "<->" {
                other_type == "point"
            } else {
                other_type == "circle"
                    && matches!(
                        operator,
                        "<<" | "&<"
                            | "&&"
                            | "&>"
                            | ">>"
                            | "~="
                            | "@>"
                            | "<@"
                            | "&<|"
                            | "<<|"
                            | "|>>"
                            | "|&>"
                    )
            }
        }
        ("brin", "box") => {
            (other_type == "box"
                && matches!(
                    operator,
                    "<<" | "&<"
                        | "&&"
                        | "&>"
                        | ">>"
                        | "~="
                        | "@>"
                        | "<@"
                        | "&<|"
                        | "<<|"
                        | "|>>"
                        | "|&>"
                ))
                || operator == "@>" && other_type == "point"
        }
        _ => false,
    }
}

pub(crate) fn geometric_index_bounds(value: &SqlValue, pg_type: &str) -> Result<Option<[f64; 4]>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let geometry = geometric_argument(value, pg_type)?;
    let bounds = geometric_bounds(&geometry).ok_or_else(|| {
        SqlError::undefined_object(format!(
            "data type {pg_type} has no geometric index envelope"
        ))
    })?;
    let values = [x(bounds.low), y(bounds.low), x(bounds.high), y(bounds.high)];
    if values.iter().any(|value| !value.is_finite()) {
        return Err(SqlError::numeric_value_out_of_range(
            "geometric index coordinates must be finite",
        ));
    }
    Ok(Some(values))
}

pub(crate) fn geometric_index_point(value: &SqlValue, pg_type: &str) -> Result<Option<(f64, f64)>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let PgGeometric::Point(value) = geometric_argument(value, pg_type)? else {
        return Ok(None);
    };
    Ok(Some((x(value), y(value))))
}

pub(crate) fn geometric_index_projection(
    value: &SqlValue,
    pg_type: &str,
) -> Result<Option<JsonValue>> {
    let Some([min_x, min_y, max_x, max_y]) = geometric_index_bounds(value, pg_type)? else {
        return Ok(None);
    };
    if pg_type == "point" {
        return Ok(Some(serde_json::json!({
            "type": "Point",
            "coordinates": [min_x, min_y]
        })));
    }
    Ok(Some(serde_json::json!({
        "type": "Polygon",
        "coordinates": [[
            [min_x, min_y],
            [max_x, min_y],
            [max_x, max_y],
            [min_x, max_y],
            [min_x, min_y]
        ]]
    })))
}

pub(crate) fn geometric_function_pg_type(
    name: &str,
    arg_types: &[Option<String>],
) -> Option<String> {
    let name = unqualified_name(name);
    let types = arg_types
        .iter()
        .map(|pg_type| pg_type.as_deref())
        .collect::<Vec<_>>();
    let signature_matches = match (name, types.as_slice()) {
        ("area", [Some("box" | "path" | "circle")])
        | ("center", [Some("box" | "circle")])
        | ("diagonal" | "height" | "width", [Some("box")])
        | ("diameter" | "radius", [Some("circle")])
        | ("isclosed" | "isopen", [Some("path")])
        | ("ishorizontal" | "isvertical", [Some("line" | "lseg")])
        | ("ishorizontal" | "isvertical", [Some("point"), Some("point")])
        | ("length", [Some("lseg" | "path")])
        | ("npoints", [Some("path" | "polygon")])
        | ("pclose" | "popen", [Some("path")])
        | ("point", [Some("circle" | "lseg" | "box" | "polygon")])
        | ("box", [Some("circle" | "point" | "polygon")])
        | ("circle", [Some("box" | "polygon")])
        | ("lseg", [Some("box")])
        | ("path", [Some("polygon")])
        | ("polygon", [Some("circle" | "box" | "path")])
        | ("bound_box", [Some("box"), Some("box")])
        | ("isparallel" | "isperp", [Some("line"), Some("line")])
        | ("isparallel" | "isperp", [Some("lseg"), Some("lseg")])
        | ("slope" | "line" | "lseg" | "box", [Some("point"), Some("point")]) => true,
        ("point", [left, right]) => {
            left.is_none_or(is_geometric_numeric_type)
                && right.is_none_or(is_geometric_numeric_type)
        }
        ("circle", [Some("point"), radius]) => radius.is_none_or(is_geometric_numeric_type),
        ("polygon", [count, Some("circle")]) => count.is_none_or(is_geometric_integer_type),
        _ => false,
    };
    if !signature_matches {
        return None;
    }
    let pg_type = match name {
        "area" | "diameter" | "height" | "length" | "radius" | "slope" | "width" => "float8",
        "isclosed" | "isopen" | "ishorizontal" | "isvertical" | "isparallel" | "isperp" => "bool",
        "npoints" => "int4",
        "center" | "point" => "point",
        "diagonal" | "lseg" => "lseg",
        "box" | "bound_box" => "box",
        "circle" => "circle",
        "line" => "line",
        "path" | "pclose" | "popen" => "path",
        "polygon" => "polygon",
        _ => return None,
    };
    Some(pg_type.to_string())
}

pub(crate) fn geometric_binary_result_pg_type(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Option<String> {
    if !is_geometric_type(left_type) || !is_geometric_type(right_type) {
        return None;
    }
    let operator = op.to_string();
    let left = left_type?;
    let right = right_type?;
    let result = match operator.as_str() {
        "+" if left == "path" && right == "path" => "path",
        "+" | "-" | "*" | "/"
            if right == "point" && matches!(left, "point" | "path" | "box" | "circle") =>
        {
            left
        }
        "<->" if geometric_distance_pair(left, right) => "float8",
        "#" if left == "box" && right == "box" => "box",
        "#" if matches!((left, right), ("line", "line") | ("lseg", "lseg")) => "point",
        "##" if geometric_closest_pair(left, right) => "point",
        "=" | "<>" | "<" | "<=" | ">" | ">="
            if geometric_comparison_pair(operator.as_str(), left, right) =>
        {
            "bool"
        }
        "~=" if left == right && matches!(left, "point" | "box" | "polygon" | "circle") => "bool",
        "@>" | "<@" if geometric_containment_pair(operator.as_str(), left, right) => "bool",
        "&&" if left == right && matches!(left, "box" | "polygon" | "circle") => "bool",
        "<<" | ">>" | "<<|" | "|>>" | "&<" | "&>" | "&<|" | "|&>"
            if left == right && matches!(left, "point" | "box" | "polygon" | "circle") =>
        {
            "bool"
        }
        "<^" | ">^" if left == right && matches!(left, "point" | "box") => "bool",
        "?#" if geometric_intersection_pair(left, right) => "bool",
        "?-" | "?|" if left == "point" && right == "point" => "bool",
        "?-|" | "?||" if left == right && matches!(left, "line" | "lseg") => "bool",
        _ => return None,
    };
    Some(result.to_string())
}

pub(crate) fn reject_unsupported_geometric_binary(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<()> {
    let (Some(left), Some(right)) = (left_type, right_type) else {
        return Ok(());
    };
    if !is_geometric_type(Some(left)) && !is_geometric_type(Some(right)) {
        return Ok(());
    }
    let operator = op.to_string();
    if matches!(
        operator.as_str(),
        "+" | "-"
            | "*"
            | "/"
            | "#"
            | "##"
            | "="
            | "<>"
            | "<"
            | "<="
            | ">"
            | ">="
            | "<->"
            | "~="
            | "@>"
            | "<@"
            | "&&"
            | "<<"
            | ">>"
            | "<<|"
            | "|>>"
            | "&<"
            | "&>"
            | "&<|"
            | "|&>"
            | "<^"
            | ">^"
            | "?#"
            | "?-"
            | "?|"
            | "?-|"
            | "?||"
    ) && geometric_binary_result_pg_type(op, Some(left), Some(right)).is_none()
    {
        return Err(SqlError::undefined_function(format!(
            "operator does not exist: {left} {operator} {right}"
        )));
    }
    Ok(())
}

pub(crate) fn geometric_unary_result_pg_type(
    op: &UnaryOperator,
    operand_type: Option<&str>,
) -> Option<String> {
    let operand = operand_type?;
    let result = match op {
        UnaryOperator::AtDashAt if matches!(operand, "lseg" | "path") => "float8",
        UnaryOperator::DoubleAt if matches!(operand, "lseg" | "box" | "polygon" | "circle") => {
            "point"
        }
        UnaryOperator::Hash if matches!(operand, "path" | "polygon") => "int4",
        UnaryOperator::QuestionDash | UnaryOperator::QuestionPipe
            if matches!(operand, "line" | "lseg") =>
        {
            "bool"
        }
        _ => return None,
    };
    Some(result.to_string())
}

pub(crate) fn is_geometric_unary_operator(op: &UnaryOperator) -> bool {
    matches!(
        op,
        UnaryOperator::AtDashAt
            | UnaryOperator::DoubleAt
            | UnaryOperator::Hash
            | UnaryOperator::QuestionDash
            | UnaryOperator::QuestionPipe
    )
}

pub(crate) fn eval_geometric_unary_value(
    op: &UnaryOperator,
    value: &SqlValue,
    operand_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    if geometric_unary_result_pg_type(op, operand_type).is_none() {
        return Ok(None);
    }
    if matches!(value, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    let geometry = geometric_argument(value, operand_type.unwrap())?;
    let result = match op {
        UnaryOperator::AtDashAt => SqlValue::Float(match geometry {
            PgGeometric::LineSegment { start, end } => point_distance(start, end),
            PgGeometric::Path { closed, points } => path_length(closed, &points),
            _ => unreachable!("geometric unary type was validated"),
        }),
        UnaryOperator::DoubleAt => point_sql_value(match geometry {
            PgGeometric::LineSegment { start, end } => midpoint(start, end),
            PgGeometric::Box { high, low } => midpoint(high, low),
            PgGeometric::Polygon { points } => polygon_center(&points),
            PgGeometric::Circle { center, .. } => center,
            _ => unreachable!("geometric unary type was validated"),
        }),
        UnaryOperator::Hash => SqlValue::Int(match geometry {
            PgGeometric::Path { points, .. } | PgGeometric::Polygon { points } => {
                i64::try_from(points.len()).unwrap_or(i64::MAX)
            }
            _ => unreachable!("geometric unary type was validated"),
        }),
        UnaryOperator::QuestionDash => SqlValue::Bool(geometry_horizontal(&geometry)),
        UnaryOperator::QuestionPipe => SqlValue::Bool(geometry_vertical(&geometry)),
        _ => unreachable!("geometric unary operator was validated"),
    };
    Ok(Some(result))
}

/// `<->` distance for the UNTYPED eval fallback, where operand pg-types are
/// unknown. Geometric values render as their canonical text (`(x,y)`,
/// `((x1,y1),(x2,y2))`, …) which never collides with the `[…]` vector
/// literal, so parse-sniffing both operands is unambiguous: if both are
/// geometric, this is PG's geometric distance; otherwise the caller falls
/// through to the vector metric. Without this, `ORDER BY spot <-> point(…)`
/// errored per row on the untyped path and the ORDER BY machinery degraded
/// every key to Null — "ordered" output was just insertion order (B10).
pub(crate) fn eval_untyped_geometric_distance(
    left: &SqlValue,
    right: &SqlValue,
) -> Result<Option<SqlValue>> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        // The vector path also maps Null operands to Null.
        return Ok(None);
    }
    const CANDIDATES: &[&str] = &["point", "lseg", "box", "circle", "polygon", "line", "path"];
    let (Ok(left), Ok(right)) = (
        infer_geometric_argument(left, CANDIDATES),
        infer_geometric_argument(right, CANDIDATES),
    ) else {
        return Ok(None);
    };
    Ok(Some(SqlValue::Float(geometric_distance(&left, &right)?)))
}

pub(crate) fn eval_geometric_binary_value(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<SqlValue>> {
    let Some(result_type) = geometric_binary_result_pg_type(op, left_type, right_type) else {
        return Ok(None);
    };
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    let left_type = left_type.unwrap();
    let right_type = right_type.unwrap();
    let left = geometric_argument(left, left_type)?;
    let right = geometric_argument(right, right_type)?;
    let operator = op.to_string();
    let value = match result_type.as_str() {
        "float8" => SqlValue::Float(geometric_distance(&left, &right)?),
        "bool" => SqlValue::Bool(geometric_truth(&left, &operator, &right)?),
        "point" if operator == "#" => geometric_intersection_point(&left, &right)?
            .map(point_sql_value)
            .unwrap_or(SqlValue::Null),
        "point" if operator == "##" => geometric_closest_point(&left, &right)?
            .map(point_sql_value)
            .unwrap_or(SqlValue::Null),
        "box" if operator == "#" => box_intersection(&left, &right)?
            .map(geometric_sql_value)
            .unwrap_or(SqlValue::Null),
        _ => geometric_sql_value(geometric_transform(left, &operator, right)?),
    };
    Ok(Some(value))
}

pub(crate) fn eval_geometric_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<Option<SqlValue>> {
    let name = unqualified_name(name);
    if geometric_function_pg_type(name, arg_types).is_none() {
        return Ok(None);
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }
    let result = match name {
        "area" => {
            require_geometric_args(name, args, 1)?;
            match typed_geometric_argument(args, arg_types, 0, &["circle", "box", "path"])? {
                PgGeometric::Circle { radius, .. } => {
                    SqlValue::Float(std::f64::consts::PI * radius.to_value().powi(2))
                }
                PgGeometric::Box { high, low } => SqlValue::Float(box_area(high, low)),
                PgGeometric::Path {
                    closed: true,
                    points,
                } => SqlValue::Float(polygon_area(&points)),
                PgGeometric::Path { closed: false, .. } => SqlValue::Null,
                _ => unreachable!("area argument candidates were validated"),
            }
        }
        "center" => {
            require_geometric_args(name, args, 1)?;
            point_sql_value(
                match typed_geometric_argument(args, arg_types, 0, &["circle", "box"])? {
                    PgGeometric::Circle { center, .. } => center,
                    PgGeometric::Box { high, low } => midpoint(high, low),
                    _ => unreachable!("center argument candidates were validated"),
                },
            )
        }
        "diagonal" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Box { high, low } = geometric_argument(&args[0], "box")? else {
                unreachable!()
            };
            geometric_sql_value(PgGeometric::LineSegment {
                start: high,
                end: low,
            })
        }
        "diameter" | "radius" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Circle { radius, .. } = geometric_argument(&args[0], "circle")? else {
                unreachable!()
            };
            SqlValue::Float(if name == "diameter" {
                radius.to_value() * 2.0
            } else {
                radius.to_value()
            })
        }
        "height" | "width" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Box { high, low } = geometric_argument(&args[0], "box")? else {
                unreachable!()
            };
            SqlValue::Float(if name == "height" {
                high.y.to_value() - low.y.to_value()
            } else {
                high.x.to_value() - low.x.to_value()
            })
        }
        "isclosed" | "isopen" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Path { closed, .. } = geometric_argument(&args[0], "path")? else {
                unreachable!()
            };
            SqlValue::Bool(if name == "isclosed" { closed } else { !closed })
        }
        "ishorizontal" | "isvertical" => {
            if args.len() == 2 {
                let first = point_argument(&args[0])?;
                let second = point_argument(&args[1])?;
                SqlValue::Bool(if name == "ishorizontal" {
                    pg_float_eq(y(first), y(second))
                } else {
                    pg_float_eq(x(first), x(second))
                })
            } else {
                require_geometric_args(name, args, 1)?;
                let geometry = typed_geometric_argument(args, arg_types, 0, &["line", "lseg"])?;
                SqlValue::Bool(if name == "ishorizontal" {
                    geometry_horizontal(&geometry)
                } else {
                    geometry_vertical(&geometry)
                })
            }
        }
        "isparallel" | "isperp" => {
            require_geometric_args(name, args, 2)?;
            let (left, right) = typed_parallel_arguments(args, arg_types)?;
            SqlValue::Bool(if name == "isparallel" {
                geometries_parallel(&left, &right)
            } else {
                geometries_perpendicular(&left, &right)
            })
        }
        "length" => {
            require_geometric_args(name, args, 1)?;
            let geometry = typed_geometric_argument(args, arg_types, 0, &["lseg", "path"])?;
            SqlValue::Float(match geometry {
                PgGeometric::LineSegment { start, end } => point_distance(start, end),
                PgGeometric::Path { closed, points } => path_length(closed, &points),
                _ => unreachable!("length argument candidates were validated"),
            })
        }
        "npoints" => {
            require_geometric_args(name, args, 1)?;
            let geometry = typed_geometric_argument(args, arg_types, 0, &["path", "polygon"])?;
            let count = match geometry {
                PgGeometric::Path { points, .. } | PgGeometric::Polygon { points } => points.len(),
                _ => unreachable!("npoints argument candidates were validated"),
            };
            SqlValue::Int(i64::try_from(count).unwrap_or(i64::MAX))
        }
        "pclose" | "popen" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Path { points, .. } = geometric_argument(&args[0], "path")? else {
                unreachable!()
            };
            geometric_sql_value(PgGeometric::Path {
                closed: name == "pclose",
                points,
            })
        }
        "slope" => {
            require_geometric_args(name, args, 2)?;
            let first = point_argument(&args[0])?;
            let second = point_argument(&args[1])?;
            SqlValue::Float(point_slope(first, second))
        }
        "point" => eval_point_constructor(args, arg_types)?,
        "box" => eval_box_constructor(args, arg_types)?,
        "bound_box" => eval_bound_box(args)?,
        "circle" => eval_circle_constructor(args, arg_types)?,
        "line" => {
            require_geometric_args(name, args, 2)?;
            geometric_sql_value(line_from_points(
                point_argument(&args[0])?,
                point_argument(&args[1])?,
            )?)
        }
        "lseg" => eval_lseg_constructor(args, arg_types)?,
        "path" => {
            require_geometric_args(name, args, 1)?;
            let PgGeometric::Polygon { points } = geometric_argument(&args[0], "polygon")? else {
                unreachable!()
            };
            geometric_sql_value(PgGeometric::Path {
                closed: true,
                points,
            })
        }
        "polygon" => eval_polygon_constructor(args, arg_types)?,
        _ => return Ok(None),
    };
    Ok(Some(result))
}

fn unqualified_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn is_geometric_integer_type(pg_type: &str) -> bool {
    matches!(pg_type, "int2" | "int4" | "int8")
}

fn is_geometric_numeric_type(pg_type: &str) -> bool {
    is_geometric_integer_type(pg_type) || matches!(pg_type, "float4" | "float8" | "numeric")
}

fn geometric_distance_pair(left: &str, right: &str) -> bool {
    matches!(
        (left, right),
        ("point", "point")
            | ("point", "lseg")
            | ("point", "path")
            | ("point", "box")
            | ("point", "polygon")
            | ("point", "line")
            | ("point", "circle")
            | ("lseg", "point")
            | ("lseg", "lseg")
            | ("lseg", "box")
            | ("lseg", "line")
            | ("path", "point")
            | ("path", "path")
            | ("box", "point")
            | ("box", "lseg")
            | ("box", "box")
            | ("polygon", "point")
            | ("polygon", "polygon")
            | ("polygon", "circle")
            | ("line", "point")
            | ("line", "lseg")
            | ("line", "line")
            | ("circle", "point")
            | ("circle", "polygon")
            | ("circle", "circle")
    )
}

fn geometric_closest_pair(left: &str, right: &str) -> bool {
    matches!(
        (left, right),
        ("point", "lseg")
            | ("point", "box")
            | ("point", "line")
            | ("lseg", "lseg")
            | ("lseg", "box")
            | ("line", "lseg")
    )
}

fn geometric_comparison_pair(operator: &str, left: &str, right: &str) -> bool {
    if left != right {
        return false;
    }
    match operator {
        "=" => matches!(left, "line" | "lseg" | "path" | "box" | "circle"),
        "<>" => matches!(left, "point" | "lseg" | "circle"),
        "<" | "<=" | ">" | ">=" => matches!(left, "lseg" | "path" | "box" | "circle"),
        _ => false,
    }
}

fn geometric_containment_pair(operator: &str, left: &str, right: &str) -> bool {
    match operator {
        "@>" => matches!(
            (left, right),
            ("path", "point")
                | ("box", "point")
                | ("box", "box")
                | ("polygon", "point")
                | ("polygon", "polygon")
                | ("circle", "point")
                | ("circle", "circle")
        ),
        "<@" => matches!(
            (left, right),
            ("point", "lseg")
                | ("point", "path")
                | ("point", "box")
                | ("point", "polygon")
                | ("point", "line")
                | ("point", "circle")
                | ("lseg", "box")
                | ("lseg", "line")
                | ("box", "box")
                | ("polygon", "polygon")
                | ("circle", "circle")
        ),
        _ => false,
    }
}

fn geometric_intersection_pair(left: &str, right: &str) -> bool {
    matches!(
        (left, right),
        ("lseg", "lseg")
            | ("lseg", "box")
            | ("lseg", "line")
            | ("path", "path")
            | ("box", "box")
            | ("line", "box")
            | ("line", "line")
    )
}

fn geometric_argument(value: &SqlValue, pg_type: &str) -> Result<PgGeometric> {
    match parse_pg_canonical_special(pg_type, &value.to_cell()) {
        Ok(Some(PgCanonicalValue::Geometric(value))) => Ok(value),
        _ => Err(SqlError::invalid_text_representation(
            pg_type,
            format!(
                "invalid input syntax for type {pg_type}: \"{}\"",
                value.to_cell()
            ),
        )),
    }
}

fn infer_geometric_argument(value: &SqlValue, candidates: &[&str]) -> Result<PgGeometric> {
    for candidate in candidates {
        if let Ok(value) = geometric_argument(value, candidate) {
            return Ok(value);
        }
    }
    Err(SqlError::undefined_function(
        "no geometric overload matches the supplied argument",
    ))
}

fn typed_geometric_argument(
    args: &[SqlValue],
    arg_types: &[Option<String>],
    index: usize,
    candidates: &[&str],
) -> Result<PgGeometric> {
    if let Some(pg_type) = arg_types.get(index).and_then(|pg_type| pg_type.as_deref()) {
        if candidates.contains(&pg_type) {
            return geometric_argument(&args[index], pg_type);
        }
    }
    infer_geometric_argument(&args[index], candidates)
}

fn typed_parallel_arguments(
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<(PgGeometric, PgGeometric)> {
    if let (Some(left), Some(right)) = (
        arg_types.first().and_then(|pg_type| pg_type.as_deref()),
        arg_types.get(1).and_then(|pg_type| pg_type.as_deref()),
    ) {
        if left == right && matches!(left, "line" | "lseg") {
            return Ok((
                geometric_argument(&args[0], left)?,
                geometric_argument(&args[1], right)?,
            ));
        }
    }
    for pg_type in ["line", "lseg"] {
        if let (Ok(left), Ok(right)) = (
            geometric_argument(&args[0], pg_type),
            geometric_argument(&args[1], pg_type),
        ) {
            return Ok((left, right));
        }
    }
    Err(SqlError::undefined_function(
        "no geometric overload matches the supplied arguments",
    ))
}

fn geometric_sql_value(value: PgGeometric) -> SqlValue {
    SqlValue::String(value.to_postgres_text())
}

fn point_sql_value(value: PgPoint) -> SqlValue {
    geometric_sql_value(PgGeometric::Point(value))
}

fn point_argument(value: &SqlValue) -> Result<PgPoint> {
    let PgGeometric::Point(point) = geometric_argument(value, "point")? else {
        unreachable!()
    };
    Ok(point)
}

fn require_geometric_args(name: &str, args: &[SqlValue], expected: usize) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(SqlError::undefined_function(format!(
            "function {name} with {} argument(s) does not exist",
            args.len()
        )))
    }
}

fn numeric_argument(value: &SqlValue, name: &str) -> Result<f64> {
    match value {
        SqlValue::Int(value) => Ok(*value as f64),
        SqlValue::Float(value) => Ok(*value),
        SqlValue::String(value) => value.parse::<f64>().map_err(|_| {
            SqlError::invalid_parameter_value(format!("{name} requires a numeric argument"))
        }),
        _ => Err(SqlError::invalid_parameter_value(format!(
            "{name} requires a numeric argument"
        ))),
    }
}

fn int_argument(value: &SqlValue, name: &str) -> Result<i64> {
    match value {
        SqlValue::Int(value) => Ok(*value),
        _ => Err(SqlError::invalid_parameter_value(format!(
            "{name} requires an integer argument"
        ))),
    }
}

fn point(x: f64, y: f64) -> PgPoint {
    PgPoint {
        x: PgFloat8::from_value(x),
        y: PgFloat8::from_value(y),
    }
}

fn x(point: PgPoint) -> f64 {
    point.x.to_value()
}

fn y(point: PgPoint) -> f64 {
    point.y.to_value()
}

fn pg_float_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= GEOMETRIC_EPSILON
}

fn pg_float_lt(left: f64, right: f64) -> bool {
    left + GEOMETRIC_EPSILON < right
}

fn pg_float_le(left: f64, right: f64) -> bool {
    left <= right + GEOMETRIC_EPSILON
}

fn pg_float_gt(left: f64, right: f64) -> bool {
    left > right + GEOMETRIC_EPSILON
}

fn pg_float_ge(left: f64, right: f64) -> bool {
    left + GEOMETRIC_EPSILON >= right
}

fn point_same(left: PgPoint, right: PgPoint) -> bool {
    if x(left).is_nan() || y(left).is_nan() || x(right).is_nan() || y(right).is_nan() {
        (x(left) == x(right) || x(left).is_nan() && x(right).is_nan())
            && (y(left) == y(right) || y(left).is_nan() && y(right).is_nan())
    } else {
        pg_float_eq(x(left), x(right)) && pg_float_eq(y(left), y(right))
    }
}

fn point_distance(left: PgPoint, right: PgPoint) -> f64 {
    (x(left) - x(right)).hypot(y(left) - y(right))
}

fn midpoint(left: PgPoint, right: PgPoint) -> PgPoint {
    point((x(left) + x(right)) / 2.0, (y(left) + y(right)) / 2.0)
}

fn point_slope(left: PgPoint, right: PgPoint) -> f64 {
    if pg_float_eq(x(left), x(right)) {
        f64::INFINITY
    } else if pg_float_eq(y(left), y(right)) {
        0.0
    } else {
        (y(left) - y(right)) / (x(left) - x(right))
    }
}

fn point_add(left: PgPoint, right: PgPoint) -> PgPoint {
    point(x(left) + x(right), y(left) + y(right))
}

fn point_sub(left: PgPoint, right: PgPoint) -> PgPoint {
    point(x(left) - x(right), y(left) - y(right))
}

fn point_mul(left: PgPoint, right: PgPoint) -> PgPoint {
    point(
        x(left) * x(right) - y(left) * y(right),
        x(left) * y(right) + y(left) * x(right),
    )
}

fn point_div(left: PgPoint, right: PgPoint) -> PgPoint {
    let divisor = x(right).powi(2) + y(right).powi(2);
    point(
        (x(left) * x(right) + y(left) * y(right)) / divisor,
        (y(left) * x(right) - x(left) * y(right)) / divisor,
    )
}

fn line_from_points(first: PgPoint, second: PgPoint) -> Result<PgGeometric> {
    if point_same(first, second) {
        return Err(SqlError::invalid_parameter_value(
            "invalid line specification: must be two distinct points",
        ));
    }
    let slope = point_slope(first, second);
    let (a, b, c) = if slope.is_infinite() {
        (-1.0, 0.0, x(first))
    } else if slope == 0.0 {
        (0.0, -1.0, y(first))
    } else {
        let c = y(first) - slope * x(first);
        (slope, -1.0, if c == 0.0 { 0.0 } else { c })
    };
    Ok(PgGeometric::Line {
        a: PgFloat8::from_value(a),
        b: PgFloat8::from_value(b),
        c: PgFloat8::from_value(c),
    })
}

fn line_coefficients(value: &PgGeometric) -> Option<(f64, f64, f64)> {
    match value {
        PgGeometric::Line { a, b, c } => Some((a.to_value(), b.to_value(), c.to_value())),
        PgGeometric::LineSegment { start, end } => match line_from_points(*start, *end).ok()? {
            PgGeometric::Line { a, b, c } => Some((a.to_value(), b.to_value(), c.to_value())),
            _ => unreachable!(),
        },
        _ => None,
    }
}

fn line_intersection(left: &PgGeometric, right: &PgGeometric) -> Option<PgPoint> {
    let (a1, b1, c1) = line_coefficients(left)?;
    let (a2, b2, c2) = line_coefficients(right)?;
    let determinant = a1 * b2 - a2 * b1;
    if pg_float_eq(determinant, 0.0) {
        return None;
    }
    let px = (b1 * c2 - b2 * c1) / determinant;
    let py = if b1 == 0.0 {
        -(a2 * px + c2) / b2
    } else {
        -(a1 * px + c1) / b1
    };
    Some(point(
        if px == 0.0 { 0.0 } else { px },
        if py == 0.0 { 0.0 } else { py },
    ))
}

fn line_same(left: &PgGeometric, right: &PgGeometric) -> bool {
    let Some((a1, b1, c1)) = line_coefficients(left) else {
        return false;
    };
    let Some((a2, b2, c2)) = line_coefficients(right) else {
        return false;
    };
    if [a1, b1, c1, a2, b2, c2].iter().any(|value| value.is_nan()) {
        return a1.to_bits() == a2.to_bits()
            && b1.to_bits() == b2.to_bits()
            && c1.to_bits() == c2.to_bits();
    }
    let ratio = if !pg_float_eq(a2, 0.0) {
        a1 / a2
    } else if !pg_float_eq(b2, 0.0) {
        b1 / b2
    } else if !pg_float_eq(c2, 0.0) {
        c1 / c2
    } else {
        1.0
    };
    pg_float_eq(a1, ratio * a2) && pg_float_eq(b1, ratio * b2) && pg_float_eq(c1, ratio * c2)
}

fn line_contains_point(line: &PgGeometric, value: PgPoint) -> bool {
    let Some((a, b, c)) = line_coefficients(line) else {
        return false;
    };
    pg_float_eq(a * x(value) + b * y(value) + c, 0.0)
}

fn closest_point_on_line(line: &PgGeometric, value: PgPoint) -> Option<PgPoint> {
    let (a, b, c) = line_coefficients(line)?;
    let denominator = a * a + b * b;
    if denominator == 0.0 {
        return None;
    }
    let distance = (a * x(value) + b * y(value) + c) / denominator;
    Some(point(x(value) - a * distance, y(value) - b * distance))
}

fn line_distance_to_point(line: &PgGeometric, value: PgPoint) -> f64 {
    closest_point_on_line(line, value)
        .map(|closest| point_distance(closest, value))
        .unwrap_or(f64::NAN)
}

fn segment_contains_point(start: PgPoint, end: PgPoint, value: PgPoint) -> bool {
    let cross =
        (x(value) - x(start)) * (y(end) - y(start)) - (y(value) - y(start)) * (x(end) - x(start));
    pg_float_eq(cross, 0.0)
        && pg_float_ge(x(value), x(start).min(x(end)))
        && pg_float_le(x(value), x(start).max(x(end)))
        && pg_float_ge(y(value), y(start).min(y(end)))
        && pg_float_le(y(value), y(start).max(y(end)))
}

fn segment_intersection(
    first_start: PgPoint,
    first_end: PgPoint,
    second_start: PgPoint,
    second_end: PgPoint,
) -> Option<PgPoint> {
    let first = line_from_points(first_start, first_end).ok()?;
    let second = line_from_points(second_start, second_end).ok()?;
    let intersection = line_intersection(&first, &second)?;
    (segment_contains_point(first_start, first_end, intersection)
        && segment_contains_point(second_start, second_end, intersection))
    .then_some(intersection)
}

fn closest_point_on_segment(start: PgPoint, end: PgPoint, value: PgPoint) -> PgPoint {
    let dx = x(end) - x(start);
    let dy = y(end) - y(start);
    let denominator = dx * dx + dy * dy;
    if denominator == 0.0 {
        return start;
    }
    let projection =
        (((x(value) - x(start)) * dx + (y(value) - y(start)) * dy) / denominator).clamp(0.0, 1.0);
    point(x(start) + projection * dx, y(start) + projection * dy)
}

fn segment_distance_to_point(start: PgPoint, end: PgPoint, value: PgPoint) -> f64 {
    point_distance(closest_point_on_segment(start, end, value), value)
}

fn segment_distance(
    first_start: PgPoint,
    first_end: PgPoint,
    second_start: PgPoint,
    second_end: PgPoint,
) -> f64 {
    if segment_intersection(first_start, first_end, second_start, second_end).is_some() {
        return 0.0;
    }
    [
        segment_distance_to_point(first_start, first_end, second_start),
        segment_distance_to_point(first_start, first_end, second_end),
        segment_distance_to_point(second_start, second_end, first_start),
        segment_distance_to_point(second_start, second_end, first_end),
    ]
    .into_iter()
    .fold(f64::INFINITY, f64::min)
}

fn path_segments(closed: bool, points: &[PgPoint]) -> Vec<(PgPoint, PgPoint)> {
    let mut segments = points
        .windows(2)
        .map(|points| (points[0], points[1]))
        .collect::<Vec<_>>();
    if closed && points.len() > 1 {
        segments.push((*points.last().unwrap(), points[0]));
    }
    segments
}

fn path_length(closed: bool, points: &[PgPoint]) -> f64 {
    path_segments(closed, points)
        .into_iter()
        .map(|(start, end)| point_distance(start, end))
        .sum()
}

fn path_contains_point(closed: bool, points: &[PgPoint], value: PgPoint) -> bool {
    if points.len() == 1 {
        return point_same(points[0], value);
    }
    path_segments(closed, points)
        .into_iter()
        .any(|(start, end)| segment_contains_point(start, end, value))
}

fn path_distance_to_point(closed: bool, points: &[PgPoint], value: PgPoint) -> f64 {
    if points.len() == 1 {
        return point_distance(points[0], value);
    }
    path_segments(closed, points)
        .into_iter()
        .map(|(start, end)| segment_distance_to_point(start, end, value))
        .fold(f64::INFINITY, f64::min)
}

fn paths_intersect(
    left_closed: bool,
    left: &[PgPoint],
    right_closed: bool,
    right: &[PgPoint],
) -> bool {
    path_segments(left_closed, left).into_iter().any(|first| {
        path_segments(right_closed, right)
            .into_iter()
            .any(|second| segment_intersection(first.0, first.1, second.0, second.1).is_some())
    })
}

fn paths_distance(
    left_closed: bool,
    left: &[PgPoint],
    right_closed: bool,
    right: &[PgPoint],
) -> f64 {
    path_segments(left_closed, left)
        .into_iter()
        .flat_map(|first| {
            path_segments(right_closed, right)
                .into_iter()
                .map(move |second| segment_distance(first.0, first.1, second.0, second.1))
        })
        .fold(f64::INFINITY, f64::min)
}

fn polygon_area(points: &[PgPoint]) -> f64 {
    if points.len() < 2 {
        return 0.0;
    }
    path_segments(true, points)
        .into_iter()
        .map(|(start, end)| x(start) * y(end) - x(end) * y(start))
        .sum::<f64>()
        .abs()
        / 2.0
}

fn polygon_center(points: &[PgPoint]) -> PgPoint {
    let count = points.len() as f64;
    point(
        points.iter().copied().map(x).sum::<f64>() / count,
        points.iter().copied().map(y).sum::<f64>() / count,
    )
}

fn point_in_polygon(value: PgPoint, points: &[PgPoint]) -> bool {
    if points.len() == 1 {
        return point_same(value, points[0]);
    }
    if path_segments(true, points)
        .into_iter()
        .any(|(start, end)| segment_contains_point(start, end, value))
    {
        return true;
    }
    let mut inside = false;
    let mut previous = *points.last().unwrap();
    for current in points.iter().copied() {
        let crosses = (y(current) > y(value)) != (y(previous) > y(value));
        if crosses {
            let crossing_x = (x(previous) - x(current)) * (y(value) - y(current))
                / (y(previous) - y(current))
                + x(current);
            if x(value) < crossing_x {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

fn polygons_same(left: &[PgPoint], right: &[PgPoint]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    (0..right.len()).any(|start| {
        point_same(left[0], right[start])
            && (0..left.len())
                .all(|offset| point_same(left[offset], right[(start + offset) % right.len()]))
            || point_same(left[0], right[start])
                && (0..left.len()).all(|offset| {
                    point_same(
                        left[offset],
                        right[(start + right.len() - offset) % right.len()],
                    )
                })
    })
}

fn polygon_contains_polygon(outer: &[PgPoint], inner: &[PgPoint]) -> bool {
    inner
        .iter()
        .copied()
        .all(|value| point_in_polygon(value, outer))
}

fn polygons_overlap(left: &[PgPoint], right: &[PgPoint]) -> bool {
    left.iter()
        .copied()
        .any(|value| point_in_polygon(value, right))
        || right
            .iter()
            .copied()
            .any(|value| point_in_polygon(value, left))
        || paths_intersect(true, left, true, right)
}

fn polygon_distance_to_point(points: &[PgPoint], value: PgPoint) -> f64 {
    if point_in_polygon(value, points) {
        0.0
    } else {
        path_distance_to_point(true, points, value)
    }
}

#[derive(Clone, Copy)]
struct Bounds {
    high: PgPoint,
    low: PgPoint,
}

fn bounds_from_points(points: &[PgPoint]) -> Bounds {
    let mut min_x = x(points[0]);
    let mut max_x = min_x;
    let mut min_y = y(points[0]);
    let mut max_y = min_y;
    for value in points.iter().copied().skip(1) {
        min_x = min_x.min(x(value));
        max_x = max_x.max(x(value));
        min_y = min_y.min(y(value));
        max_y = max_y.max(y(value));
    }
    Bounds {
        high: point(max_x, max_y),
        low: point(min_x, min_y),
    }
}

fn geometric_bounds(value: &PgGeometric) -> Option<Bounds> {
    match value {
        PgGeometric::Point(value) => Some(Bounds {
            high: *value,
            low: *value,
        }),
        PgGeometric::Box { high, low } => Some(Bounds {
            high: *high,
            low: *low,
        }),
        PgGeometric::Path { points, .. } | PgGeometric::Polygon { points } => {
            Some(bounds_from_points(points))
        }
        PgGeometric::Circle { center, radius } => {
            let radius = radius.to_value();
            Some(Bounds {
                high: point(x(*center) + radius, y(*center) + radius),
                low: point(x(*center) - radius, y(*center) - radius),
            })
        }
        _ => None,
    }
}

fn normalized_box(first: PgPoint, second: PgPoint) -> PgGeometric {
    PgGeometric::Box {
        high: point(x(first).max(x(second)), y(first).max(y(second))),
        low: point(x(first).min(x(second)), y(first).min(y(second))),
    }
}

fn box_area(high: PgPoint, low: PgPoint) -> f64 {
    (x(high) - x(low)) * (y(high) - y(low))
}

fn box_contains_point(bounds: Bounds, value: PgPoint) -> bool {
    pg_float_ge(x(value), x(bounds.low))
        && pg_float_le(x(value), x(bounds.high))
        && pg_float_ge(y(value), y(bounds.low))
        && pg_float_le(y(value), y(bounds.high))
}

fn box_contains_box(outer: Bounds, inner: Bounds) -> bool {
    box_contains_point(outer, inner.low) && box_contains_point(outer, inner.high)
}

fn boxes_overlap(left: Bounds, right: Bounds) -> bool {
    pg_float_le(x(left.low), x(right.high))
        && pg_float_le(x(right.low), x(left.high))
        && pg_float_le(y(left.low), y(right.high))
        && pg_float_le(y(right.low), y(left.high))
}

fn box_edges(bounds: Bounds) -> [(PgPoint, PgPoint); 4] {
    let top_left = point(x(bounds.low), y(bounds.high));
    let bottom_right = point(x(bounds.high), y(bounds.low));
    [
        (bounds.low, top_left),
        (top_left, bounds.high),
        (bounds.high, bottom_right),
        (bottom_right, bounds.low),
    ]
}

fn closest_point_in_box(bounds: Bounds, value: PgPoint) -> PgPoint {
    // `f64::clamp` asserts `min <= max`, which a NaN bound violates — and
    // the geometric literal parser admits `nan`/`inf`, so
    // `point <-> box '((nan,5),(10,20))'` would panic mid-query. Ordering
    // with `min`/`max` is NaN-tolerant and keeps the operator total.
    fn clamp_finite(value: f64, low: f64, high: f64) -> f64 {
        let (low, high) = if low <= high {
            (low, high)
        } else {
            (high, low)
        };
        value.max(low).min(high)
    }
    point(
        clamp_finite(x(value), x(bounds.low), x(bounds.high)),
        clamp_finite(y(value), y(bounds.low), y(bounds.high)),
    )
}

fn box_segment_intersects(bounds: Bounds, start: PgPoint, end: PgPoint) -> bool {
    box_contains_point(bounds, start)
        || box_contains_point(bounds, end)
        || box_edges(bounds)
            .into_iter()
            .any(|edge| segment_intersection(edge.0, edge.1, start, end).is_some())
}

fn box_segment_distance(bounds: Bounds, start: PgPoint, end: PgPoint) -> f64 {
    if box_segment_intersects(bounds, start, end) {
        0.0
    } else {
        box_edges(bounds)
            .into_iter()
            .map(|edge| segment_distance(edge.0, edge.1, start, end))
            .fold(f64::INFINITY, f64::min)
    }
}

fn geometry_horizontal(value: &PgGeometric) -> bool {
    match value {
        PgGeometric::Line { a, .. } => a.to_value().abs() <= GEOMETRIC_EPSILON,
        PgGeometric::LineSegment { start, end } => pg_float_eq(y(*start), y(*end)),
        _ => false,
    }
}

fn geometry_vertical(value: &PgGeometric) -> bool {
    match value {
        PgGeometric::Line { b, .. } => b.to_value().abs() <= GEOMETRIC_EPSILON,
        PgGeometric::LineSegment { start, end } => pg_float_eq(x(*start), x(*end)),
        _ => false,
    }
}

fn geometry_slope(value: &PgGeometric) -> Option<f64> {
    match value {
        PgGeometric::Line { a, b, .. } => {
            if pg_float_eq(a.to_value(), 0.0) {
                Some(0.0)
            } else if pg_float_eq(b.to_value(), 0.0) {
                Some(f64::INFINITY)
            } else {
                Some(a.to_value() / -b.to_value())
            }
        }
        PgGeometric::LineSegment { start, end } => Some(point_slope(*start, *end)),
        _ => None,
    }
}

fn geometries_parallel(left: &PgGeometric, right: &PgGeometric) -> bool {
    match (geometry_slope(left), geometry_slope(right)) {
        (Some(left), Some(right)) => pg_float_eq(left, right),
        _ => false,
    }
}

fn geometries_perpendicular(left: &PgGeometric, right: &PgGeometric) -> bool {
    if geometry_horizontal(left) {
        return geometry_vertical(right);
    }
    if geometry_vertical(left) {
        return geometry_horizontal(right);
    }
    match (geometry_slope(left), geometry_slope(right)) {
        (Some(left), Some(right)) => pg_float_eq(left * right, -1.0),
        _ => false,
    }
}

fn geometric_area(value: &PgGeometric) -> Option<f64> {
    match value {
        PgGeometric::LineSegment { start, end } => Some(point_distance(*start, *end)),
        PgGeometric::Path { points, .. } => Some(points.len() as f64),
        PgGeometric::Box { high, low } => Some(box_area(*high, *low)),
        PgGeometric::Circle { radius, .. } => {
            Some(std::f64::consts::PI * radius.to_value().powi(2))
        }
        _ => None,
    }
}

fn geometric_truth(left: &PgGeometric, operator: &str, right: &PgGeometric) -> Result<bool> {
    match operator {
        "=" | "<>" | "<" | "<=" | ">" | ">=" => {
            if let (PgGeometric::Point(left), PgGeometric::Point(right)) = (left, right) {
                return Ok(!point_same(*left, *right));
            }
            if matches!(
                (left, right),
                (PgGeometric::Line { .. }, PgGeometric::Line { .. })
            ) {
                let equal = line_same(left, right);
                return Ok(if operator == "<>" { !equal } else { equal });
            }
            if operator == "=" || operator == "<>" {
                if let (
                    PgGeometric::LineSegment {
                        start: left_start,
                        end: left_end,
                    },
                    PgGeometric::LineSegment {
                        start: right_start,
                        end: right_end,
                    },
                ) = (left, right)
                {
                    let equal =
                        point_same(*left_start, *right_start) && point_same(*left_end, *right_end);
                    return Ok(if operator == "<>" { !equal } else { equal });
                }
            }
            let left = geometric_area(left).unwrap();
            let right = geometric_area(right).unwrap();
            Ok(match operator {
                "=" => pg_float_eq(left, right),
                "<>" => !pg_float_eq(left, right),
                "<" => pg_float_lt(left, right),
                "<=" => pg_float_le(left, right),
                ">" => pg_float_gt(left, right),
                ">=" => pg_float_ge(left, right),
                _ => unreachable!(),
            })
        }
        "~=" => Ok(match (left, right) {
            (PgGeometric::Point(left), PgGeometric::Point(right)) => point_same(*left, *right),
            (
                PgGeometric::Box {
                    high: left_high,
                    low: left_low,
                },
                PgGeometric::Box {
                    high: right_high,
                    low: right_low,
                },
            ) => point_same(*left_high, *right_high) && point_same(*left_low, *right_low),
            (PgGeometric::Polygon { points: left }, PgGeometric::Polygon { points: right }) => {
                polygons_same(left, right)
            }
            (
                PgGeometric::Circle {
                    center: left_center,
                    radius: left_radius,
                },
                PgGeometric::Circle {
                    center: right_center,
                    radius: right_radius,
                },
            ) => {
                point_same(*left_center, *right_center)
                    && (left_radius.to_value().is_nan() && right_radius.to_value().is_nan()
                        || pg_float_eq(left_radius.to_value(), right_radius.to_value()))
            }
            _ => false,
        }),
        "@>" => geometric_contains(left, right),
        "<@" => geometric_contains(right, left),
        "&&" => Ok(match (left, right) {
            (PgGeometric::Box { .. }, PgGeometric::Box { .. }) => boxes_overlap(
                geometric_bounds(left).unwrap(),
                geometric_bounds(right).unwrap(),
            ),
            (PgGeometric::Polygon { points: left }, PgGeometric::Polygon { points: right }) => {
                polygons_overlap(left, right)
            }
            (
                PgGeometric::Circle {
                    center: left_center,
                    radius: left_radius,
                },
                PgGeometric::Circle {
                    center: right_center,
                    radius: right_radius,
                },
            ) => pg_float_le(
                point_distance(*left_center, *right_center),
                left_radius.to_value() + right_radius.to_value(),
            ),
            _ => false,
        }),
        "<<" | ">>" | "<<|" | "|>>" | "&<" | "&>" | "&<|" | "|&>" | "<^" | ">^" => {
            positional_truth(left, operator, right)
        }
        "?#" => geometric_intersects(left, right),
        "?-" => match (left, right) {
            (PgGeometric::Point(left), PgGeometric::Point(right)) => {
                Ok(pg_float_eq(y(*left), y(*right)))
            }
            _ => Ok(false),
        },
        "?|" => match (left, right) {
            (PgGeometric::Point(left), PgGeometric::Point(right)) => {
                Ok(pg_float_eq(x(*left), x(*right)))
            }
            _ => Ok(false),
        },
        "?||" => Ok(geometries_parallel(left, right)),
        "?-|" => Ok(geometries_perpendicular(left, right)),
        _ => Err(SqlError::undefined_function(format!(
            "operator does not exist for geometric operands: {operator}"
        ))),
    }
}

fn positional_truth(left: &PgGeometric, operator: &str, right: &PgGeometric) -> Result<bool> {
    let left = geometric_bounds(left).ok_or_else(|| {
        SqlError::undefined_function("positional operator requires bounded geometry")
    })?;
    let right = geometric_bounds(right).ok_or_else(|| {
        SqlError::undefined_function("positional operator requires bounded geometry")
    })?;
    Ok(match operator {
        "<<" => pg_float_lt(x(left.high), x(right.low)),
        ">>" => pg_float_gt(x(left.low), x(right.high)),
        "<<|" | "<^" => pg_float_lt(y(left.high), y(right.low)),
        "|>>" | ">^" => pg_float_gt(y(left.low), y(right.high)),
        "&<" => pg_float_le(x(left.high), x(right.high)),
        "&>" => pg_float_ge(x(left.low), x(right.low)),
        "&<|" => pg_float_le(y(left.high), y(right.high)),
        "|&>" => pg_float_ge(y(left.low), y(right.low)),
        _ => unreachable!(),
    })
}

fn geometric_contains(outer: &PgGeometric, inner: &PgGeometric) -> Result<bool> {
    Ok(match (outer, inner) {
        (PgGeometric::Line { .. }, PgGeometric::Point(point)) => line_contains_point(outer, *point),
        (PgGeometric::Line { .. }, PgGeometric::LineSegment { start, end }) => {
            line_contains_point(outer, *start) && line_contains_point(outer, *end)
        }
        (PgGeometric::LineSegment { start, end }, PgGeometric::Point(point)) => {
            segment_contains_point(*start, *end, *point)
        }
        (PgGeometric::Path { closed, points }, PgGeometric::Point(point)) => {
            path_contains_point(*closed, points, *point)
        }
        (PgGeometric::Box { .. }, PgGeometric::Point(point)) => {
            box_contains_point(geometric_bounds(outer).unwrap(), *point)
        }
        (PgGeometric::Box { .. }, PgGeometric::LineSegment { start, end }) => {
            let bounds = geometric_bounds(outer).unwrap();
            box_contains_point(bounds, *start) && box_contains_point(bounds, *end)
        }
        (PgGeometric::Box { .. }, PgGeometric::Box { .. }) => box_contains_box(
            geometric_bounds(outer).unwrap(),
            geometric_bounds(inner).unwrap(),
        ),
        (PgGeometric::Polygon { points }, PgGeometric::Point(point)) => {
            point_in_polygon(*point, points)
        }
        (PgGeometric::Polygon { points: outer }, PgGeometric::Polygon { points: inner }) => {
            polygon_contains_polygon(outer, inner)
        }
        (PgGeometric::Circle { center, radius }, PgGeometric::Point(point)) => {
            point_distance(*center, *point) <= radius.to_value()
        }
        (
            PgGeometric::Circle {
                center: outer_center,
                radius: outer_radius,
            },
            PgGeometric::Circle {
                center: inner_center,
                radius: inner_radius,
            },
        ) => pg_float_le(
            point_distance(*outer_center, *inner_center),
            outer_radius.to_value() - inner_radius.to_value(),
        ),
        _ => false,
    })
}

fn geometric_intersects(left: &PgGeometric, right: &PgGeometric) -> Result<bool> {
    Ok(match (left, right) {
        (
            PgGeometric::LineSegment {
                start: left_start,
                end: left_end,
            },
            PgGeometric::LineSegment {
                start: right_start,
                end: right_end,
            },
        ) => segment_intersection(*left_start, *left_end, *right_start, *right_end).is_some(),
        (PgGeometric::LineSegment { start, end }, PgGeometric::Box { .. })
        | (PgGeometric::Box { .. }, PgGeometric::LineSegment { start, end }) => {
            let bounds = if matches!(left, PgGeometric::Box { .. }) {
                geometric_bounds(left).unwrap()
            } else {
                geometric_bounds(right).unwrap()
            };
            box_segment_intersects(bounds, *start, *end)
        }
        (PgGeometric::LineSegment { start, end }, PgGeometric::Line { .. }) => {
            let segment_line = line_from_points(*start, *end)?;
            line_intersection(&segment_line, right)
                .is_some_and(|point| segment_contains_point(*start, *end, point))
        }
        (
            PgGeometric::Path {
                closed: left_closed,
                points: left,
            },
            PgGeometric::Path {
                closed: right_closed,
                points: right,
            },
        ) => paths_intersect(*left_closed, left, *right_closed, right),
        (PgGeometric::Box { .. }, PgGeometric::Box { .. }) => boxes_overlap(
            geometric_bounds(left).unwrap(),
            geometric_bounds(right).unwrap(),
        ),
        (PgGeometric::Line { .. }, PgGeometric::Box { .. }) => {
            box_edges(geometric_bounds(right).unwrap())
                .into_iter()
                .any(|edge| {
                    line_from_points(edge.0, edge.1)
                        .ok()
                        .and_then(|edge_line| line_intersection(left, &edge_line))
                        .is_some_and(|point| segment_contains_point(edge.0, edge.1, point))
                })
        }
        (PgGeometric::Line { .. }, PgGeometric::Line { .. }) => {
            line_intersection(left, right).is_some()
        }
        _ => false,
    })
}

fn geometric_distance(left: &PgGeometric, right: &PgGeometric) -> Result<f64> {
    Ok(match (left, right) {
        (PgGeometric::Point(left), PgGeometric::Point(right)) => point_distance(*left, *right),
        (PgGeometric::Point(point), PgGeometric::LineSegment { start, end })
        | (PgGeometric::LineSegment { start, end }, PgGeometric::Point(point)) => {
            segment_distance_to_point(*start, *end, *point)
        }
        (PgGeometric::Point(point), PgGeometric::Path { closed, points })
        | (PgGeometric::Path { closed, points }, PgGeometric::Point(point)) => {
            path_distance_to_point(*closed, points, *point)
        }
        (PgGeometric::Point(point), PgGeometric::Box { .. })
        | (PgGeometric::Box { .. }, PgGeometric::Point(point)) => point_distance(
            *point,
            closest_point_in_box(
                geometric_bounds(if matches!(left, PgGeometric::Box { .. }) {
                    left
                } else {
                    right
                })
                .unwrap(),
                *point,
            ),
        ),
        (PgGeometric::Point(point), PgGeometric::Polygon { points })
        | (PgGeometric::Polygon { points }, PgGeometric::Point(point)) => {
            polygon_distance_to_point(points, *point)
        }
        (PgGeometric::Point(point), PgGeometric::Line { .. })
        | (PgGeometric::Line { .. }, PgGeometric::Point(point)) => line_distance_to_point(
            if matches!(left, PgGeometric::Line { .. }) {
                left
            } else {
                right
            },
            *point,
        ),
        (PgGeometric::Point(point), PgGeometric::Circle { center, radius })
        | (PgGeometric::Circle { center, radius }, PgGeometric::Point(point)) => {
            (point_distance(*point, *center) - radius.to_value()).max(0.0)
        }
        (
            PgGeometric::LineSegment {
                start: first_start,
                end: first_end,
            },
            PgGeometric::LineSegment {
                start: second_start,
                end: second_end,
            },
        ) => segment_distance(*first_start, *first_end, *second_start, *second_end),
        (PgGeometric::LineSegment { start, end }, PgGeometric::Box { .. })
        | (PgGeometric::Box { .. }, PgGeometric::LineSegment { start, end }) => {
            box_segment_distance(
                geometric_bounds(if matches!(left, PgGeometric::Box { .. }) {
                    left
                } else {
                    right
                })
                .unwrap(),
                *start,
                *end,
            )
        }
        (PgGeometric::LineSegment { start, end }, PgGeometric::Line { .. })
        | (PgGeometric::Line { .. }, PgGeometric::LineSegment { start, end }) => {
            let line = if matches!(left, PgGeometric::Line { .. }) {
                left
            } else {
                right
            };
            let segment_line = line_from_points(*start, *end)?;
            if line_intersection(line, &segment_line)
                .is_some_and(|point| segment_contains_point(*start, *end, point))
            {
                0.0
            } else {
                line_distance_to_point(line, *start).min(line_distance_to_point(line, *end))
            }
        }
        (
            PgGeometric::Path {
                closed: left_closed,
                points: left,
            },
            PgGeometric::Path {
                closed: right_closed,
                points: right,
            },
        ) => paths_distance(*left_closed, left, *right_closed, right),
        (
            PgGeometric::Box {
                high: left_high,
                low: left_low,
            },
            PgGeometric::Box {
                high: right_high,
                low: right_low,
            },
        ) => point_distance(
            midpoint(*left_high, *left_low),
            midpoint(*right_high, *right_low),
        ),
        (PgGeometric::Polygon { points: left }, PgGeometric::Polygon { points: right }) => {
            if polygons_overlap(left, right) {
                0.0
            } else {
                left.iter()
                    .copied()
                    .map(|point| polygon_distance_to_point(right, point))
                    .chain(
                        right
                            .iter()
                            .copied()
                            .map(|point| polygon_distance_to_point(left, point)),
                    )
                    .fold(f64::INFINITY, f64::min)
            }
        }
        (PgGeometric::Polygon { points }, PgGeometric::Circle { center, radius })
        | (PgGeometric::Circle { center, radius }, PgGeometric::Polygon { points }) => {
            (polygon_distance_to_point(points, *center) - radius.to_value()).max(0.0)
        }
        (PgGeometric::Line { .. }, PgGeometric::Line { .. }) => {
            if line_intersection(left, right).is_some() {
                0.0
            } else {
                let (a1, b1, c1) = line_coefficients(left).unwrap();
                let (a2, b2, c2) = line_coefficients(right).unwrap();
                let ratio = if !pg_float_eq(a1, 0.0) && !pg_float_eq(a2, 0.0) {
                    a1 / a2
                } else if !pg_float_eq(b1, 0.0) && !pg_float_eq(b2, 0.0) {
                    b1 / b2
                } else {
                    1.0
                };
                (c1 - ratio * c2).abs() / a1.hypot(b1)
            }
        }
        (
            PgGeometric::Circle {
                center: left_center,
                radius: left_radius,
            },
            PgGeometric::Circle {
                center: right_center,
                radius: right_radius,
            },
        ) => (point_distance(*left_center, *right_center)
            - left_radius.to_value()
            - right_radius.to_value())
        .max(0.0),
        _ => {
            return Err(SqlError::undefined_function(
                "distance is not defined for these geometric operands",
            ))
        }
    })
}

fn geometric_intersection_point(
    left: &PgGeometric,
    right: &PgGeometric,
) -> Result<Option<PgPoint>> {
    Ok(match (left, right) {
        (PgGeometric::Line { .. }, PgGeometric::Line { .. }) => line_intersection(left, right),
        (
            PgGeometric::LineSegment {
                start: left_start,
                end: left_end,
            },
            PgGeometric::LineSegment {
                start: right_start,
                end: right_end,
            },
        ) => segment_intersection(*left_start, *left_end, *right_start, *right_end),
        _ => None,
    })
}

fn geometric_closest_point(left: &PgGeometric, right: &PgGeometric) -> Result<Option<PgPoint>> {
    Ok(match (left, right) {
        (PgGeometric::Point(point), PgGeometric::LineSegment { start, end }) => {
            Some(closest_point_on_segment(*start, *end, *point))
        }
        (PgGeometric::Point(point), PgGeometric::Box { .. }) => Some(closest_point_in_box(
            geometric_bounds(right).unwrap(),
            *point,
        )),
        (PgGeometric::Point(point), PgGeometric::Line { .. }) => {
            closest_point_on_line(right, *point)
        }
        (
            PgGeometric::LineSegment {
                start: left_start,
                end: left_end,
            },
            PgGeometric::LineSegment {
                start: right_start,
                end: right_end,
            },
        ) => {
            if point_slope(*left_start, *left_end) == point_slope(*right_start, *right_end) {
                None
            } else if let Some(intersection) =
                segment_intersection(*left_start, *left_end, *right_start, *right_end)
            {
                Some(intersection)
            } else {
                let mut closest = closest_point_on_segment(*right_start, *right_end, *left_start);
                let mut distance = point_distance(closest, *left_start);

                let candidate = closest_point_on_segment(*right_start, *right_end, *left_end);
                let candidate_distance = point_distance(candidate, *left_end);
                if candidate_distance < distance {
                    closest = candidate;
                    distance = candidate_distance;
                }

                for candidate in [*right_start, *right_end] {
                    let candidate_distance =
                        segment_distance_to_point(*left_start, *left_end, candidate);
                    if candidate_distance < distance {
                        closest = candidate;
                        distance = candidate_distance;
                    }
                }
                Some(closest)
            }
        }
        (PgGeometric::LineSegment { start, end }, PgGeometric::Box { .. }) => {
            let bounds = geometric_bounds(right).unwrap();
            if box_segment_intersects(bounds, *start, *end) {
                Some(closest_point_on_segment(
                    *start,
                    *end,
                    midpoint(bounds.high, bounds.low),
                ))
            } else {
                box_edges(bounds)
                    .into_iter()
                    .flat_map(|edge| [edge.0, edge.1])
                    .min_by(|left, right| {
                        segment_distance_to_point(*start, *end, *left)
                            .total_cmp(&segment_distance_to_point(*start, *end, *right))
                    })
            }
        }
        (PgGeometric::Line { .. }, PgGeometric::LineSegment { start, end }) => {
            let segment_line = line_from_points(*start, *end)?;
            if geometries_parallel(left, &segment_line) {
                None
            } else {
                line_intersection(left, &segment_line)
                    .filter(|point| segment_contains_point(*start, *end, *point))
                    .or_else(|| {
                        [*start, *end]
                            .into_iter()
                            .min_by(|left_point, right_point| {
                                line_distance_to_point(left, *left_point)
                                    .total_cmp(&line_distance_to_point(left, *right_point))
                            })
                    })
            }
        }
        _ => None,
    })
}

fn box_intersection(left: &PgGeometric, right: &PgGeometric) -> Result<Option<PgGeometric>> {
    let left = geometric_bounds(left).unwrap();
    let right = geometric_bounds(right).unwrap();
    if !boxes_overlap(left, right) {
        return Ok(None);
    }
    Ok(Some(PgGeometric::Box {
        high: point(
            x(left.high).min(x(right.high)),
            y(left.high).min(y(right.high)),
        ),
        low: point(x(left.low).max(x(right.low)), y(left.low).max(y(right.low))),
    }))
}

fn geometric_transform(
    left: PgGeometric,
    operator: &str,
    right: PgGeometric,
) -> Result<PgGeometric> {
    if operator == "+" {
        if let (
            PgGeometric::Path {
                closed: left_closed,
                points: mut left,
            },
            PgGeometric::Path {
                closed: right_closed,
                points: right,
            },
        ) = (left.clone(), right.clone())
        {
            if left_closed || right_closed {
                return Err(SqlError::invalid_parameter_value(
                    "open paths are required for concatenation",
                ));
            }
            left.extend(right);
            return Ok(PgGeometric::Path {
                closed: false,
                points: left,
            });
        }
    }
    let PgGeometric::Point(transform) = right else {
        return Err(SqlError::undefined_function(
            "geometric transformation requires a point",
        ));
    };
    let apply = |value| match operator {
        "+" => point_add(value, transform),
        "-" => point_sub(value, transform),
        "*" => point_mul(value, transform),
        "/" => point_div(value, transform),
        _ => value,
    };
    Ok(match left {
        PgGeometric::Point(value) => PgGeometric::Point(apply(value)),
        PgGeometric::Path { closed, points } => PgGeometric::Path {
            closed,
            points: points.into_iter().map(apply).collect(),
        },
        PgGeometric::Box { high, low } => normalized_box(apply(high), apply(low)),
        PgGeometric::Circle { center, radius } => {
            let magnitude = x(transform).hypot(y(transform));
            PgGeometric::Circle {
                center: apply(center),
                radius: PgFloat8::from_value(match operator {
                    "*" => radius.to_value() * magnitude,
                    "/" => radius.to_value() / magnitude,
                    _ => radius.to_value(),
                }),
            }
        }
        _ => {
            return Err(SqlError::undefined_function(
                "transformation is not defined for this geometric type",
            ))
        }
    })
}

fn eval_point_constructor(args: &[SqlValue], arg_types: &[Option<String>]) -> Result<SqlValue> {
    match args.len() {
        2 => Ok(point_sql_value(point(
            numeric_argument(&args[0], "point")?,
            numeric_argument(&args[1], "point")?,
        ))),
        1 => Ok(point_sql_value(
            match typed_geometric_argument(
                args,
                arg_types,
                0,
                &["circle", "lseg", "box", "polygon"],
            )? {
                PgGeometric::Circle { center, .. } => center,
                PgGeometric::LineSegment { start, end } => midpoint(start, end),
                PgGeometric::Box { high, low } => midpoint(high, low),
                PgGeometric::Polygon { points } => polygon_center(&points),
                _ => unreachable!(),
            },
        )),
        _ => Err(SqlError::undefined_function(
            "function point with supplied arguments does not exist",
        )),
    }
}

fn eval_box_constructor(args: &[SqlValue], arg_types: &[Option<String>]) -> Result<SqlValue> {
    let geometry = match args.len() {
        2 => normalized_box(point_argument(&args[0])?, point_argument(&args[1])?),
        1 => match typed_geometric_argument(args, arg_types, 0, &["circle", "point", "polygon"])? {
            PgGeometric::Circle { center, radius } => {
                let delta = radius.to_value() / 2.0_f64.sqrt();
                normalized_box(
                    point(x(center) + delta, y(center) + delta),
                    point(x(center) - delta, y(center) - delta),
                )
            }
            PgGeometric::Point(value) => normalized_box(value, value),
            PgGeometric::Polygon { points } => {
                let bounds = bounds_from_points(&points);
                PgGeometric::Box {
                    high: bounds.high,
                    low: bounds.low,
                }
            }
            _ => unreachable!(),
        },
        _ => {
            return Err(SqlError::undefined_function(
                "function box with supplied arguments does not exist",
            ))
        }
    };
    Ok(geometric_sql_value(geometry))
}

fn eval_bound_box(args: &[SqlValue]) -> Result<SqlValue> {
    require_geometric_args("bound_box", args, 2)?;
    let left = geometric_bounds(&geometric_argument(&args[0], "box")?).unwrap();
    let right = geometric_bounds(&geometric_argument(&args[1], "box")?).unwrap();
    Ok(geometric_sql_value(PgGeometric::Box {
        high: point(
            x(left.high).max(x(right.high)),
            y(left.high).max(y(right.high)),
        ),
        low: point(x(left.low).min(x(right.low)), y(left.low).min(y(right.low))),
    }))
}

fn eval_circle_constructor(args: &[SqlValue], arg_types: &[Option<String>]) -> Result<SqlValue> {
    let circle = match args.len() {
        2 => PgGeometric::Circle {
            center: point_argument(&args[0])?,
            radius: PgFloat8::from_value(numeric_argument(&args[1], "circle")?),
        },
        1 => match typed_geometric_argument(args, arg_types, 0, &["box", "polygon"])? {
            PgGeometric::Box { high, low } => PgGeometric::Circle {
                center: midpoint(high, low),
                radius: PgFloat8::from_value(point_distance(midpoint(high, low), high)),
            },
            PgGeometric::Polygon { points } => {
                let center = polygon_center(&points);
                let radius = points
                    .iter()
                    .copied()
                    .map(|value| point_distance(value, center))
                    .sum::<f64>()
                    / points.len() as f64;
                PgGeometric::Circle {
                    center,
                    radius: PgFloat8::from_value(radius),
                }
            }
            _ => unreachable!(),
        },
        _ => {
            return Err(SqlError::undefined_function(
                "function circle with supplied arguments does not exist",
            ))
        }
    };
    Ok(geometric_sql_value(circle))
}

fn eval_lseg_constructor(args: &[SqlValue], _arg_types: &[Option<String>]) -> Result<SqlValue> {
    let (start, end) = match args.len() {
        2 => (point_argument(&args[0])?, point_argument(&args[1])?),
        1 => {
            let PgGeometric::Box { high, low } = geometric_argument(&args[0], "box")? else {
                unreachable!()
            };
            (high, low)
        }
        _ => {
            return Err(SqlError::undefined_function(
                "function lseg with supplied arguments does not exist",
            ))
        }
    };
    Ok(geometric_sql_value(PgGeometric::LineSegment { start, end }))
}

fn eval_polygon_constructor(args: &[SqlValue], arg_types: &[Option<String>]) -> Result<SqlValue> {
    let points = match args.len() {
        2 => {
            let count = int_argument(&args[0], "polygon")?;
            if count < 2 {
                return Err(SqlError::invalid_parameter_value(
                    "must request at least 2 points",
                ));
            }
            // The point count is a client-supplied i64 driving a Vec: a
            // request for a billion-point polygon is an allocation attack,
            // not a query (`bicdb_core::parse_budget`).
            if count > MAX_POLYGON_POINTS {
                return Err(SqlError::invalid_parameter_value(format!(
                    "cannot build a polygon with more than {MAX_POLYGON_POINTS} points"
                )));
            }
            let PgGeometric::Circle { center, radius } = geometric_argument(&args[1], "circle")?
            else {
                unreachable!()
            };
            circle_polygon_points(count as usize, center, radius.to_value())?
        }
        1 => match typed_geometric_argument(args, arg_types, 0, &["circle", "box", "path"])? {
            PgGeometric::Circle { center, radius } => {
                circle_polygon_points(12, center, radius.to_value())?
            }
            PgGeometric::Box { high, low } => {
                vec![low, point(x(low), y(high)), high, point(x(high), y(low))]
            }
            PgGeometric::Path {
                closed: true,
                points,
            } => points,
            PgGeometric::Path { closed: false, .. } => {
                return Err(SqlError::invalid_parameter_value(
                    "open path cannot be converted to polygon",
                ));
            }
            _ => unreachable!(),
        },
        _ => {
            return Err(SqlError::undefined_function(
                "function polygon with supplied arguments does not exist",
            ))
        }
    };
    Ok(geometric_sql_value(PgGeometric::Polygon { points }))
}

/// Ceiling on points synthesized for `polygon(n, circle)`. PostgreSQL is
/// bounded by its 1 GB varlena limit; this is the same order of magnitude
/// in points and far past any drawable shape.
const MAX_POLYGON_POINTS: i64 = 1_000_000;

fn circle_polygon_points(count: usize, center: PgPoint, radius: f64) -> Result<Vec<PgPoint>> {
    if pg_float_eq(radius, 0.0) {
        return Err(SqlError::Unsupported(
            "cannot convert circle with radius zero to polygon".to_string(),
        ));
    }
    Ok((0..count)
        .map(|index| {
            let angle = 2.0 * std::f64::consts::PI * index as f64 / count as f64;
            point(
                x(center) - radius * angle.cos(),
                y(center) + radius * angle.sin(),
            )
        })
        .collect())
}
