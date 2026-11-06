//! Source CAST defaults must not reinterpret generated native numeric carriers.
use super::*;
use sqlparser::ast::{visit_expressions_mut, ExactNumberInfo};

pub(super) fn source_cast_defaults(source: &str) -> Option<String> {
    let upper = source.to_ascii_uppercase();
    if !upper.contains("CAST")
        || (!upper.contains("NUMERIC") && !upper.contains("DECIMAL") && !upper.contains("DEC"))
    {
        return None;
    }
    // Extended routine/block grammar has a separate source compiler. Do not
    // infer a source type from SQL that compiler has already canonicalized.
    let Ok(mut statements) = Parser::parse_sql(&OracleDialect {}, source) else {
        return None;
    };
    let mut changed = false;
    let _: ControlFlow<()> = visit_expressions_mut(&mut statements, |expr| {
        if let Expr::Cast { data_type, .. } = expr {
            if let DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) =
                data_type
            {
                if matches!(info, ExactNumberInfo::None) {
                    *info = ExactNumberInfo::PrecisionAndScale(38, 0);
                    changed = true;
                }
            }
        }
        ControlFlow::Continue(())
    });
    changed.then(|| {
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_numeric_cast_defaults_preserve_number_and_explicit_modifiers() {
        let sql = source_cast_defaults("SELECT CAST(15.193 AS NUMBER),CAST(15.193 AS NUMERIC),CAST(15.193 AS DECIMAL),CAST(15.193 AS DEC),CAST(15.193 AS NUMERIC(8,3)) FROM DUAL").unwrap();
        assert!(sql.contains("AS NUMBER)"), "{sql}");
        assert!(sql.contains("AS NUMERIC(38,0))"), "{sql}");
        assert!(sql.contains("AS DECIMAL(38,0))"), "{sql}");
        assert!(sql.contains("AS DEC(38,0))"), "{sql}");
        assert!(sql.contains("AS NUMERIC(8,3))"), "{sql}");
    }

    #[test]
    fn source_numeric_cast_defaults_leave_parameters_literals_and_other_sql_alone() {
        for sql in [
            "SELECT $1 FROM DUAL",
            "SELECT 'CAST(1 AS NUMERIC)' FROM DUAL",
            "SELECT CAST(15.193 AS NUMBER) FROM DUAL",
        ] {
            assert!(source_cast_defaults(sql).is_none(), "{sql}");
        }
    }
}
