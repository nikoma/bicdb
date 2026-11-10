//! Typed scalar function identity for the PostgreSQL FunctionCall frontend.
//! Execution stays in SqlSession, so this metadata grants no execution rights.
use crate::*;

#[derive(Clone, Debug)]
pub struct PgFunctionCallSignature {
    pub sql_name: String,
    pub arguments: Vec<(i32, String)>,
    pub result_oid: i32,
    pub variadic: bool,
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn number(row: &BTreeMap<String, SqlValue>, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Int(n)) => Ok(*n),
        _ => Err(SqlError::data_exception(
            "XX001",
            "invalid function catalog number",
            None,
        )),
    }
}
fn text<'a>(row: &'a BTreeMap<String, SqlValue>, key: &str) -> Result<&'a str> {
    match row.get(key) {
        Some(SqlValue::String(s)) => Ok(s),
        _ => Err(SqlError::data_exception(
            "XX001",
            "invalid function catalog text",
            None,
        )),
    }
}
fn namespace(db: &BicDb, oid: i64) -> Result<String> {
    match oid {
        11 => Ok("pg_catalog".into()),
        2200 => Ok("public".into()),
        _ => list_namespaces(db)?
            .into_iter()
            .find(|n| namespace_oid(&n.name) == oid)
            .map(|n| n.name)
            .ok_or_else(|| {
                SqlError::data_exception("XX001", "function namespace is missing", None)
            }),
    }
}
fn type_sql(db: &BicDb, oid: i32) -> Result<String> {
    if let Some(spec) = pg_type_spec_by_oid(oid) {
        return Ok(format!("pg_catalog.{}", quote(spec.name)));
    }
    if let Some(element) = pg_array_element_spec_by_oid(oid) {
        return Ok(format!("pg_catalog.{}[]", quote(element.name)));
    }
    let types = pg_type_rows_for_db(db)?;
    let row = types
        .iter()
        .find(|row| row.get("oid") == Some(&SqlValue::Int(oid as i64)))
        .ok_or_else(|| {
            SqlError::Unsupported(format!("function argument type {oid} is unavailable"))
        })?;
    Ok(format!(
        "{}.{}",
        quote(&namespace(db, number(row, "typnamespace")?)?),
        quote(text(row, "typname")?)
    ))
}

/// Read actual catalog metadata, including authored scalar functions. OID
/// resolution cannot substitute a different overload when SQL executes it.
pub fn postgres_function_call_signature(
    db: &BicDb,
    oid: u32,
) -> Result<Option<PgFunctionCallSignature>> {
    db.check_recovery_state()?;
    let rows = pg_proc_rows_shared(db)?;
    let Some(row) = rows
        .iter()
        .find(|row| row.get("oid") == Some(&SqlValue::Int(oid as i64)))
    else {
        return Ok(None);
    };
    if text(row, "prokind")? != "f" || row.get("proretset") == Some(&SqlValue::Bool(true)) {
        return Err(SqlError::Unsupported(
            "FunctionCall requires a scalar function".into(),
        ));
    }
    let arguments = text(row, "proargtypes")?
        .split_whitespace()
        .map(|oid| {
            let oid: i32 = oid.parse().map_err(|_| {
                SqlError::data_exception("XX001", "invalid function argument OID", None)
            })?;
            Ok((oid, type_sql(db, oid)?))
        })
        .collect::<Result<Vec<_>>>()?;
    if number(row, "pronargs")? != arguments.len() as i64 {
        return Err(SqlError::Unsupported(
            "FunctionCall requires a complete catalog signature".into(),
        ));
    }
    let result_oid = i32::try_from(number(row, "prorettype")?)
        .map_err(|_| SqlError::data_exception("XX001", "invalid function result OID", None))?;
    let signature = PgFunctionCallSignature {
        sql_name: format!(
            "{}.{}",
            quote(&namespace(db, number(row, "pronamespace")?)?),
            quote(text(row, "proname")?)
        ),
        arguments,
        result_oid,
        variadic: number(row, "provariadic")? != 0,
    };
    db.check_recovery_state()?;
    Ok(Some(signature))
}
