//! PostgreSQL host parameters in raw Oracle anonymous submissions.
//! Native DO and stored routine bodies keep their ordinary literal boundary.
use super::*;

#[derive(Clone, Debug)]
enum Part {
    Sql(String),
    Parameter(usize),
}

#[derive(Clone, Debug)]
pub struct AnonymousBlock {
    parts: Vec<Part>,
    namespace: String,
    parameter_types: Vec<Option<String>>,
}

fn parameter(token: &Token) -> Result<Option<usize>> {
    let text = match token {
        Token::Placeholder(value) => value.as_str(),
        Token::Word(value) if value.quote_style.is_none() => value.value.as_str(),
        _ => return Ok(None),
    };
    let Some(number) = text.strip_prefix('$') else {
        return Ok(None);
    };
    if number.is_empty() || !number.bytes().all(|c| c.is_ascii_digit()) {
        return Ok(None);
    }
    let index = number
        .parse::<usize>()
        .map_err(|_| unsupported("host parameter index is too large"))?;
    if index == 0 || index > i16::MAX as usize {
        return Err(unsupported(
            "host parameter index is outside the protocol range",
        ));
    }
    Ok(Some(index - 1))
}

/// Capture only a raw preview block that the Oracle front end has lowered.
/// Dollar-quoted DO/function source never introduces outer host parameters.
pub fn prepare(
    db: &BicDb,
    source: &str,
    lowered: &str,
    profile: &str,
    search_path: Option<&str>,
    user: &str,
) -> Result<Option<AnonymousBlock>> {
    if profile != ORACLE_PREVIEW_PROFILE {
        return Ok(None);
    }
    let source = tokens(source)?;
    let block = source
        .first()
        .is_some_and(|t| word(t, "BEGIN") || word(t, "DECLARE"))
        && source.iter().any(|t| word(t, "BEGIN"))
        && source.iter().any(|t| word(t, "END"));
    if !block {
        return Ok(None);
    }
    let Some(body) = raw_do_block_body(lowered)? else {
        return Ok(None);
    };
    if raw_do_block_language(lowered)?.as_deref() != Some(PREVIEW_ROUTINE_LANGUAGE) {
        return Err(unsupported(
            "host block language does not match the preview compiler",
        ));
    }
    let mut parts = Vec::new();
    let mut text = String::new();
    let mut count = 0;
    for token in source {
        if let Some(index) = parameter(&token)? {
            if !text.is_empty() {
                parts.push(Part::Sql(std::mem::take(&mut text)));
            }
            parts.push(Part::Parameter(index));
            count = count.max(index + 1);
        } else {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(&render_token(&token));
        }
    }
    if count == 0 {
        return Ok(None);
    }
    if !text.is_empty() {
        parts.push(Part::Sql(text));
    }
    let namespace = preview_schema(db, search_path, user)?.unwrap_or_else(default_schema_name);
    let (mut declarations, mut statements, mut handlers) = parse_plpgsql_body(&body)?;
    routines::bind_block_namespace(
        Some(db),
        &mut declarations,
        &mut statements,
        &mut handlers,
        &namespace,
        true,
    )?;
    let mut parameter_types =
        infer_routine_host_parameter_types(db, &declarations, &statements, &handlers);
    parameter_types.resize(count, None);
    Ok(Some(AnonymousBlock {
        parts,
        namespace,
        parameter_types,
    }))
}

impl AnonymousBlock {
    pub fn memory_estimate(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self.namespace.capacity()
            + self.parts.capacity() * std::mem::size_of::<Part>()
            + self
                .parts
                .iter()
                .map(|part| match part {
                    Part::Sql(sql) => sql.capacity(),
                    Part::Parameter(_) => 0,
                })
                .sum::<usize>()
            + self.parameter_types.capacity() * std::mem::size_of::<Option<String>>()
            + self
                .parameter_types
                .iter()
                .flatten()
                .map(String::capacity)
                .sum::<usize>()
    }

    pub fn parameter_types(&self) -> &[Option<String>] {
        &self.parameter_types
    }

    /// Values must be SQL literals produced by the wire codec, never raw input.
    /// Re-lower the tokenized Oracle source after binding so conversions and a
    /// fresh dollar delimiter include the actual values. Literal/comment text
    /// cannot become a parameter, identifier or block terminator.
    pub fn bind(&self, db: &BicDb, values: &[String]) -> Result<String> {
        if values.len() < self.parameter_types.len() {
            return Err(SqlError::InvalidSql(
                "anonymous block parameter count mismatch".into(),
            ));
        }
        let source = self
            .parts
            .iter()
            .map(|part| match part {
                Part::Sql(text) => text.as_str(),
                Part::Parameter(index) => values[*index].as_str(),
            })
            .collect::<Vec<_>>()
            .join(" ");
        lower_profile_sql_in_namespace(db, &source, ORACLE_PREVIEW_PROFILE, Some(&self.namespace))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oracle_anonymous_host_markers_respect_literal_boundaries_and_reselect_delimiters() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let source = "BEGIN INSERT INTO host_rows(k,v) VALUES($1,'literal $2'); END;";
        let lowered =
            lower_session_sql(&db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb").unwrap();
        let block = prepare(&db, source, &lowered, ORACLE_PREVIEW_PROFILE, None, "bicdb")
            .unwrap()
            .unwrap();
        assert_eq!(block.parameter_types().len(), 1);
        let bound = block
            .bind(&db, &["'$bicdb_oracle_0$; punctuation'".into()])
            .unwrap();
        assert!(bound.contains("$bicdb_oracle_1$"));
        assert!(bound.contains("'literal $2'"));
        assert_eq!(split_sql_statements(&bound).len(), 1);
        assert!(prepare(
            &db,
            "DO $$BEGIN PERFORM $1; END$$",
            "DO $$BEGIN PERFORM $1; END$$",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb"
        )
        .unwrap()
        .is_none());
        assert!(
            prepare(&db, source, &lowered, POSTGRES_PROFILE, None, "bicdb")
                .unwrap()
                .is_none()
        );
    }
}
