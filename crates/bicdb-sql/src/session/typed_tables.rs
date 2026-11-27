//! Typed tables: `CREATE TABLE name OF composite_type [( column WITH OPTIONS
//! ..., table_constraint, ... )]`.
//!
//! The table's columns are the type's attributes, in order; the optional
//! list adds defaults and constraints to them and table constraints. As in
//! PostgreSQL the columns follow the type, so `ALTER TABLE` cannot add, drop,
//! rename or retype them or change the table's inheritance, `ALTER TYPE`
//! refuses to change a type that has typed tables, and `DROP TYPE` needs
//! `CASCADE`, which drops the typed tables too. `pg_class.reloftype` names
//! the type.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

fn keyword(token: Option<&&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn ident(token: Option<&&Token>) -> Option<String> {
    match token? {
        Token::Word(word) if word.quote_style == Some('"') => Some(word.value.clone()),
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_lowercase()),
        _ => None,
    }
}

struct TypedTable {
    /// `CREATE [...] TABLE [IF NOT EXISTS] name` as written.
    head: String,
    table: Vec<String>,
    if_not_exists: bool,
    type_name: Vec<String>,
    /// The definition-list elements: (first word, quoted?, text after it
    /// and an optional `WITH OPTIONS`, the whole element).
    elements: Vec<(String, bool, String, String)>,
    /// Everything after the type name and list (PARTITION BY, WITH, ...).
    tail: String,
}

fn parse(sql: &str) -> Option<TypedTable> {
    let head = sql.trim_start();
    if !(head.len() >= 6 && head[..6].eq_ignore_ascii_case("CREATE"))
        || !sql.to_ascii_lowercase().contains(" of ")
    {
        return None;
    }
    let raw = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()?;
    let sig: Vec<usize> = raw
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::SemiColon))
        .map(|(index, _)| index)
        .collect();
    if raw
        .iter()
        .filter(|token| **token == Token::SemiColon)
        .count()
        > 1
    {
        return None;
    }
    let tokens: Vec<&Token> = sig.iter().map(|&index| &raw[index]).collect();
    let text = |from: usize, to: usize| {
        if from >= to {
            return String::new();
        }
        raw[sig[from]..=sig[to - 1]]
            .iter()
            .map(crate::eval::rewrite_helpers::token_sql)
            .collect::<String>()
    };
    let mut at = 1;
    while ["GLOBAL", "LOCAL", "TEMP", "TEMPORARY", "UNLOGGED"]
        .iter()
        .any(|word| keyword(tokens.get(at), word))
    {
        at += 1;
    }
    if !keyword(tokens.first(), "CREATE") || !keyword(tokens.get(at), "TABLE") {
        return None;
    }
    at += 1;
    let mut if_not_exists = false;
    if keyword(tokens.get(at), "IF") {
        if_not_exists = true;
        at += 3;
    }
    let name_at = at;
    let mut table = vec![ident(tokens.get(at))?];
    at += 1;
    while tokens.get(at) == Some(&&Token::Period) {
        table.push(ident(tokens.get(at + 1))?);
        at += 2;
    }
    let head = text(0, at);
    let _ = name_at;
    if !keyword(tokens.get(at), "OF") {
        return None;
    }
    at += 1;
    let mut type_name = vec![ident(tokens.get(at))?];
    at += 1;
    while tokens.get(at) == Some(&&Token::Period) {
        type_name.push(ident(tokens.get(at + 1))?);
        at += 2;
    }
    let mut elements = Vec::new();
    if tokens.get(at) == Some(&&Token::LParen) {
        let mut depth = 0;
        let mut start = at + 1;
        let mut close = None;
        for position in at..tokens.len() {
            match tokens[position] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(position);
                        break;
                    }
                }
                Token::Comma if depth == 1 => {
                    elements.push((start, position));
                    start = position + 1;
                }
                _ => {}
            }
        }
        let close = close?;
        elements.push((start, close));
        at = close + 1;
    }
    let elements = elements
        .into_iter()
        .filter(|(start, end)| start < end)
        .map(|(start, end)| {
            let quoted = matches!(tokens[start], Token::Word(word) if word.quote_style.is_some());
            let first = ident(tokens.get(start)).unwrap_or_else(|| text(start, start + 1));
            let rest = if keyword(tokens.get(start + 1), "WITH")
                && keyword(tokens.get(start + 2), "OPTIONS")
            {
                text(start + 3, end)
            } else {
                text(start + 1, end)
            };
            (first, quoted, rest, text(start, end))
        })
        .collect();
    Some(TypedTable {
        head,
        table,
        if_not_exists,
        type_name,
        elements,
        tail: text(at, tokens.len()),
    })
}

/// SQL text of a composite attribute's type.
fn attribute_type_sql(attribute: &CompositeAttributeSchema) -> String {
    if let Some(user_type) = &attribute.user_type {
        let mut name = user_type.formatted_name();
        if user_type.array && !name.ends_with("[]") {
            name.push_str("[]");
        }
        return name;
    }
    let typmod = attribute
        .type_modifier
        .as_ref()
        .map(PgTypeModifier::catalog_value)
        .unwrap_or(-1);
    crate::pg_types::pg_format_type(pg_type_oid(&attribute.pg_type) as i32, typmod as i32)
        .unwrap_or_else(|| attribute.pg_type.clone())
}

const TABLE_CONSTRAINT_STARTS: &[&str] = &[
    "CONSTRAINT",
    "PRIMARY",
    "UNIQUE",
    "CHECK",
    "FOREIGN",
    "EXCLUDE",
    "NOT",
    "LIKE",
];

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_typed_table(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(typed) = parse(sql) else {
            return Ok(None);
        };
        let (schema_name, type_name) = match typed.type_name.as_slice() {
            [name] => ("public".to_string(), name.clone()),
            [.., schema, name] => (schema.clone(), name.clone()),
            [] => return Ok(None),
        };
        let shown = typed.type_name.join(".");
        let Some(user_type) = load_user_type(self.db_ref(), &schema_name, &type_name)? else {
            let table_name = resolve_session_relation_name_if_exists(
                self.db_ref(),
                &crate::eval::rewrite_helpers::relation_name_from_parts(&typed.type_name)?,
            );
            if load_schema(self.db_ref(), &table_name)?.is_some() {
                return Err(SqlError::with_detail(
                    SqlError::data_exception_public(
                        "42809",
                        format!("type {shown} is the row type of another table"),
                        None,
                    ),
                    "A typed table must use a stand-alone composite type created with CREATE TYPE.",
                ));
            }
            return Err(SqlError::undefined_object(format!(
                "type \"{shown}\" does not exist"
            )));
        };
        let UserTypeKind::Composite { attributes, .. } = &user_type.kind else {
            return Err(SqlError::data_exception_public(
                "42809",
                format!("type {shown} is not a composite type"),
                None,
            ));
        };
        self.ensure_user_type_usage(Some(&user_type.column_type(false)))?;
        let attributes = attributes
            .iter()
            .filter(|attribute| !attribute.dropped)
            .collect::<Vec<_>>();
        // Column options by name; anything else is a table constraint.
        let mut options: Vec<(String, String)> = Vec::new();
        let mut constraints = Vec::new();
        for (first, quoted, rest, whole) in &typed.elements {
            let is_constraint = !quoted
                && TABLE_CONSTRAINT_STARTS
                    .iter()
                    .any(|word| first.eq_ignore_ascii_case(word));
            if is_constraint {
                constraints.push(whole.clone());
                continue;
            }
            let column = first.clone();
            if !attributes.iter().any(|attribute| attribute.name == column) {
                return Err(SqlError::data_exception_public(
                    "42703",
                    format!("column \"{column}\" does not exist"),
                    None,
                ));
            }
            if options.iter().any(|(name, _)| *name == column) {
                return Err(SqlError::data_exception_public(
                    "42701",
                    format!("column \"{column}\" specified more than once"),
                    None,
                ));
            }
            options.push((column, rest.clone()));
        }
        let quote = super::not_null_constraints::quote;
        let mut definition = attributes
            .iter()
            .map(|attribute| {
                let extra = options
                    .iter()
                    .find(|(name, _)| *name == attribute.name)
                    .map(|(_, text)| format!(" {}", text.trim()))
                    .unwrap_or_default();
                format!(
                    "{} {}{extra}",
                    quote(&attribute.name),
                    attribute_type_sql(attribute)
                )
            })
            .collect::<Vec<_>>();
        definition.extend(constraints);
        let create = format!("{} ({}) {}", typed.head, definition.join(", "), typed.tail);
        let table = crate::eval::rewrite_helpers::relation_name_from_parts(&typed.table)?;
        let existed = load_schema(
            self.db_ref(),
            &resolve_session_relation_name_if_exists(self.db_ref(), &table),
        )?
        .is_some();
        let result = self.execute_inner(&create)?;
        if existed && typed.if_not_exists {
            return Ok(Some(result));
        }
        let resolved = resolve_session_relation_name(self.db_ref(), &table)?;
        let mut schema = load_schema(self.db_ref(), &resolved)?
            .ok_or_else(|| SqlError::InvalidCollection(resolved.clone()))?;
        schema.of_type = Some((user_type.schema_name.clone(), user_type.name.clone()));
        self.save_session_schema(&schema)?;
        Ok(Some(result))
    }

    /// Typed tables keep exactly their type's columns.
    pub(crate) fn reject_typed_table_change(
        schema: &TableSchema,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        if schema.of_type.is_none() {
            return Ok(());
        }
        for operation in operations {
            let message = match operation {
                AlterTableOperation::AddColumn { .. } => "cannot add column to typed table",
                AlterTableOperation::DropColumn { .. } => "cannot drop column from typed table",
                AlterTableOperation::RenameColumn { .. } => "cannot rename column of typed table",
                AlterTableOperation::AlterColumn {
                    op: AlterColumnOperation::SetDataType { .. },
                    ..
                } => "cannot alter column type of typed table",
                _ => continue,
            };
            return Err(SqlError::data_exception_public("42809", message, None));
        }
        Ok(())
    }

    /// The typed tables of a composite type.
    pub(crate) fn typed_tables_of(&self, user_type: &UserTypeSchema) -> Result<Vec<TableSchema>> {
        let identity = (user_type.schema_name.clone(), user_type.name.clone());
        Ok(list_schemas(self.db_ref())?
            .into_iter()
            .filter(|schema| schema.of_type.as_ref() == Some(&identity))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typed_table_definitions() {
        let typed = parse(
            "CREATE TABLE IF NOT EXISTS s.p OF person_type (id WITH OPTIONS PRIMARY KEY, UNIQUE (name)) PARTITION BY LIST (id)",
        )
        .unwrap();
        assert!(typed.if_not_exists);
        assert_eq!(typed.table, vec!["s", "p"]);
        assert_eq!(typed.type_name, vec!["person_type"]);
        assert_eq!(typed.elements.len(), 2);
        assert_eq!(typed.elements[0].2, "PRIMARY KEY");
        assert_eq!(typed.elements[1].3, "UNIQUE (name)");
        assert_eq!(typed.tail, "PARTITION BY LIST (id)");
        assert!(parse("CREATE TABLE t (a int)").is_none());
    }
}
