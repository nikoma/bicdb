//! PostgreSQL 18 not-null constraints written as table constraints:
//! `[CONSTRAINT name] NOT NULL column [NO INHERIT] [NOT VALID]` in
//! `CREATE TABLE` and `ALTER TABLE ... ADD`, plus `DROP`, `VALIDATE` and
//! `RENAME CONSTRAINT` of a not-null constraint.
//!
//! A not-null constraint is a column property here (`ColumnSchema::nullable`
//! plus `ColumnAttributes`), so these forms are applied to the column: the
//! SQL parser never sees them. A `NOT VALID` one is enforced on new rows like
//! any other (`nullable` is false) while `not_null_not_valid` tells every
//! optimization that existing rows may still hold NULL.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NotNullSpec {
    pub(crate) name: Option<String>,
    pub(crate) column: String,
    pub(crate) no_inherit: bool,
    pub(crate) not_valid: bool,
}

thread_local! {
    static CREATE_NO_INHERIT: RefCell<Vec<(String, Vec<String>)>> = const { RefCell::new(Vec::new()) };
}

/// Preserve NO INHERIT through the raw rewrite so inherited constraints
/// can be checked before CREATE publishes the relation.
pub(super) struct CreateNoInheritScope(bool);

impl CreateNoInheritScope {
    pub(super) fn enter(relation: &str, columns: Vec<String>) -> Self {
        if columns.is_empty() {
            return Self(false);
        }
        CREATE_NO_INHERIT.with(|active| {
            active.borrow_mut().push((relation.to_string(), columns));
        });
        Self(true)
    }
}

impl Drop for CreateNoInheritScope {
    fn drop(&mut self) {
        if self.0 {
            CREATE_NO_INHERIT.with(|active| active.borrow_mut().pop());
        }
    }
}

pub(super) fn validate_created_no_inherit(
    name: &ObjectName,
    parents: &[TableSchema],
) -> Result<()> {
    CREATE_NO_INHERIT.with(|active| {
        let active = active.borrow();
        if active.is_empty() {
            return Ok(());
        }
        let relation = relation_name(name)?;
        for column in active
            .iter()
            .filter(|(table, _)| *table == relation)
            .flat_map(|(_, columns)| columns)
        {
            if parents
                .iter()
                .any(|parent| inheritable_not_null(parent, column))
            {
                return Err(no_inherit_conflicts_with_parent(column));
            }
        }
        Ok(())
    })
}

fn no_inherit_conflicts_with_parent(column: &str) -> SqlError {
    SqlError::coded(
        "42804",
        format!("cannot define not-null constraint with NO INHERIT on column \"{column}\""),
        Some("The column has an inherited not-null constraint.".into()),
        None,
    )
}

#[derive(Debug, PartialEq)]
pub(crate) enum NotNullDdl {
    /// `CREATE TABLE` with the not-null table constraints removed.
    Create {
        sql: String,
        table: String,
        specs: Vec<NotNullSpec>,
    },
    /// `ALTER TABLE ... ADD <spec> [, ADD <spec> ...]`.
    Add {
        table: String,
        if_exists: bool,
        only: bool,
        specs: Vec<NotNullSpec>,
    },
    /// A single `DROP CONSTRAINT`, `VALIDATE CONSTRAINT` or
    /// `RENAME CONSTRAINT`; handled here only when it names a not-null
    /// constraint.
    Named {
        table: String,
        if_exists: bool,
        only: bool,
        action: NamedAction,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum NamedAction {
    Drop { name: String, if_exists: bool },
    Validate { name: String },
    Rename { from: String, to: String },
}

fn keyword(token: Option<&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn ident(token: Option<&Token>) -> Option<String> {
    match token? {
        Token::Word(word) if word.quote_style == Some('"') => Some(word.value.clone()),
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_lowercase()),
        _ => None,
    }
}

/// `"name"` with embedded quotes doubled.
pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Reads `[CONSTRAINT name] NOT NULL column [NO INHERIT | NOT VALID]...`
/// from exactly the tokens `element`.
fn parse_spec(element: &[&Token]) -> Option<NotNullSpec> {
    let mut at = 0;
    let mut name = None;
    if keyword(element.first().copied(), "CONSTRAINT") {
        name = Some(ident(element.get(1).copied())?);
        at = 2;
    }
    if !(keyword(element.get(at).copied(), "NOT") && keyword(element.get(at + 1).copied(), "NULL"))
    {
        return None;
    }
    let column = ident(element.get(at + 2).copied())?;
    at += 3;
    let mut spec = NotNullSpec {
        name,
        column,
        no_inherit: false,
        not_valid: false,
    };
    while at < element.len() {
        let next = element.get(at + 1).copied();
        if keyword(element.get(at).copied(), "NO") && keyword(next, "INHERIT") {
            spec.no_inherit = true;
        } else if keyword(element.get(at).copied(), "NOT") && keyword(next, "VALID") {
            spec.not_valid = true;
        } else {
            return None;
        }
        at += 2;
    }
    Some(spec)
}

/// A relation name (possibly qualified) at `at`; returns its SQL text as
/// written and the next position.
fn relation_at(tokens: &[&Token], at: usize) -> Option<(String, usize)> {
    let mut text = String::new();
    let mut next = at;
    loop {
        let Some(Token::Word(_)) = tokens.get(next) else {
            return None;
        };
        text.push_str(&crate::eval::rewrite_helpers::token_sql(tokens[next]));
        next += 1;
        if tokens.get(next) == Some(&&Token::Period) {
            text.push('.');
            next += 1;
        } else {
            return Some((text, next));
        }
    }
}

pub(crate) fn parse_not_null_ddl(sql: &str) -> Result<Option<NotNullDdl>> {
    let head = sql.trim_start();
    let starts =
        |word: &str| head.len() >= word.len() && head[..word.len()].eq_ignore_ascii_case(word);
    if !(starts("CREATE") || starts("ALTER")) {
        return Ok(None);
    }
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("null") || lower.contains("constraint")) {
        return Ok(None);
    }
    let Ok(tokens) = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql).tokenize()
    else {
        return Ok(None);
    };
    let mut significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .collect();
    while matches!(significant.last(), Some((_, Token::SemiColon))) {
        significant.pop();
    }
    if significant
        .iter()
        .any(|(_, token)| matches!(token, Token::SemiColon))
    {
        return Ok(None);
    }
    let sig: Vec<&Token> = significant.iter().map(|(_, token)| *token).collect();
    if keyword(sig.first().copied(), "CREATE") {
        return Ok(parse_create(&tokens, &significant, &sig));
    }
    if !(keyword(sig.first().copied(), "ALTER") && keyword(sig.get(1).copied(), "TABLE")) {
        return Ok(None);
    }
    let mut at = 2;
    let mut if_exists = false;
    if keyword(sig.get(at).copied(), "IF") && keyword(sig.get(at + 1).copied(), "EXISTS") {
        if_exists = true;
        at += 2;
    }
    let mut only = false;
    if keyword(sig.get(at).copied(), "ONLY") {
        only = true;
        at += 1;
    }
    let Some((table, mut at)) = relation_at(&sig, at) else {
        return Ok(None);
    };
    // Single named-constraint actions.
    let rest = &sig[at..];
    let named = |action| {
        Ok(Some(NotNullDdl::Named {
            table: table.clone(),
            if_exists,
            only,
            action,
        }))
    };
    if keyword(rest.first().copied(), "DROP") && keyword(rest.get(1).copied(), "CONSTRAINT") {
        let mut position = 2;
        let mut drop_if_exists = false;
        if keyword(rest.get(2).copied(), "IF") && keyword(rest.get(3).copied(), "EXISTS") {
            drop_if_exists = true;
            position = 4;
        }
        let Some(name) = ident(rest.get(position).copied()) else {
            return Ok(None);
        };
        let tail = &rest[position + 1..];
        if tail.len() > 1
            || (tail.len() == 1
                && !keyword(tail.first().copied(), "CASCADE")
                && !keyword(tail.first().copied(), "RESTRICT"))
        {
            return Ok(None);
        }
        return named(NamedAction::Drop {
            name,
            if_exists: drop_if_exists,
        });
    }
    if keyword(rest.first().copied(), "VALIDATE")
        && keyword(rest.get(1).copied(), "CONSTRAINT")
        && rest.len() == 3
    {
        let Some(name) = ident(rest.get(2).copied()) else {
            return Ok(None);
        };
        return named(NamedAction::Validate { name });
    }
    if keyword(rest.first().copied(), "RENAME")
        && keyword(rest.get(1).copied(), "CONSTRAINT")
        && keyword(rest.get(3).copied(), "TO")
        && rest.len() == 5
    {
        let (Some(from), Some(to)) = (ident(rest.get(2).copied()), ident(rest.get(4).copied()))
        else {
            return Ok(None);
        };
        return named(NamedAction::Rename { from, to });
    }
    // One or more `ADD <spec>` actions.
    let mut specs = Vec::new();
    let mut saw_other = false;
    loop {
        let end = (at..sig.len())
            .find(|&index| sig[index] == &Token::Comma && paren_depth(&sig[at..index]) == 0)
            .unwrap_or(sig.len());
        let action = &sig[at..end];
        match keyword(action.first().copied(), "ADD")
            .then(|| parse_spec(&action[1..]))
            .flatten()
        {
            Some(spec) => specs.push(spec),
            None => saw_other = true,
        }
        if end == sig.len() {
            break;
        }
        at = end + 1;
    }
    if specs.is_empty() {
        return Ok(None);
    }
    if saw_other {
        return Err(SqlError::Unsupported(
            "ALTER TABLE combining ADD NOT NULL constraints with other actions".to_string(),
        ));
    }
    Ok(Some(NotNullDdl::Add {
        table,
        if_exists,
        only,
        specs,
    }))
}

fn paren_depth(tokens: &[&Token]) -> i64 {
    tokens.iter().fold(0, |depth, token| match token {
        Token::LParen => depth + 1,
        Token::RParen => depth - 1,
        _ => depth,
    })
}

fn parse_create(
    tokens: &[Token],
    significant: &[(usize, &Token)],
    sig: &[&Token],
) -> Option<NotNullDdl> {
    let mut at = 1;
    if keyword(sig.get(at).copied(), "GLOBAL") || keyword(sig.get(at).copied(), "LOCAL") {
        at += 1;
    }
    if ["TEMP", "TEMPORARY", "UNLOGGED"]
        .iter()
        .any(|word| keyword(sig.get(at).copied(), word))
    {
        at += 1;
    }
    if !keyword(sig.get(at).copied(), "TABLE") {
        return None;
    }
    at += 1;
    if keyword(sig.get(at).copied(), "IF") {
        at += 3;
    }
    let (table, after_name) = relation_at(sig, at)?;
    if sig.get(after_name) != Some(&&Token::LParen) {
        return None;
    }
    // Elements of the definition list, as significant-index ranges.
    let mut elements = Vec::new();
    let mut depth = 0;
    let mut start = after_name + 1;
    let mut close = None;
    for (position, token) in sig.iter().enumerate().skip(after_name) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    elements.push((start, position));
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
    close?;
    let mut specs = Vec::new();
    let mut removed = Vec::new();
    for (index, &(start, end)) in elements.iter().enumerate() {
        let Some(spec) = parse_spec(&sig[start..end]) else {
            continue;
        };
        specs.push(spec);
        // Drop the element and the comma that separates it from a kept one.
        removed.push(if index > 0 {
            (start - 1, end)
        } else if elements.len() > 1 {
            (start, end + 1)
        } else {
            (start, end)
        });
    }
    if specs.is_empty() {
        return None;
    }
    // Adjacent removals may overlap on a comma; keep whole tokens.
    let removed_token = |token_index: usize| {
        removed.iter().any(|&(from, to)| {
            let first = significant[from].0;
            let last = significant[to - 1].0;
            token_index >= first && token_index <= last
        })
    };
    let sql = tokens
        .iter()
        .enumerate()
        .filter(|(index, _)| !removed_token(*index))
        .map(|(_, token)| crate::eval::rewrite_helpers::token_sql(token))
        .collect::<String>();
    Some(NotNullDdl::Create { sql, table, specs })
}

/// The constraint names of `schema` other than its not-null ones.
fn other_constraint_names(schema: &TableSchema) -> Vec<String> {
    let mut names = schema
        .constraints
        .iter()
        .map(|constraint| constraint_name(constraint).to_string())
        .collect::<Vec<_>>();
    if !primary_key_columns_for_schema(schema).is_empty() {
        names.push(schema.primary_key_constraint_name());
    }
    names.extend(
        schema
            .temporal_foreign_keys
            .iter()
            .map(|foreign_key| foreign_key.name.clone()),
    );
    names
}

/// A declared not-null name may not repeat another constraint's name.
pub(crate) fn validate_declared_not_null_names(schema: &TableSchema) -> Result<()> {
    let others = other_constraint_names(schema);
    let mut seen = BTreeSet::new();
    for column in &schema.columns {
        if column.hidden || (column.nullable && !column.primary_key) {
            continue;
        }
        let Some(name) = &column.attributes.not_null_name else {
            continue;
        };
        if others.contains(name) || !seen.insert(name.clone()) {
            return Err(SqlError::duplicate_constraint(
                &relation_name_for_message(&schema.name),
                name,
            ));
        }
    }
    Ok(())
}

fn no_inherit_key(schema: &TableSchema, column: &str) -> String {
    not_null_constraint_name(&schema.name, column)
}

/// The column names in `column` order of the first spec per column, merged
/// with later ones as PostgreSQL merges repeated declarations.
fn merge_specs(specs: Vec<NotNullSpec>) -> Result<Vec<NotNullSpec>> {
    let mut merged: Vec<NotNullSpec> = Vec::new();
    for spec in specs {
        let Some(existing) = merged.iter_mut().find(|other| other.column == spec.column) else {
            merged.push(spec);
            continue;
        };
        if existing.no_inherit != spec.no_inherit {
            return Err(SqlError::data_exception_public(
                "42601",
                format!(
                    "conflicting NO INHERIT declaration for not-null constraint on column \"{}\"",
                    spec.column
                ),
                None,
            ));
        }
        match (&existing.name, &spec.name) {
            (Some(first), Some(second)) if first != second => {
                return Err(conflicting_names(first, second));
            }
            (None, Some(second)) => existing.name = Some(second.clone()),
            _ => {}
        }
    }
    Ok(merged)
}

fn conflicting_names(first: &str, second: &str) -> SqlError {
    SqlError::data_exception_public(
        "42601",
        format!("conflicting not-null constraint names \"{first}\" and \"{second}\""),
        None,
    )
}

fn undefined_column(column: &str, table: &str) -> SqlError {
    SqlError::data_exception_public(
        "42703",
        format!(
            "column \"{column}\" of relation \"{}\" does not exist",
            relation_name_for_message(table)
        ),
        None,
    )
}

fn column_index(schema: &TableSchema, column: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|candidate| !candidate.hidden && candidate.name == column)
}

/// The quoted, schema-qualified name of a table for generated SQL.
fn quoted_table(schema: &TableSchema) -> String {
    let bare = schema.name.rsplit('.').next().unwrap_or(&schema.name);
    if schema.schema_name == "public" {
        quote(bare)
    } else {
        format!("{}.{}", quote(&schema.schema_name), quote(bare))
    }
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_not_null_constraints(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(ddl) = parse_not_null_ddl(sql)? else {
            return Ok(None);
        };
        match ddl {
            NotNullDdl::Create { sql, table, specs } => {
                let relation = relation_name_from_parts(&sql_identifier_parts(&table)?)?;
                let existed = load_schema(
                    self.db_ref(),
                    &resolve_session_relation_name_if_exists(self.db_ref(), &relation),
                )?
                .is_some();
                let no_inherit = specs
                    .iter()
                    .filter(|spec| spec.no_inherit)
                    .map(|spec| spec.column.clone())
                    .collect();
                let result = {
                    let _scope = CreateNoInheritScope::enter(&relation, no_inherit);
                    self.execute_inner(&sql)?
                };
                if existed {
                    return Ok(Some(result));
                }
                if let Err(error) = self.apply_created_not_null(&relation, specs) {
                    if self.tx.is_none() {
                        let _ = self.execute_inner(&format!("DROP TABLE {table}"));
                    }
                    return Err(error);
                }
                Ok(Some(result))
            }
            NotNullDdl::Add {
                table,
                if_exists,
                only,
                specs,
            } => self.add_not_null_constraints(&table, if_exists, only, specs),
            NotNullDdl::Named {
                table,
                if_exists,
                only,
                action,
            } => self.named_not_null_action(&table, if_exists, only, action),
        }
    }

    fn apply_created_not_null(&mut self, table: &str, specs: Vec<NotNullSpec>) -> Result<()> {
        let resolved = resolve_session_relation_name(self.db_ref(), table)?;
        let mut schema = load_schema(self.db_ref(), &resolved)?
            .ok_or_else(|| SqlError::InvalidCollection(resolved.clone()))?;
        let schemas = list_schemas(self.db_ref())?;
        for spec in merge_specs(specs)? {
            let index = column_index(&schema, &spec.column)
                .ok_or_else(|| undefined_column(&spec.column, &resolved))?;
            let column = &schema.columns[index];
            let key = no_inherit_key(&schema, &spec.column);
            if !column.nullable || column.primary_key {
                if let (Some(first), Some(second)) = (&column.attributes.not_null_name, &spec.name)
                {
                    if first != second {
                        return Err(conflicting_names(first, second));
                    }
                }
                if schema.no_inherit_constraints.contains(&key) != spec.no_inherit {
                    return Err(SqlError::data_exception_public(
                        "42601",
                        format!(
                            "conflicting NO INHERIT declaration for not-null constraint on column \"{}\"",
                            spec.column
                        ),
                        None,
                    ));
                }
            }
            if spec.no_inherit {
                if schema.partitioning.is_some() {
                    return Err(SqlError::data_exception_public(
                        "42P16",
                        "not-null constraints on partitioned tables cannot be NO INHERIT",
                        None,
                    ));
                }
                if !inherited_not_null_parents(&schemas, &schema, &spec.column).is_empty() {
                    return Err(no_inherit_conflicts_with_parent(&spec.column));
                }
            }
            let column = &mut schema.columns[index];
            column.nullable = false;
            if spec.name.is_some() {
                column.attributes.not_null_name = spec.name;
            }
            if spec.no_inherit && !schema.no_inherit_constraints.contains(&key) {
                schema.no_inherit_constraints.push(key);
            }
        }
        validate_declared_not_null_names(&schema)?;
        self.save_session_schema(&schema)
    }

    fn missing_relation(&self, table: &str, if_exists: bool) -> Result<Option<SqlResult>> {
        if if_exists {
            crate::statement_context::notice_skipping_missing(
                "relation",
                &relation_name_for_message(table),
            );
            return Ok(Some(SqlResult::command("ALTER TABLE")));
        }
        Err(SqlError::data_exception_public(
            "42P01",
            format!(
                "relation \"{}\" does not exist",
                relation_name_for_message(table)
            ),
            None,
        ))
    }

    fn add_not_null_constraints(
        &mut self,
        table: &str,
        if_exists: bool,
        only: bool,
        specs: Vec<NotNullSpec>,
    ) -> Result<Option<SqlResult>> {
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), table);
        let Some(schema) = load_schema(self.db_ref(), &resolved)? else {
            return self.missing_relation(table, if_exists);
        };
        self.require_table_ownership(&schema.name, "ALTER TABLE")?;
        let display = relation_name_for_message(&schema.name);
        let schemas = list_schemas(self.db_ref())?;
        // Check every action before changing anything.
        let mut work = Vec::new();
        for spec in specs {
            let index = column_index(&schema, &spec.column)
                .ok_or_else(|| undefined_column(&spec.column, &schema.name))?;
            let column = &schema.columns[index];
            if !column.nullable || column.primary_key {
                let existing = effective_not_null_name(&schemas, &schema, &spec.column);
                let existing_no_inherit = schema
                    .no_inherit_constraints
                    .contains(&no_inherit_key(&schema, &spec.column));
                if existing_no_inherit != spec.no_inherit {
                    return Err(SqlError::with_detail_fields(
                        SqlError::object_not_in_prerequisite_state(format!(
                            "cannot change NO INHERIT status of NOT NULL constraint \"{existing}\" on relation \"{display}\""
                        )),
                        None,
                        Some("You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.".to_string()),
                        None,
                    ));
                }
                if column.attributes.not_null_not_valid && !spec.not_valid {
                    return Err(incompatible_not_valid(&existing, &display));
                }
                if let Some(name) = spec.name.as_ref().filter(|name| **name != existing) {
                    return Err(SqlError::with_detail(
                        SqlError::object_not_in_prerequisite_state(format!(
                            "cannot create not-null constraint \"{name}\" on column \"{}\" of table \"{display}\"",
                            spec.column
                        )),
                        format!(
                            "A not-null constraint named \"{existing}\" already exists for this column."
                        ),
                    ));
                }
                continue;
            }
            if spec.no_inherit && schema.partitioning.is_some() {
                return Err(SqlError::data_exception_public(
                    "42P16",
                    "not-null constraints on partitioned tables cannot be NO INHERIT",
                    None,
                ));
            }
            if let Some(name) = &spec.name {
                let taken = other_constraint_names(&schema).contains(name)
                    || schema.columns.iter().any(|other| {
                        other.name != spec.column
                            && (!other.nullable || other.primary_key)
                            && effective_not_null_name(&schemas, &schema, &other.name) == *name
                    });
                if taken {
                    return Err(SqlError::duplicate_constraint(&display, name));
                }
            }
            if !spec.not_valid {
                // Every row is checked, whatever row security would show.
                let recurse = !only && !spec.no_inherit;
                for target in self.not_null_targets(&schema, recurse)? {
                    let has_null = self
                        .ddl_rows()?
                        .visible(&target.name)?
                        .iter()
                        .any(|record| {
                            matches!(
                                record_column_value(record, &target, &spec.column),
                                SqlValue::Null
                            )
                        });
                    if has_null {
                        return Err(SqlError::data_exception_public(
                            "23502",
                            format!(
                                "column \"{}\" of relation \"{}\" contains null values",
                                spec.column,
                                relation_name_for_message(&target.name)
                            ),
                            None,
                        ));
                    }
                }
            }
            work.push(spec);
        }
        for spec in work {
            let recurse = !only && !spec.no_inherit;
            if spec.not_valid {
                let current = load_schema(self.db_ref(), &schema.name)?
                    .ok_or_else(|| SqlError::InvalidCollection(schema.name.clone()))?;
                let targets = self.not_null_targets(&current, recurse)?;
                for mut target in targets {
                    if let Some(column) = target.columns.iter_mut().find(|column| {
                        column.name == spec.column && column.nullable && !column.primary_key
                    }) {
                        column.nullable = false;
                        column.attributes.not_null_not_valid = true;
                        self.save_session_schema(&target)?;
                    }
                }
            } else {
                let scope = if recurse { "" } else { "ONLY " };
                self.execute_inner(&format!(
                    "ALTER TABLE {scope}{} ALTER COLUMN {} SET NOT NULL",
                    quoted_table(&schema),
                    quote(&spec.column)
                ))?;
            }
            let mut updated = load_schema(self.db_ref(), &schema.name)?
                .ok_or_else(|| SqlError::InvalidCollection(schema.name.clone()))?;
            let key = no_inherit_key(&updated, &spec.column);
            if let Some(index) = column_index(&updated, &spec.column) {
                if spec.name.is_some() {
                    updated.columns[index].attributes.not_null_name = spec.name.clone();
                }
            }
            if spec.no_inherit && !updated.no_inherit_constraints.contains(&key) {
                updated.no_inherit_constraints.push(key);
            }
            self.save_session_schema(&updated)?;
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }

    /// `schema` and, when the change recurses, its inheritance descendants
    /// and partitions.
    fn not_null_targets(&self, schema: &TableSchema, recurse: bool) -> Result<Vec<TableSchema>> {
        let mut targets = vec![schema.clone()];
        if recurse {
            targets.extend(crate::inheritance::inheritance_descendants(
                self.db_ref(),
                schema,
            )?);
            if schema.partitioning.is_some() {
                targets.extend(
                    partition::physical_tables(self.db_ref(), schema)?
                        .into_iter()
                        .skip(1),
                );
            }
        }
        Ok(targets)
    }

    fn named_not_null_action(
        &mut self,
        table: &str,
        if_exists: bool,
        only: bool,
        action: NamedAction,
    ) -> Result<Option<SqlResult>> {
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), table);
        let Some(mut schema) = load_schema(self.db_ref(), &resolved)? else {
            return Ok(None);
        };
        let name = match &action {
            NamedAction::Drop { name, .. } | NamedAction::Validate { name } => name,
            NamedAction::Rename { from, .. } => from,
        };
        let schemas = list_schemas(self.db_ref())?;
        let Some(index) = schema.columns.iter().position(|column| {
            !column.hidden
                && (!column.nullable || column.primary_key)
                && effective_not_null_name(&schemas, &schema, &column.name) == *name
        }) else {
            return Ok(None);
        };
        let _ = if_exists;
        self.require_table_ownership(&schema.name, "ALTER TABLE")?;
        let display = relation_name_for_message(&schema.name);
        let column = schema.columns[index].name.clone();
        let scope = if only { "ONLY " } else { "" };
        match action {
            NamedAction::Drop { name, .. } => {
                let inherited = !inherited_not_null_parents(&schemas, &schema, &column).is_empty();
                if inherited && schema.columns[index].attributes.not_null_name.is_none() {
                    return Err(SqlError::data_exception_public(
                        "42P16",
                        format!(
                            "cannot drop inherited constraint \"{name}\" of relation \"{display}\""
                        ),
                        None,
                    ));
                }
                if schema.columns[index].primary_key {
                    return Err(SqlError::data_exception_public(
                        "42P16",
                        format!("column \"{column}\" is in a primary key"),
                        None,
                    ));
                }
                self.execute_inner(&format!(
                    "ALTER TABLE {scope}{} ALTER COLUMN {} DROP NOT NULL",
                    quoted_table(&schema),
                    quote(&column)
                ))?;
            }
            NamedAction::Validate { .. } => {
                if schema.columns[index].attributes.not_null_not_valid {
                    self.execute_inner(&format!(
                        "ALTER TABLE {scope}{} ALTER COLUMN {} SET NOT NULL",
                        quoted_table(&schema),
                        quote(&column)
                    ))?;
                }
            }
            NamedAction::Rename { from, to } => {
                if from != to {
                    let taken = other_constraint_names(&schema).contains(&to)
                        || schema.columns.iter().any(|other| {
                            other.name != column
                                && (!other.nullable || other.primary_key)
                                && effective_not_null_name(&schemas, &schema, &other.name) == to
                        });
                    if taken {
                        return Err(SqlError::duplicate_constraint(&display, &to));
                    }
                    schema.columns[index].attributes.not_null_name = Some(to);
                    self.save_session_schema(&schema)?;
                }
            }
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }
}

fn incompatible_not_valid(name: &str, table: &str) -> SqlError {
    SqlError::with_detail_fields(
        SqlError::object_not_in_prerequisite_state(format!(
            "incompatible NOT VALID constraint \"{name}\" on relation \"{table}\""
        )),
        None,
        Some(
            "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.".to_string(),
        ),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_level_not_null_elements() {
        let Some(NotNullDdl::Create { sql, table, specs }) = parse_not_null_ddl(
            "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a NO INHERIT, b int, NOT NULL b)",
        )
        .unwrap() else {
            panic!("not parsed");
        };
        assert_eq!(table, "t");
        assert_eq!(sql, "CREATE TABLE t (a int, b int)");
        assert_eq!(
            specs,
            vec![
                NotNullSpec {
                    name: Some("nn".into()),
                    column: "a".into(),
                    no_inherit: true,
                    not_valid: false,
                },
                NotNullSpec {
                    name: None,
                    column: "b".into(),
                    no_inherit: false,
                    not_valid: false,
                },
            ]
        );
        let Some(NotNullDdl::Create { sql, .. }) =
            parse_not_null_ddl("create table t (constraint foo not null a, a int)").unwrap()
        else {
            panic!("not parsed");
        };
        assert_eq!(sql, "create table t ( a int)");
        // A column-level constraint is left to the parser.
        assert_eq!(
            parse_not_null_ddl("CREATE TABLE t (a int CONSTRAINT x NOT NULL)").unwrap(),
            None
        );
    }

    #[test]
    fn parses_alter_table_forms() {
        assert_eq!(
            parse_not_null_ddl(
                "ALTER TABLE ONLY s.t ADD NOT NULL a NOT VALID, ADD CONSTRAINT x NOT NULL \"B\""
            )
            .unwrap(),
            Some(NotNullDdl::Add {
                table: "s.t".into(),
                if_exists: false,
                only: true,
                specs: vec![
                    NotNullSpec {
                        name: None,
                        column: "a".into(),
                        no_inherit: false,
                        not_valid: true,
                    },
                    NotNullSpec {
                        name: Some("x".into()),
                        column: "B".into(),
                        no_inherit: false,
                        not_valid: false,
                    },
                ],
            })
        );
        assert!(parse_not_null_ddl("ALTER TABLE t ADD NOT NULL a, ADD COLUMN b int").is_err());
        assert_eq!(
            parse_not_null_ddl("ALTER TABLE t ADD CONSTRAINT c CHECK (a IS NOT NULL)").unwrap(),
            None
        );
        assert_eq!(
            parse_not_null_ddl("ALTER TABLE t VALIDATE CONSTRAINT nn").unwrap(),
            Some(NotNullDdl::Named {
                table: "t".into(),
                if_exists: false,
                only: false,
                action: NamedAction::Validate { name: "nn".into() },
            })
        );
    }
}
