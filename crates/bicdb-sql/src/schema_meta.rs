//! Schema/catalog metadata helpers: foreign key cascade constraint discovery, migration SQL splitting, extension bookkeeping, schema/namespace/extension listing, role membership and ACL values, session relation resolution, and view persistence.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use schema_meta::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn inbound_foreign_key_update_constraints_uncached(
    db: &BicDb,
    table: &str,
) -> Result<Vec<InboundForeignKeyUpdate>> {
    let mut constraints = Vec::new();
    for child_schema in list_schemas(db)? {
        for constraint in &child_schema.constraints {
            let ConstraintSchema::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_update,
                ..
            } = constraint
            else {
                continue;
            };
            if foreign_table.eq_ignore_ascii_case(table) {
                constraints.push(InboundForeignKeyUpdate {
                    child_schema: child_schema.clone(),
                    name: name.clone(),
                    columns: columns.clone(),
                    referred_columns: referred_columns.clone(),
                    on_update: *on_update,
                });
            }
        }
    }
    Ok(constraints)
}

pub(crate) fn inbound_foreign_key_delete_constraints_uncached(
    db: &BicDb,
    table: &str,
) -> Result<Vec<InboundForeignKeyDelete>> {
    sql_profile_foreign_key_parent_delete_schema_scan();
    let mut constraints = Vec::new();
    for child_schema in list_schemas(db)? {
        for constraint in &child_schema.constraints {
            let ConstraintSchema::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                ..
            } = constraint
            else {
                continue;
            };
            if foreign_table.eq_ignore_ascii_case(table) {
                constraints.push(InboundForeignKeyDelete {
                    child_schema: child_schema.clone(),
                    name: name.clone(),
                    columns: columns.clone(),
                    referred_columns: referred_columns.clone(),
                    on_delete: *on_delete,
                });
            }
        }
    }
    Ok(constraints)
}

pub(crate) fn changed_parent_update_keys(
    schema: &TableSchema,
    referred_columns: &[String],
    before_after_records: &[(Record, Record)],
) -> Result<Vec<(Vec<String>, Vec<SqlValue>)>> {
    let mut changes = Vec::new();
    let mut seen = BTreeSet::new();
    for (before, after) in before_after_records {
        let old_key = record_column_values(before, schema, referred_columns);
        let new_key = record_column_values(after, schema, referred_columns);
        if typed_column_values_not_distinct(schema, referred_columns, &old_key, &new_key)? {
            continue;
        }
        let old_label = typed_column_key_label(schema, referred_columns, &old_key)?;
        if seen.insert(old_label.clone()) {
            changes.push((old_label, new_key));
        }
    }
    Ok(changes)
}

pub(crate) fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    #[cfg(test)]
    SQL_PARSE_STATEMENT_CALLS.with(|calls| *calls.borrow_mut() += 1);

    reject_oversized_sql(sql)?;
    let dialect = PostgreSqlDialect {};
    let parse_sql = rewrite_postgres_parse_compat(sql);
    let sql_for_parse = parse_sql.as_deref().unwrap_or(sql);
    reject_deep_parse_nesting(sql_for_parse)?;
    let mut statements = Parser::parse_sql(&dialect, sql_for_parse)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    // Refuse a pathologically deep expression before ANY recursive pass touches
    // it — the precedence visitor below, the evaluator, and even Drop all
    // overflow the stack (aborting the process) on a hostile AST.
    reject_deep_expressions(&statements)?;
    restore_postgres_distinct_boolean_precedence(&mut statements);
    Ok(statements)
}

/// sqlparser 0.62 parses the right operand of `IS [NOT] DISTINCT FROM` with
/// `parse_expr()`, so an unparenthesized following `AND` or `OR` is absorbed
/// into that operand. PostgreSQL binds `IS [NOT] DISTINCT FROM` more tightly:
/// `a IS DISTINCT FROM b OR c` means `(a IS DISTINCT FROM b) OR c`.
///
/// Repair the parser AST centrally so every BicDB execution and describe path
/// sees PostgreSQL's operator precedence. Explicitly parenthesized boolean
/// operands remain `Expr::Nested` and are intentionally left unchanged.
pub(crate) fn restore_postgres_distinct_boolean_precedence(statements: &mut Vec<Statement>) {
    let _ = visit_expressions_mut(statements, |expr| {
        let repaired = match expr {
            Expr::IsDistinctFrom(left, right) => Some(reassociate_distinct_boolean_rhs(
                left.as_ref().clone(),
                right.as_ref().clone(),
                false,
            )),
            Expr::IsNotDistinctFrom(left, right) => Some(reassociate_distinct_boolean_rhs(
                left.as_ref().clone(),
                right.as_ref().clone(),
                true,
            )),
            _ => None,
        };
        if let Some(repaired) = repaired {
            *expr = repaired;
        }
        ControlFlow::<()>::Continue(())
    });
}

fn reassociate_distinct_boolean_rhs(left: Expr, right: Expr, negated: bool) -> Expr {
    match right {
        Expr::BinaryOp {
            left: boolean_left,
            op,
            right: boolean_right,
        } if matches!(op, BinaryOperator::And | BinaryOperator::Or) => Expr::BinaryOp {
            left: Box::new(reassociate_distinct_boolean_rhs(
                left,
                *boolean_left,
                negated,
            )),
            op,
            right: boolean_right,
        },
        right if negated => Expr::IsNotDistinctFrom(Box::new(left), Box::new(right)),
        right => Expr::IsDistinctFrom(Box::new(left), Box::new(right)),
    }
}

/// Cheap check for "no executable statements": true iff the text contains only
/// whitespace, bare `;`, and SQL comments. Equivalent to
/// `split_sql_statements(sql).is_empty()` without tokenizing the whole string
/// into an allocated `Vec` — the per-statement hot path calls this once per
/// execute just to reject empty input, so it must exit at the first real byte.
pub fn sql_has_no_statements(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b if b.is_ascii_whitespace() => idx += 1,
            b';' => idx += 1,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                idx += 2;
                loop {
                    if idx >= bytes.len() {
                        // Unterminated block comment: split_sql_statements
                        // swallows the rest of the input too.
                        return true;
                    }
                    if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                        idx += 2;
                        break;
                    }
                    idx += 1;
                }
            }
            _ => return false,
        }
    }
    true
}

/// The first keyword of a statement, after leading whitespace, semicolons and
/// comments, or `None` when the text does not start with an identifier.
pub fn leading_sql_keyword(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b if b.is_ascii_whitespace() => idx += 1,
            b';' => idx += 1,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                idx += 2;
                loop {
                    if idx >= bytes.len() {
                        return None;
                    }
                    if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                        idx += 2;
                        break;
                    }
                    idx += 1;
                }
            }
            _ => break,
        }
    }
    let start = idx;
    while idx < bytes.len() && (bytes[idx].is_ascii_alphabetic() || bytes[idx] == b'_') {
        idx += 1;
    }
    (idx > start).then(|| &sql[start..idx])
}

/// Statements that none of the raw-text handlers in `execute_inner` can
/// match: they go straight to the parser. Every raw handler keys on a DDL /
/// admin verb (CREATE, ALTER, DROP, COMMENT, ANALYZE, VACUUM, RESET, GRANT,
/// REVOKE, DO, MERGE/ROLLUP/EXPLAIN MATERIALIZED, PACK, PROCESS, TRIM) or
/// on a SELECT shape, so a statement that starts with one of these verbs
/// cannot be theirs. TPC-C's whole mix arrives as `CALL`, and every one of
/// those used to pay ~40 text probes plus the builtin matcher before parsing.
pub fn leading_keyword_bypasses_raw_probes(sql: &str) -> bool {
    leading_sql_keyword(sql).is_some_and(|keyword| {
        ["call", "insert", "update", "delete"]
            .iter()
            .any(|verb| keyword.eq_ignore_ascii_case(verb))
    })
}

pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut idx = 0;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;

    while idx < bytes.len() {
        if line_comment {
            if bytes[idx] == b'\n' {
                line_comment = false;
            }
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if bytes[idx..].starts_with(delimiter.as_bytes()) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&sql[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    idx += 1;
                }
            }
            b';' => {
                let statement = sql[start..idx].trim();
                if has_executable_sql(statement) {
                    statements.push(statement.to_string());
                }
                start = idx + 1;
                idx += 1;
            }
            _ => idx += 1,
        }
    }

    let statement = sql[start..].trim();
    if has_executable_sql(statement) {
        statements.push(statement.to_string());
    }
    statements
}

pub(crate) fn has_executable_sql(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            byte if byte.is_ascii_whitespace() => idx += 1,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                idx += 2;
                while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                    idx += 1;
                }
                idx = (idx + 2).min(bytes.len());
            }
            _ => return true,
        }
    }
    false
}

pub(crate) fn cast_routine_return_value(
    value: SqlValue,
    routine: &RoutineSchema,
) -> Result<SqlValue> {
    let return_type = routine.return_type.trim();
    if matches!(return_type, "" | "void" | "record" | "trigger") {
        return Ok(value);
    }
    let pg_type = pg_type_regtype_name(return_type)
        .unwrap_or_else(|| collapse_sql_whitespace(return_type).to_ascii_lowercase());
    cast_value_to_pg_type(value, &pg_type)
}

pub(crate) fn dollar_quote_delimiter(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    if bytes.first() != Some(&b'$') {
        return None;
    }
    let end = bytes[1..].iter().position(|byte| *byte == b'$')? + 1;
    if bytes[1..end]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        Some(sql[..=end].to_string())
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatementSafety {
    TransactionalDdl,
    OnlineSafeDdl,
    RowRewrite,
    NonDdl,
    Unsupported,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationStatementPlan {
    pub ordinal: usize,
    pub sql: String,
    pub safety: MigrationStatementSafety,
    pub reason: String,
}

pub fn plan_migration_sql(sql: &str) -> Result<Vec<MigrationStatementPlan>> {
    let statements = parse_statements(sql)?;
    Ok(statements
        .iter()
        .enumerate()
        .map(|(idx, statement)| {
            let (safety, reason) = classify_migration_statement(statement);
            MigrationStatementPlan {
                ordinal: idx + 1,
                sql: statement.to_string(),
                safety,
                reason,
            }
        })
        .collect())
}

pub fn split_migration_sql(sql: &str) -> Result<Vec<String>> {
    Ok(plan_migration_sql(sql)?
        .into_iter()
        .map(|statement| statement.sql)
        .collect())
}

pub(crate) fn classify_migration_statement(
    statement: &Statement,
) -> (MigrationStatementSafety, String) {
    match statement {
        Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::CreateSequence { .. }
        | Statement::CreateFunction(_)
        | Statement::CreateProcedure { .. }
        | Statement::CreateTrigger(_)
        | Statement::CreateRole(_) => (
            MigrationStatementSafety::TransactionalDdl,
            "metadata is committed through BicDB atomic catalog/record writes".to_string(),
        ),
        Statement::CreateIndex(_) => (
            MigrationStatementSafety::OnlineSafeDdl,
            "BicDB builds secondary indexes from committed records before catalog publication"
                .to_string(),
        ),
        Statement::Drop {
            object_type, names, ..
        } => match object_type {
            ObjectType::Table | ObjectType::Index | ObjectType::Sequence | ObjectType::View => (
                MigrationStatementSafety::TransactionalDdl,
                format!(
                    "DROP {object_type} uses atomic catalog updates for {} object(s)",
                    names.len()
                ),
            ),
            _ => (
                MigrationStatementSafety::Unsupported,
                format!("DROP {object_type} is not supported by BicDB migrations"),
            ),
        },
        Statement::DropFunction(_)
        | Statement::DropProcedure { .. }
        | Statement::AlterFunction(_)
        | Statement::DropTrigger(_) => (
            MigrationStatementSafety::TransactionalDdl,
            "routine/trigger metadata is stored in BicDB system catalogs".to_string(),
        ),
        Statement::AlterTable(alter_table) => classify_alter_table_migration(alter_table),
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => (
            MigrationStatementSafety::NonDdl,
            "DML is allowed in migrations but is row-transactional, not DDL".to_string(),
        ),
        Statement::Analyze(_) => (
            MigrationStatementSafety::OnlineSafeDdl,
            "ANALYZE updates planner statistics from committed records".to_string(),
        ),
        Statement::Lock(_) => (
            MigrationStatementSafety::TransactionalDdl,
            "LOCK TABLE is accepted as a migration coordination command; BicDB serializes catalog writes"
                .to_string(),
        ),
        _ => (
            MigrationStatementSafety::Unsupported,
            format!("statement is not supported by BicDB migrations: {statement}"),
        ),
    }
}

pub(crate) fn classify_alter_table_migration(
    alter_table: &AlterTable,
) -> (MigrationStatementSafety, String) {
    let mut saw_online_safe = false;
    for operation in &alter_table.operations {
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let nullable = column_def
                    .options
                    .iter()
                    .all(|option| !matches!(option.option, ColumnOption::NotNull));
                let has_default = column_def
                    .options
                    .iter()
                    .any(|option| matches!(option.option, ColumnOption::Default(_)));
                if nullable && !has_default {
                    saw_online_safe = true;
                } else {
                    return (
                        MigrationStatementSafety::RowRewrite,
                        "ADD COLUMN with NOT NULL or DEFAULT may rewrite existing rows; use add nullable column, chunked backfill, validate, then enforce".to_string(),
                    );
                }
            }
            AlterTableOperation::AddConstraint { .. }
            | AlterTableOperation::DropConstraint { .. }
            | AlterTableOperation::RenameConstraint { .. }
            | AlterTableOperation::EnableRowLevelSecurity
            | AlterTableOperation::DisableRowLevelSecurity => {
                saw_online_safe = true;
            }
            AlterTableOperation::AlterColumn { op, .. } => match op {
                AlterColumnOperation::SetNotNull
                | AlterColumnOperation::DropNotNull
                | AlterColumnOperation::SetDefault { .. }
                | AlterColumnOperation::DropDefault => saw_online_safe = true,
                AlterColumnOperation::SetDataType { .. }
                | AlterColumnOperation::AddGenerated { .. } => {
                    return (
                        MigrationStatementSafety::RowRewrite,
                        format!(
                            "ALTER COLUMN {op} may rewrite existing rows and is not migration-transactional"
                        ),
                    );
                }
            },
            AlterTableOperation::DropColumn { .. }
            | AlterTableOperation::RenameColumn { .. }
            | AlterTableOperation::RenameTable { .. } => {
                return (
                    MigrationStatementSafety::RowRewrite,
                    format!(
                        "ALTER TABLE operation {operation} rewrites records or collection files"
                    ),
                );
            }
            other => {
                return (
                    MigrationStatementSafety::Unsupported,
                    format!("ALTER TABLE operation {other} is not supported by BicDB migrations"),
                );
            }
        }
    }
    if saw_online_safe {
        (
            MigrationStatementSafety::OnlineSafeDdl,
            "ALTER TABLE operation follows BicDB online-safe metadata-first migration patterns"
                .to_string(),
        )
    } else {
        (
            MigrationStatementSafety::TransactionalDdl,
            "empty ALTER TABLE has no mutation".to_string(),
        )
    }
}

pub fn parse_transaction_control(sql: &str) -> Result<Option<TransactionControl>> {
    let statements = parse_statements(sql)?;
    let [statement] = statements.as_slice() else {
        return Ok(None);
    };
    match statement {
        Statement::StartTransaction { modes, .. } => {
            validate_start_transaction_modes(modes)?;
            Ok(Some(TransactionControl::Begin))
        }
        Statement::Set(Set::SetTransaction {
            modes, snapshot, ..
        }) => {
            if snapshot.is_some() {
                return Err(SqlError::Unsupported(
                    "transaction snapshots are not supported".to_string(),
                ));
            }
            validate_set_transaction_modes(modes)?;
            Ok(Some(TransactionControl::SetTransaction))
        }
        _ => Ok(None),
    }
}

pub(crate) fn validate_start_transaction_modes(modes: &[TransactionMode]) -> Result<()> {
    validate_transaction_modes(modes)
}

pub(crate) fn validate_set_transaction_modes(modes: &[TransactionMode]) -> Result<()> {
    validate_transaction_modes(modes)
}

pub(crate) fn validate_transaction_modes(modes: &[TransactionMode]) -> Result<()> {
    let read_only = modes.iter().any(|mode| {
        matches!(
            mode,
            TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
        )
    });
    for mode in modes {
        if let TransactionMode::IsolationLevel(level) = mode {
            match level {
                TransactionIsolationLevel::ReadCommitted => {}
                TransactionIsolationLevel::RepeatableRead if read_only => {}
                TransactionIsolationLevel::RepeatableRead => {
                    return Err(SqlError::Unsupported(
                        "transaction isolation level REPEATABLE READ requires READ ONLY"
                            .to_string(),
                    ));
                }
                TransactionIsolationLevel::Serializable => {
                    return Err(SqlError::Unsupported(
                        "transaction isolation level SERIALIZABLE is not supported".to_string(),
                    ));
                }
                TransactionIsolationLevel::ReadUncommitted => {
                    return Err(SqlError::Unsupported(
                        "transaction isolation level READ UNCOMMITTED is not supported".to_string(),
                    ));
                }
                TransactionIsolationLevel::Snapshot => {
                    return Err(SqlError::Unsupported(
                        "transaction isolation level SNAPSHOT is not supported".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_query(sql: &str) -> Result<Query> {
    let mut statements = parse_statements(sql)?;
    let Some(Statement::Query(query)) = statements.pop() else {
        return Err(SqlError::InvalidSql(format!("invalid view query {sql}")));
    };
    Ok(*query)
}

pub(crate) fn savepoint_name(name: &Ident) -> String {
    if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    }
}

pub(crate) fn ensure_schema_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(SCHEMA_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_namespace_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(NAMESPACE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_database_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(DATABASE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_extension_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(EXTENSION_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_user_type_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(USER_TYPE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_user_type_oid_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(USER_TYPE_OID_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_sequence_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(SEQUENCE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_view_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(VIEW_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_routine_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(ROUTINE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_trigger_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(TRIGGER_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_notification_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(NOTIFICATION_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_role_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(ROLE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_role_membership_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(ROLE_MEMBERSHIP_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_privilege_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(PRIVILEGE_COLLECTION)?;
    Ok(())
}

pub(crate) fn ensure_default_privilege_collection(db: &mut BicDb) -> Result<()> {
    db.create_collection(DEFAULT_PRIVILEGE_COLLECTION)?;
    Ok(())
}

/// The cached table schema, shared. A cache hit is a refcount bump; callers
/// that only read the schema should use this (or `load_schema_shared`) —
/// `load_schema` clones the whole TableSchema for callers that mutate it.
pub(crate) fn load_schema_raw_shared(
    db: &BicDb,
    table: &str,
) -> Result<Option<std::sync::Arc<TableSchema>>> {
    if let Some(schema) = sql_schema_cache_get(db, table) {
        return Ok(schema);
    }
    let schema = match db.get(SCHEMA_COLLECTION, table) {
        Ok(Some(record)) => {
            let bytes = if sql_profile_active() {
                serde_json::to_vec(&record.metadata)?.len()
            } else {
                0
            };
            sql_profile_schema_load(bytes);
            Some(std::sync::Arc::new(serde_json::from_value::<TableSchema>(
                record.metadata.clone(),
            )?))
        }
        Ok(None) => {
            sql_profile_schema_load(0);
            None
        }
        Err(BicDbError::CollectionNotFound(_)) => {
            sql_profile_schema_load(0);
            None
        }
        Err(error) => return Err(error.into()),
    };
    sql_schema_cache_set(db, table, schema.clone());
    Ok(schema)
}

pub(crate) fn load_schema_raw(db: &BicDb, table: &str) -> Result<Option<TableSchema>> {
    Ok(load_schema_raw_shared(db, table)?.map(|schema| (*schema).clone()))
}

/// `load_schema` without the clone: partition children are reconciled into a
/// fresh Arc; everything else is the cached Arc itself.
pub(crate) fn load_schema_shared(
    db: &BicDb,
    table: &str,
) -> Result<Option<std::sync::Arc<TableSchema>>> {
    let Some(schema) = load_schema_raw_shared(db, table)? else {
        return Ok(None);
    };
    if schema.partition_of.is_none() {
        return Ok(Some(schema));
    }
    reconcile_partition_schema(db, (*schema).clone())
        .map(std::sync::Arc::new)
        .map(Some)
}

pub(crate) fn load_schema(db: &BicDb, table: &str) -> Result<Option<TableSchema>> {
    let Some(schema) = load_schema_raw(db, table)? else {
        return Ok(None);
    };
    reconcile_partition_schema(db, schema).map(Some)
}

pub(crate) fn list_schemas_raw(db: &BicDb) -> Result<Vec<TableSchema>> {
    #[cfg(test)]
    SQL_SCHEMA_LIST_RAW_LOADS.with(|loads| *loads.borrow_mut() += 1);

    let records = match db.scan_collection(SCHEMA_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<TableSchema>(record.metadata).map_err(SqlError::from)
        })
        .collect()
}

pub(crate) fn list_schemas_shared(db: &BicDb) -> Result<std::sync::Arc<Vec<TableSchema>>> {
    if let Some(schemas) = sql_schema_list_cache_get(db) {
        return Ok(schemas);
    }
    let schemas = std::sync::Arc::new(reconcile_partition_schemas(list_schemas_raw(db)?));
    for schema in schemas.iter() {
        sql_schema_cache_set(db, &schema.name, Some(std::sync::Arc::new(schema.clone())));
    }
    sql_schema_list_cache_set(db, std::sync::Arc::clone(&schemas));
    Ok(schemas)
}

pub(crate) fn list_schemas(db: &BicDb) -> Result<Vec<TableSchema>> {
    Ok((*list_schemas_shared(db)?).clone())
}

pub fn ensure_primary_key_indexes(db: &mut BicDb) -> Result<()> {
    for schema in list_schemas(db)? {
        ensure_primary_key_index_for_schema(db, &schema)?;
    }
    Ok(())
}

/// Backfill executable B-tree indexes for SQL UNIQUE constraints created by
/// older BicDB builds. PostgreSQL UNIQUE constraints are index-backed; keeping
/// them as catalog-only metadata forces every validation and ON CONFLICT probe
/// to materialize the complete relation.
pub fn ensure_unique_constraint_indexes(db: &mut BicDb) -> Result<()> {
    for schema in list_schemas(db)? {
        ensure_unique_constraint_indexes_for_schema(db, &schema)?;
    }
    Ok(())
}

pub(crate) fn ensure_unique_constraint_indexes_for_schema(
    db: &mut BicDb,
    schema: &TableSchema,
) -> Result<()> {
    for definition in unique_constraint_index_definitions_for_schema(schema) {
        let existing = db
            .index_definitions()
            .into_iter()
            .find(|index| index.name.eq_ignore_ascii_case(&definition.name));
        if let Some(existing) = existing {
            if existing
                .collection
                .eq_ignore_ascii_case(&definition.collection)
                && existing.unique == definition.unique
                && existing.kind == definition.kind
                && index_field_lists_match(&existing.fields, &definition.fields)
                && existing.predicate == definition.predicate
                && existing.exclusion == definition.exclusion
            {
                continue;
            }
            return Err(SqlError::InvalidSql(format!(
                "index \"{}\" already exists with a definition that does not match UNIQUE constraint \"{}\"",
                definition.name, definition.name
            )));
        }
        db.create_index(definition)?;
    }
    Ok(())
}

pub(crate) fn unique_constraint_index_definitions_for_schema(
    schema: &TableSchema,
) -> Vec<IndexDefinition> {
    schema
        .constraints
        .iter()
        .filter_map(|constraint| {
            let ConstraintSchema::Unique { name, columns, .. } = constraint else {
                return None;
            };
            if unique_constraint_is_primary_key(schema, name, columns) {
                return None;
            }
            Some(IndexDefinition {
                name: name.clone(),
                collection: schema.name.clone(),
                fields: primary_key_index_fields_for_columns(columns),
                unique: true,
                kind: IndexKind::BTree,
                predicate: None,
                exclusion: None,
            })
        })
        .collect()
}

pub(crate) fn ensure_primary_key_index_for_schema(
    db: &mut BicDb,
    schema: &TableSchema,
) -> Result<()> {
    let Some(definition) = primary_key_index_definition_for_schema(schema) else {
        return Ok(());
    };
    if let Some(existing) = executable_primary_key_index_for_schema(db, schema) {
        if existing.name.eq_ignore_ascii_case(&definition.name) {
            return Ok(());
        }
    }
    if db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(&definition.name))
    {
        return Err(SqlError::InvalidSql(format!(
            "index \"{}\" already exists with a definition that does not match primary key \"{}\"",
            definition.name,
            schema.primary_key_constraint_name()
        )));
    }
    db.create_index(definition)?;
    Ok(())
}

pub(crate) fn executable_primary_key_index_for_schema(
    db: &BicDb,
    schema: &TableSchema,
) -> Option<IndexDefinition> {
    let expected = primary_key_index_definition_for_schema(schema)?;
    index_definitions_shared(db)
        .iter()
        .find(|index| {
            index.name.eq_ignore_ascii_case(&expected.name)
                && index.collection.eq_ignore_ascii_case(&expected.collection)
                && index.unique == expected.unique
                && index.kind == expected.kind
                && index_field_lists_match(&index.fields, &expected.fields)
        })
        .cloned()
}

pub(crate) fn primary_key_index_definition_for_schema(
    schema: &TableSchema,
) -> Option<IndexDefinition> {
    if schema.has_hidden_primary_key() {
        return None;
    }
    let columns = primary_key_columns_for_schema(schema);
    if columns.is_empty() {
        return None;
    }
    Some(IndexDefinition {
        name: schema.primary_key_constraint_name(),
        collection: schema.name.clone(),
        fields: primary_key_index_fields_for_columns(&columns),
        unique: true,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
}

pub(crate) fn list_table_catalog_summaries(db: &BicDb) -> Result<Vec<TableCatalogSummary>> {
    let records = match db.scan_collection(SCHEMA_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(table_catalog_summary_from_record)
        .collect()
}

pub(crate) fn table_catalog_summary_from_record(record: Record) -> Result<TableCatalogSummary> {
    let metadata = record.metadata.as_object().ok_or_else(|| {
        SqlError::InvalidSql("schema catalog record must contain an object".to_string())
    })?;
    let name = metadata
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            SqlError::InvalidSql("schema catalog record is missing table name".to_string())
        })?
        .to_string();
    let schema_name = metadata
        .get("schema_name")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .unwrap_or_else(default_schema_name);
    let partitioned = metadata
        .get("partitioning")
        .is_some_and(|value| !value.is_null());
    Ok(TableCatalogSummary {
        name,
        schema_name,
        partitioned,
    })
}

pub(crate) fn reconcile_partition_schema(db: &BicDb, schema: TableSchema) -> Result<TableSchema> {
    let Some(partition_of) = schema.partition_of.as_ref() else {
        return Ok(schema);
    };
    let Some(parent) = load_schema_raw(db, &partition_of.parent_table)? else {
        return Ok(schema);
    };
    Ok(schema_with_parent_inheritance(schema, &parent))
}

pub(crate) fn reconcile_partition_schemas(schemas: Vec<TableSchema>) -> Vec<TableSchema> {
    let by_name = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema.clone()))
        .collect::<BTreeMap<_, _>>();
    schemas
        .into_iter()
        .map(|schema| {
            let Some(partition_of) = schema.partition_of.as_ref() else {
                return schema;
            };
            by_name
                .get(&partition_of.parent_table.to_ascii_lowercase())
                .map(|parent| schema_with_parent_inheritance(schema.clone(), parent))
                .unwrap_or(schema)
        })
        .collect()
}

pub(crate) fn schema_with_parent_inheritance(
    mut schema: TableSchema,
    parent: &TableSchema,
) -> TableSchema {
    if schema.partition_of.is_some() {
        schema.columns = parent.columns.clone();
        schema.primary_key_name = parent.primary_key_name.clone();
        schema.constraints = merge_inherited_constraints(parent, &schema);
        schema.rls_enabled = parent.rls_enabled;
        schema.rls_forced = parent.rls_forced;
        schema.policies = parent.policies.clone();
    }
    schema
}

pub(crate) fn merge_inherited_constraints(
    parent: &TableSchema,
    child: &TableSchema,
) -> Vec<ConstraintSchema> {
    let mut constraints = parent.constraints.clone();
    let inherited_names = constraints
        .iter()
        .map(|constraint| constraint_name(constraint).to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    constraints.extend(
        child
            .constraints
            .iter()
            .filter(|constraint| {
                !inherited_names.contains(&constraint_name(constraint).to_ascii_lowercase())
            })
            .cloned(),
    );
    constraints
}

pub(crate) fn save_schema(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    ensure_schema_collection(db)?;
    let metadata = serde_json::to_value(schema)?;
    if sql_profile_active() {
        sql_profile_schema_save(serde_json::to_vec(&metadata)?.len());
    }
    db.insert(
        SCHEMA_COLLECTION,
        Record::new(&schema.name).with_metadata(metadata),
    )?;
    sql_schema_list_cache_invalidate(db);
    sql_schema_cache_set(db, &schema.name, Some(std::sync::Arc::new(schema.clone())));
    Ok(())
}

pub(crate) fn list_namespaces(db: &BicDb) -> Result<Vec<NamespaceSchema>> {
    let records = match db.scan_collection(NAMESPACE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<NamespaceSchema>(record.metadata).map_err(SqlError::from)
        })
        .collect()
}

pub(crate) fn load_namespace(db: &BicDb, namespace: &str) -> Result<Option<NamespaceSchema>> {
    match db.get(NAMESPACE_COLLECTION, namespace) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) => Ok(None),
        Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn save_namespace_if_missing(
    db: &mut BicDb,
    namespace: NamespaceSchema,
    if_not_exists: bool,
) -> Result<()> {
    ensure_namespace_collection(db)?;
    match db.get(NAMESPACE_COLLECTION, &namespace.name)? {
        Some(_) if if_not_exists => Ok(()),
        Some(_) => Err(SqlError::InvalidSql(format!(
            "schema \"{}\" already exists",
            namespace.name
        ))),
        None => {
            db.insert(
                NAMESPACE_COLLECTION,
                Record::new(&namespace.name).with_metadata(serde_json::to_value(namespace)?),
            )?;
            Ok(())
        }
    }
}

pub(crate) fn save_namespace(db: &mut BicDb, namespace: NamespaceSchema) -> Result<()> {
    ensure_namespace_collection(db)?;
    db.insert(
        NAMESPACE_COLLECTION,
        Record::new(&namespace.name).with_metadata(serde_json::to_value(namespace)?),
    )?;
    Ok(())
}

pub(crate) fn namespace_owner(db: &BicDb, namespace: &str) -> Result<String> {
    Ok(load_namespace(db, namespace)?
        .map(|namespace| namespace.owner)
        .unwrap_or_else(current_role_name))
}

pub(crate) fn delete_namespace(db: &mut BicDb, namespace: &str) -> Result<bool> {
    match db.delete(NAMESPACE_COLLECTION, namespace) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn default_database_schema() -> DatabaseSchema {
    DatabaseSchema {
        name: "bicdb".to_string(),
        owner: current_role_name(),
    }
}

pub(crate) fn list_databases(db: &BicDb) -> Result<Vec<DatabaseSchema>> {
    let mut databases = vec![default_database_schema()];
    let records = match db.scan_collection(DATABASE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    for record in records {
        let database = serde_json::from_value::<DatabaseSchema>(record.metadata)?;
        if let Some(existing) = databases
            .iter_mut()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&database.name))
        {
            *existing = database;
        } else {
            databases.push(database);
        }
    }
    databases.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(databases)
}

pub(crate) fn database_exists(db: &BicDb, name: &str) -> Result<bool> {
    Ok(list_databases(db)?
        .iter()
        .any(|database| database.name.eq_ignore_ascii_case(name)))
}

pub(crate) fn ensure_database_exists(db: &BicDb, name: &str) -> Result<()> {
    if database_exists(db, name)? {
        return Ok(());
    }
    Err(SqlError::InvalidSql(format!(
        "database \"{name}\" does not exist"
    )))
}

pub(crate) fn save_database_if_missing(db: &mut BicDb, database: DatabaseSchema) -> Result<()> {
    if database_exists(db, &database.name)? {
        return Err(SqlError::InvalidSql(format!(
            "database \"{}\" already exists",
            database.name
        )));
    }
    save_database(db, database)
}

pub(crate) fn save_database_owner(db: &mut BicDb, name: &str, owner: &str) -> Result<()> {
    ensure_database_exists(db, name)?;
    save_database(
        db,
        DatabaseSchema {
            name: normalize_database_name(name),
            owner: normalize_role_name(owner),
        },
    )
}

pub(crate) fn save_database(db: &mut BicDb, database: DatabaseSchema) -> Result<()> {
    ensure_database_collection(db)?;
    db.insert(
        DATABASE_COLLECTION,
        Record::new(normalize_database_name(&database.name))
            .with_metadata(serde_json::to_value(database)?),
    )?;
    Ok(())
}

pub(crate) fn normalize_database_name(name: &str) -> String {
    name.trim_matches('"').to_ascii_lowercase()
}

pub(crate) fn list_extensions(db: &BicDb) -> Result<Vec<ExtensionSchema>> {
    let records = match db.scan_collection(EXTENSION_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<ExtensionSchema>(record.metadata).map_err(SqlError::from)
        })
        .collect()
}

pub(crate) fn user_type_key(schema_name: &str, name: &str) -> String {
    serde_json::to_string(&(schema_name, name)).expect("user type identity is serializable")
}

pub(crate) fn list_user_types(db: &BicDb) -> Result<Vec<UserTypeSchema>> {
    let records = match db.scan_collection(USER_TYPE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut types = records
        .into_iter()
        .map(|record| serde_json::from_value::<UserTypeSchema>(record.metadata).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    types.sort_by(|left, right| {
        (&left.schema_name, &left.name).cmp(&(&right.schema_name, &right.name))
    });
    Ok(types)
}

pub(crate) fn load_user_type(
    db: &BicDb,
    schema_name: &str,
    name: &str,
) -> Result<Option<UserTypeSchema>> {
    // Same identity as the stored key (`user_type_key`), served from the
    // generation-validated list instead of a point read + JSON decode per
    // lookup: routine expression evaluation asks for user types per row.
    let types = user_types_shared(db)?;
    Ok(types
        .iter()
        .find(|user_type| user_type.schema_name == schema_name && user_type.name == name)
        .cloned())
}

/// Per-thread, generation-validated memos for the two catalog lookups the
/// pgwire result encoder performs PER RESULT COLUMN: "is this OID a table row
/// type?" and "is this OID a user-defined type?". Both used to re-scan and
/// re-deserialize their whole catalog on every call, outside any session
/// schema-cache scope, which made result encoding cost more than executing
/// the query (TPC-C on dev9 fell from ~418k to ~30k NOPM; 77% of CPU was
/// `list_schemas` under `row_description_with_db`).
struct RowTypeOidIndex {
    schemas: Vec<TableSchema>,
    row: rustc_hash::FxHashMap<i64, usize>,
    array: rustc_hash::FxHashMap<i64, usize>,
}

thread_local! {
    static ROW_TYPE_OID_INDEX: std::cell::RefCell<rustc_hash::FxHashMap<u64, (u64, std::sync::Arc<RowTypeOidIndex>)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
    static USER_TYPES_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<u64, (u64, std::sync::Arc<Vec<UserTypeSchema>>)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

fn row_type_oid_index(db: &BicDb) -> Result<std::sync::Arc<RowTypeOidIndex>> {
    let key = db.instance_id();
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    let hit = ROW_TYPE_OID_INDEX.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generation)
            .map(|(_, index)| std::sync::Arc::clone(index))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let schemas = (*list_schemas_shared(db)?).clone();
    let mut row = rustc_hash::FxHashMap::default();
    let mut array = rustc_hash::FxHashMap::default();
    for (position, schema) in schemas.iter().enumerate() {
        row.insert(schema.row_type_oid(), position);
        array.insert(schema.row_array_type_oid(), position);
    }
    let index = std::sync::Arc::new(RowTypeOidIndex {
        schemas,
        row,
        array,
    });
    ROW_TYPE_OID_INDEX.with(|memo| {
        memo.borrow_mut()
            .insert(key, (generation, std::sync::Arc::clone(&index)));
    });
    Ok(index)
}

fn user_types_shared(db: &BicDb) -> Result<std::sync::Arc<Vec<UserTypeSchema>>> {
    let key = db.instance_id();
    let generation = db.collection_generation(USER_TYPE_COLLECTION);
    let hit = USER_TYPES_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generation)
            .map(|(_, types)| std::sync::Arc::clone(types))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let types = std::sync::Arc::new(list_user_types(db)?);
    USER_TYPES_MEMO.with(|memo| {
        memo.borrow_mut()
            .insert(key, (generation, std::sync::Arc::clone(&types)));
    });
    Ok(types)
}

pub(crate) fn load_user_type_by_oid(db: &BicDb, oid: i64) -> Result<Option<UserTypeSchema>> {
    let types = user_types_shared(db)?;
    if types.is_empty() {
        return Ok(None);
    }
    Ok(types
        .iter()
        .find(|user_type| user_type.oid == oid || user_type.array_oid == oid)
        .cloned())
}

pub(crate) fn user_type_comment_by_oid(db: &BicDb, oid: i64) -> Result<Option<String>> {
    Ok(load_user_type_by_oid(db, oid)?.and_then(|user_type| {
        if user_type.oid == oid {
            user_type.comment
        } else {
            None
        }
    }))
}

pub(crate) fn table_row_type_schema(schema: &TableSchema) -> UserTypeSchema {
    let attributes = schema
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .map(|column| CompositeAttributeSchema {
            name: column.name.clone(),
            pg_type: column.pg_type.clone(),
            user_type: column.user_type.clone(),
            collation: column.collation.clone(),
            type_modifier: column.type_modifier.clone(),
            array_ndims: column.array_ndims,
            dropped: false,
        })
        .collect();
    UserTypeSchema {
        name: schema.name.clone(),
        schema_name: schema.schema_name.clone(),
        owner: schema.owner.clone().unwrap_or_else(current_role_name),
        comment: None,
        acl_explicit: false,
        oid: schema.row_type_oid(),
        array_oid: schema.row_array_type_oid(),
        kind: UserTypeKind::Composite {
            relation_oid: 0,
            attributes,
        },
    }
}

pub(crate) fn table_row_type_column(schema: &TableSchema, array: bool) -> UserTypeColumnSchema {
    table_row_type_schema(schema).column_type(array)
}

pub fn pg_user_type_oid_by_name(db: &BicDb, type_name: &str) -> Result<Option<i32>> {
    let type_name = type_name.trim();
    let (type_name, array) = type_name
        .strip_suffix("[]")
        .map(|name| (name.trim(), true))
        .unwrap_or((type_name, false));
    let parts = type_name
        .split('.')
        .map(|part| part.trim().trim_matches('"'))
        .collect::<Vec<_>>();
    let (schema_name, name) = match parts.as_slice() {
        [name] => ("public", *name),
        [schema_name, name] => (*schema_name, *name),
        _ => return Ok(None),
    };
    let oid = if let Some(user_type) = load_user_type(db, schema_name, name)? {
        if array {
            user_type.array_oid
        } else {
            user_type.oid
        }
    } else if let Some(schema) = list_schemas(db)?.into_iter().find(|schema| {
        schema.schema_name.eq_ignore_ascii_case(schema_name)
            && schema.name.eq_ignore_ascii_case(name)
    }) {
        if array {
            schema.row_array_type_oid()
        } else {
            schema.row_type_oid()
        }
    } else {
        return Ok(None);
    };
    i32::try_from(oid)
        .map(Some)
        .map_err(|_| SqlError::numeric_value_out_of_range("user-defined type OID exceeds int4"))
}

pub fn pg_table_row_array_element_oid(db: &BicDb, oid: i32) -> Result<Option<i32>> {
    let index = row_type_oid_index(db)?;
    let Some(&position) = index.array.get(&i64::from(oid)) else {
        return Ok(None);
    };
    i32::try_from(index.schemas[position].row_type_oid())
        .map(Some)
        .map_err(|_| SqlError::numeric_value_out_of_range("table row type OID exceeds int4"))
}

pub fn pg_is_table_row_type_oid(db: &BicDb, oid: i32) -> Result<bool> {
    let index = row_type_oid_index(db)?;
    let oid = i64::from(oid);
    if index.row.contains_key(&oid) || index.array.contains_key(&oid) {
        return Ok(true);
    }
    Ok(
        load_user_type_by_oid(db, i64::from(oid))?.is_some_and(|user_type| {
            user_type.oid == i64::from(oid)
                && matches!(user_type.kind, UserTypeKind::Composite { .. })
        }),
    )
}

pub fn pg_table_row_type_definition(
    db: &BicDb,
    oid: i32,
) -> Result<Option<(String, Vec<(String, String)>)>> {
    if let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? {
        if user_type.oid == i64::from(oid) {
            if let UserTypeKind::Composite { attributes, .. } = user_type.kind {
                let type_name = if user_type.schema_name.eq_ignore_ascii_case("public") {
                    user_type.name
                } else {
                    format!("{}.{}", user_type.schema_name, user_type.name)
                };
                let fields = attributes
                    .into_iter()
                    .filter(|attribute| !attribute.dropped)
                    .map(|attribute| (attribute.name, attribute.pg_type))
                    .collect();
                return Ok(Some((type_name, fields)));
            }
        }
    }
    for schema in list_schemas(db)? {
        if schema.row_type_oid() != i64::from(oid) {
            continue;
        }
        let type_name = if schema.schema_name.eq_ignore_ascii_case("public") {
            schema.name.clone()
        } else {
            format!("{}.{}", schema.schema_name, schema.name)
        };
        let fields = schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| (column.name.clone(), column.pg_type.clone()))
            .collect();
        return Ok(Some((type_name, fields)));
    }
    Ok(None)
}

pub fn pg_user_type_array_element_oid(db: &BicDb, oid: i32) -> Result<Option<i32>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.array_oid != i64::from(oid) {
        return Ok(None);
    }
    i32::try_from(user_type.oid)
        .map(Some)
        .map_err(|_| SqlError::numeric_value_out_of_range("user-defined type OID exceeds int4"))
}

fn user_type_delimiter(kind: &UserTypeKind) -> char {
    match kind {
        UserTypeKind::Base { delimiter, .. } => *delimiter,
        UserTypeKind::Domain {
            base_user_type: Some(base),
            ..
        } => user_type_delimiter(&base.kind),
        _ => ',',
    }
}

pub fn pg_user_type_array_delimiter(db: &BicDb, oid: i32) -> Result<Option<char>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.array_oid != i64::from(oid) {
        return Ok(None);
    }
    Ok(Some(user_type_delimiter(&user_type.kind)))
}

pub fn pg_user_type_delimiter(db: &BicDb, oid: i32) -> Result<Option<char>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.oid != i64::from(oid) {
        return Ok(None);
    }
    Ok(Some(user_type_delimiter(&user_type.kind)))
}

pub(crate) fn pg_type_delimiter_with_db(db: &BicDb, type_name: &str) -> Result<char> {
    let (schema_name, name) = type_name
        .rsplit_once('.')
        .map(|(schema_name, name)| (schema_name.trim_matches('"'), name.trim_matches('"')))
        .unwrap_or(("public", type_name.trim_matches('"')));
    Ok(load_user_type(db, schema_name, name)?
        .map(|user_type| user_type_delimiter(&user_type.kind))
        .or_else(|| pg_type_delimiter(name))
        .unwrap_or(','))
}

/// Whether `role` administers the server.
///
/// Exposed for the wire layer, which must decide whether a session may see
/// other sessions' peer addresses and in-flight SQL. Mirrors the SQL
/// layer's own rule: the bootstrap role, or a role the catalog marks
/// superuser.
pub fn pg_role_is_superuser(db: &BicDb, role: &str) -> Result<bool> {
    if role.eq_ignore_ascii_case(BOOTSTRAP_ROLE_NAME) {
        return Ok(true);
    }
    Ok(load_role_schema(db, role)?.is_some_and(|schema| schema.superuser))
}

pub fn pg_is_user_type_oid(db: &BicDb, oid: i32) -> Result<bool> {
    Ok(load_user_type_by_oid(db, i64::from(oid))?.is_some())
}

pub fn pg_user_type_binary_base_oid(db: &BicDb, oid: i32) -> Result<Option<i32>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.array_oid == i64::from(oid) {
        return Ok(None);
    }
    let base_oid = match user_type.kind {
        UserTypeKind::Base {
            codec_type,
            receive: Some(_),
            send: Some(_),
            ..
        } => pg_type_oid(&codec_type),
        UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        } => base_user_type
            .as_deref()
            .map(UserTypeColumnSchema::type_oid)
            .unwrap_or_else(|| pg_type_oid(&base_type)),
        _ => return Ok(None),
    };
    i32::try_from(base_oid)
        .map(Some)
        .map_err(|_| SqlError::numeric_value_out_of_range("domain base type OID exceeds int4"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgUserRangeTypeInfo {
    pub range_oid: i32,
    pub multirange_oid: i32,
    pub subtype_oid: i32,
    pub multirange: bool,
}

pub fn pg_user_range_type_info(db: &BicDb, oid: i32) -> Result<Option<PgUserRangeTypeInfo>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.array_oid == i64::from(oid) {
        return Ok(None);
    }
    let info = match user_type.kind {
        UserTypeKind::Range {
            value,
            multirange_oid,
            ..
        } => PgUserRangeTypeInfo {
            range_oid: i32::try_from(user_type.oid)
                .map_err(|_| SqlError::numeric_value_out_of_range("user range OID exceeds int4"))?,
            multirange_oid: i32::try_from(multirange_oid).map_err(|_| {
                SqlError::numeric_value_out_of_range("user multirange OID exceeds int4")
            })?,
            subtype_oid: i32::try_from(value.subtype_oid).map_err(|_| {
                SqlError::numeric_value_out_of_range("range subtype OID exceeds int4")
            })?,
            multirange: false,
        },
        UserTypeKind::Multirange {
            value, range_oid, ..
        } => PgUserRangeTypeInfo {
            range_oid: i32::try_from(range_oid)
                .map_err(|_| SqlError::numeric_value_out_of_range("user range OID exceeds int4"))?,
            multirange_oid: i32::try_from(user_type.oid).map_err(|_| {
                SqlError::numeric_value_out_of_range("user multirange OID exceeds int4")
            })?,
            subtype_oid: i32::try_from(value.subtype_oid).map_err(|_| {
                SqlError::numeric_value_out_of_range("range subtype OID exceeds int4")
            })?,
            multirange: true,
        },
        _ => return Ok(None),
    };
    Ok(Some(info))
}

pub fn pg_user_range_values(
    db: &BicDb,
    oid: i32,
    value: &SqlValue,
) -> Result<Option<Vec<PgRange>>> {
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    let column = user_type.column_type(false);
    let canonical = cast_value_to_user_type(value.clone(), &column)?;
    if matches!(canonical, SqlValue::Null) {
        return Ok(Some(Vec::new()));
    }
    let text = canonical.to_cell();
    match &user_type.kind {
        UserTypeKind::Range { value, .. } => PgRange::from_postgres_text_with_policy(
            &text,
            &column.formatted_name(),
            &value.subtype,
            value.canonical_discrete,
        )
        .map(|range| Some(vec![range]))
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string())),
        UserTypeKind::Multirange { value, .. } => parse_pg_multirange_with_policy(
            &text,
            &column.formatted_name(),
            &value.subtype,
            value.canonical_discrete,
        )
        .map(Some)
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string())),
        _ => Ok(None),
    }
}

pub(crate) fn save_user_type(db: &mut BicDb, user_type: &UserTypeSchema) -> Result<()> {
    ensure_user_type_collection(db)?;
    db.insert(
        USER_TYPE_COLLECTION,
        Record::new(user_type_key(&user_type.schema_name, &user_type.name))
            .with_metadata(serde_json::to_value(user_type)?),
    )?;
    Ok(())
}

pub(crate) fn delete_user_type(db: &mut BicDb, schema_name: &str, name: &str) -> Result<bool> {
    match db.delete(USER_TYPE_COLLECTION, &user_type_key(schema_name, name)) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn allocate_user_type_oids(db: &mut BicDb, count: usize) -> Result<Vec<i64>> {
    ensure_user_type_oid_collection(db)?;
    let allocator = match db.get(USER_TYPE_OID_COLLECTION, "allocator")? {
        Some(record) => serde_json::from_value::<UserTypeOidAllocator>(record.metadata.clone())?,
        None => UserTypeOidAllocator {
            version: USER_TYPE_OID_ALLOCATOR_VERSION,
            next_oid: FIRST_USER_TYPE_OID,
        },
    };
    if allocator.version > USER_TYPE_OID_ALLOCATOR_VERSION {
        return Err(SqlError::InvalidSql(format!(
            "user type OID allocator version {} is newer than supported version {}",
            allocator.version, USER_TYPE_OID_ALLOCATOR_VERSION
        )));
    }
    let count = i64::try_from(count)
        .map_err(|_| SqlError::InvalidSql("too many user type OIDs requested".to_string()))?;
    if count == 0 {
        return Ok(Vec::new());
    }

    let occupied = occupied_user_type_oids(db)?;
    let first_oid = allocator.next_oid.max(FIRST_USER_TYPE_OID).max(
        occupied
            .range(FIRST_USER_TYPE_OID..USER_TYPE_OID_LIMIT)
            .next_back()
            .copied()
            .unwrap_or(0)
            + 1,
    );
    let end = first_oid
        .checked_add(count)
        .ok_or_else(|| SqlError::InvalidSql("user type OID space exhausted".to_string()))?;
    if end > USER_TYPE_OID_LIMIT {
        return Err(SqlError::InvalidSql(
            "user type OID space exhausted".to_string(),
        ));
    }
    let allocated = (first_oid..end).collect::<Vec<_>>();
    let mut records = allocated
        .iter()
        .map(|oid| {
            Ok(
                Record::new(format!("oid:{oid}")).with_metadata(serde_json::to_value(
                    UserTypeOidReservation {
                        version: USER_TYPE_OID_ALLOCATOR_VERSION,
                        oid: *oid,
                    },
                )?),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    records.push(Record::new("allocator").with_metadata(serde_json::to_value(
        UserTypeOidAllocator {
            version: USER_TYPE_OID_ALLOCATOR_VERSION,
            next_oid: end,
        },
    )?));
    db.batch_insert(USER_TYPE_OID_COLLECTION, records)?;
    Ok(allocated)
}

fn occupied_user_type_oids(db: &BicDb) -> Result<BTreeSet<i64>> {
    let mut occupied = BTreeSet::new();
    for record in db.scan_collection(USER_TYPE_OID_COLLECTION)? {
        if record.id == "allocator" {
            continue;
        }
        let Some(key_oid) = record.id.strip_prefix("oid:") else {
            return Err(SqlError::InvalidSql(format!(
                "invalid user type OID reservation key {}",
                record.id
            )));
        };
        let key_oid = key_oid.parse::<i64>().map_err(|_| {
            SqlError::InvalidSql(format!(
                "invalid user type OID reservation key {}",
                record.id
            ))
        })?;
        let reservation =
            serde_json::from_value::<UserTypeOidReservation>(record.metadata.clone())?;
        if reservation.version > USER_TYPE_OID_ALLOCATOR_VERSION || reservation.oid != key_oid {
            return Err(SqlError::InvalidSql(format!(
                "invalid user type OID reservation metadata for {}",
                record.id
            )));
        }
        occupied.insert(key_oid);
    }
    for user_type in list_user_types(db)? {
        occupied.insert(user_type.oid);
        occupied.insert(user_type.array_oid);
        match user_type.kind {
            UserTypeKind::Enum { labels } => {
                occupied.extend(labels.into_iter().map(|label| label.oid));
            }
            UserTypeKind::Composite { relation_oid, .. } => {
                occupied.insert(relation_oid);
            }
            _ => {}
        }
    }
    for schema in list_schemas(db)? {
        occupied.insert(schema.row_type_oid());
        occupied.insert(schema.row_array_type_oid());
    }
    Ok(occupied)
}

pub(crate) fn save_extension_if_missing(
    db: &mut BicDb,
    extension: ExtensionSchema,
    if_not_exists: bool,
) -> Result<()> {
    if extension.name.eq_ignore_ascii_case("plpgsql") {
        return if if_not_exists {
            Ok(())
        } else {
            Err(SqlError::InvalidSql(
                "extension \"plpgsql\" already exists".to_string(),
            ))
        };
    }
    ensure_extension_collection(db)?;
    match db.get(EXTENSION_COLLECTION, &extension.name)? {
        Some(_) if if_not_exists => Ok(()),
        Some(_) => Err(SqlError::InvalidSql(format!(
            "extension \"{}\" already exists",
            extension.name
        ))),
        None => {
            db.insert(
                EXTENSION_COLLECTION,
                Record::new(&extension.name).with_metadata(serde_json::to_value(extension)?),
            )?;
            Ok(())
        }
    }
}

pub(crate) fn delete_schema(db: &mut BicDb, table: &str) -> Result<()> {
    let result = match db.delete(SCHEMA_COLLECTION, table) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    };
    if result.is_ok() {
        sql_schema_list_cache_invalidate(db);
        sql_schema_cache_set(db, table, None);
    }
    result
}

pub(crate) fn default_role_schema(name: &str) -> RoleSchema {
    RoleSchema {
        name: normalize_role_name(name),
        superuser: false,
        inherit: true,
        create_role: false,
        create_db: false,
        can_login: false,
        replication: false,
        bypass_rls: false,
        connection_limit: -1,
        password_set: false,
        valid_until: None,
    }
}

pub(crate) fn current_role_schema() -> RoleSchema {
    RoleSchema {
        name: current_role_name(),
        can_login: true,
        ..default_role_schema("bicdb")
    }
}

pub(crate) fn create_role_record(
    db: &mut BicDb,
    role: RoleSchema,
    if_not_exists: bool,
) -> Result<()> {
    if role_exists(db, &role.name)? {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::DuplicateRole { name: role.name });
    }
    ensure_role_collection(db)?;
    db.insert(
        ROLE_COLLECTION,
        Record::new(&role.name).with_metadata(serde_json::to_value(role)?),
    )?;
    Ok(())
}

pub(crate) fn role_exists(db: &BicDb, name: &str) -> Result<bool> {
    let name = normalize_role_name(name);
    if name == current_role_name() {
        return Ok(true);
    }
    match db.get(ROLE_COLLECTION, &name) {
        Ok(Some(_)) => Ok(true),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_roles(db: &BicDb) -> Result<Vec<RoleSchema>> {
    let mut roles = vec![current_role_schema()];
    let records = match db.scan_collection(ROLE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    for record in records {
        let role = serde_json::from_value::<RoleSchema>(record.metadata)?;
        if !roles
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(&role.name))
        {
            roles.push(role);
        }
    }
    roles.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(roles)
}

pub(crate) fn ensure_known_role(roles: &[RoleSchema], name: &str) -> Result<()> {
    if roles
        .iter()
        .any(|role| role.name.eq_ignore_ascii_case(name))
    {
        return Ok(());
    }
    Err(SqlError::UndefinedRole {
        name: name.to_string(),
    })
}

pub(crate) fn save_role_membership(db: &mut BicDb, membership: &RoleMembership) -> Result<()> {
    ensure_role_membership_collection(db)?;
    db.insert(
        ROLE_MEMBERSHIP_COLLECTION,
        Record::new(role_membership_key(&membership.role, &membership.member))
            .with_metadata(serde_json::to_value(membership)?),
    )?;
    Ok(())
}

pub(crate) fn delete_role_membership(db: &mut BicDb, role: &str, member: &str) -> Result<()> {
    match db.delete(
        ROLE_MEMBERSHIP_COLLECTION,
        &role_membership_key(role, member),
    ) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_role_memberships(db: &BicDb) -> Result<Vec<RoleMembership>> {
    let records = match db.scan_collection(ROLE_MEMBERSHIP_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| serde_json::from_value::<RoleMembership>(record.metadata).map_err(Into::into))
        .collect()
}

/// Copies PostgreSQL's cluster-wide role catalog into a newly-created database.
/// Object ACLs remain database-local; only roles and role memberships are shared.
pub fn copy_cluster_role_catalog(source: &BicDb, target: &mut BicDb) -> Result<()> {
    ensure_role_collection(target)?;
    ensure_role_membership_collection(target)?;
    for role in list_roles(source)? {
        if role.name != current_role_name() {
            if role_exists(target, &role.name)? {
                delete_role_record(target, &role.name)?;
            }
            create_role_record(target, role, false)?;
        }
    }
    for membership in list_role_memberships(target)? {
        delete_role_membership(target, &membership.role, &membership.member)?;
    }
    for membership in list_role_memberships(source)? {
        save_role_membership(target, &membership)?;
    }
    Ok(())
}

pub(crate) fn role_membership_key(role: &str, member: &str) -> String {
    format!(
        "{}:{}",
        normalize_role_name(role),
        normalize_role_name(member)
    )
}

pub(crate) fn save_privilege(db: &mut BicDb, grant: &PrivilegeGrant) -> Result<()> {
    ensure_privilege_collection(db)?;
    db.insert(
        PRIVILEGE_COLLECTION,
        Record::new(privilege_key(grant)).with_metadata(serde_json::to_value(grant)?),
    )?;
    Ok(())
}

pub(crate) fn delete_privilege(db: &mut BicDb, grant: &PrivilegeGrant) -> Result<()> {
    match db.delete(PRIVILEGE_COLLECTION, &privilege_key(grant)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_privileges(db: &BicDb) -> Result<Vec<PrivilegeGrant>> {
    let records = match db.scan_collection(PRIVILEGE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| serde_json::from_value::<PrivilegeGrant>(record.metadata).map_err(Into::into))
        .collect()
}

pub(crate) fn privilege_key(grant: &PrivilegeGrant) -> String {
    let key = format!(
        "{}:{}:{}:{}",
        privilege_object_type_name(grant.object_type),
        grant.object_name,
        grant.grantee,
        grant.privilege
    );
    match &grant.column {
        Some(column) => format!(
            "{key}:column:{}",
            serde_json::to_string(column).expect("string JSON")
        ),
        None => key,
    }
}

pub(crate) fn save_default_privilege(db: &mut BicDb, grant: &DefaultPrivilegeGrant) -> Result<()> {
    ensure_default_privilege_collection(db)?;
    db.insert(
        DEFAULT_PRIVILEGE_COLLECTION,
        Record::new(default_privilege_key(grant)).with_metadata(serde_json::to_value(grant)?),
    )?;
    Ok(())
}

pub(crate) fn delete_default_privilege(
    db: &mut BicDb,
    grant: &DefaultPrivilegeGrant,
) -> Result<()> {
    match db.delete(DEFAULT_PRIVILEGE_COLLECTION, &default_privilege_key(grant)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_default_privileges(db: &BicDb) -> Result<Vec<DefaultPrivilegeGrant>> {
    let records = match db.scan_collection(DEFAULT_PRIVILEGE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<DefaultPrivilegeGrant>(record.metadata).map_err(Into::into)
        })
        .collect()
}

pub(crate) fn default_privilege_key(grant: &DefaultPrivilegeGrant) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        grant.grantor,
        grant.schema_name,
        privilege_object_type_name(grant.object_type),
        grant.grantee,
        grant.privilege
    )
}

pub(crate) fn privilege_object_type_name(object_type: PrivilegeObjectType) -> &'static str {
    match object_type {
        PrivilegeObjectType::Database => "database",
        PrivilegeObjectType::Schema => "schema",
        PrivilegeObjectType::Table => "table",
        PrivilegeObjectType::Function => "function",
        PrivilegeObjectType::Sequence => "sequence",
        PrivilegeObjectType::Type => "type",
    }
}

pub(crate) fn normalize_role_name(name: &str) -> String {
    name.trim_matches('"').to_ascii_lowercase()
}

pub(crate) const BOOTSTRAP_ROLE_NAME: &str = "bicdb";

// Reserved session GUC keys mirroring PostgreSQL's identity GUCs.
pub(crate) const SESSION_AUTHORIZATION_GUC: &str = "session_authorization";
pub(crate) const CURRENT_ROLE_GUC: &str = "role";
// Connect-time identity recorded by the pgwire server; SET SESSION
// AUTHORIZATION permission checks and RESET restore against this value.
pub(crate) const INITIAL_SESSION_AUTHORIZATION_GUC: &str = "bicdb.initial_session_authorization";
// Internal channel for definer-view semantics: RLS bypass/policy-selection
// decisions are made as this role while expressions still evaluate with the
// session's identity, matching PostgreSQL's checkAsUser behavior. Reserved;
// not settable from SQL.
pub(crate) const RLS_CHECK_AS_GUC: &str = "bicdb.rls_check_as";

pub(crate) fn rls_check_user_from_gucs(gucs: &HashMap<String, String>) -> String {
    gucs.get(RLS_CHECK_AS_GUC)
        .map(|user| normalize_role_name(user))
        .filter(|user| !user.is_empty())
        .unwrap_or_else(|| current_user_from_gucs(gucs))
}

/// Whether the session's EFFECTIVE SQL role is a superuser.
///
/// Used to build `BrokerCaller::Sql`. This is an authorization fact about a
/// known identity — deliberately not a proxy for "where did this call come
/// from", which the broker now receives explicitly.
pub(crate) fn session_role_is_superuser(
    db: &BicDb,
    session_gucs: &HashMap<String, String>,
) -> bool {
    let role = current_user_from_gucs(session_gucs);
    role == BOOTSTRAP_ROLE_NAME
        || role_schema_shared(db, &role)
            .ok()
            .flatten()
            .is_some_and(|role| role.superuser)
}

pub(crate) fn current_role_name() -> String {
    BOOTSTRAP_ROLE_NAME.to_string()
}

pub(crate) fn session_user_from_gucs(gucs: &HashMap<String, String>) -> String {
    gucs.get(SESSION_AUTHORIZATION_GUC)
        .map(|user| normalize_role_name(user))
        .filter(|user| !user.is_empty())
        .unwrap_or_else(current_role_name)
}

pub(crate) fn current_user_from_gucs(gucs: &HashMap<String, String>) -> String {
    gucs.get(CURRENT_ROLE_GUC)
        .map(|role| normalize_role_name(role))
        .filter(|role| !role.is_empty() && role != "none")
        .unwrap_or_else(|| session_user_from_gucs(gucs))
}

pub(crate) fn initial_session_user_from_gucs(gucs: &HashMap<String, String>) -> String {
    gucs.get(INITIAL_SESSION_AUTHORIZATION_GUC)
        .map(|user| normalize_role_name(user))
        .filter(|user| !user.is_empty())
        .unwrap_or_else(|| session_user_from_gucs(gucs))
}

thread_local! {
    // role name -> parsed role, validated by the role catalog generation.
    static ROLE_SCHEMA_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<(usize, String), (u64, Option<std::sync::Arc<RoleSchema>>)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
    // (role, routine) -> EXECUTE decision, validated by every catalog it
    // derives from: roles, routines (ownership), privileges, memberships.
    static ROUTINE_EXECUTE_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<(usize, String, String), ([u64; 4], bool)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

const AUTHZ_MEMO_MAX_ENTRIES: usize = 4096;

/// The parsed role record, shared. `load_role_schema` ran a catalog point
/// read plus a JSON deserialization on every privilege check — and the
/// routine EXECUTE gate runs one per stored-function call, so a TPC-C NEWORD
/// (about twenty DBMS_RANDOM calls) parsed the caller's role twenty times.
pub(crate) fn role_schema_shared(
    db: &BicDb,
    name: &str,
) -> Result<Option<std::sync::Arc<RoleSchema>>> {
    let name = normalize_role_name(name);
    if name == current_role_name() {
        return Ok(Some(std::sync::Arc::new(current_role_schema())));
    }
    let generation = db.collection_generation(ROLE_COLLECTION);
    let key = (db as *const BicDb as usize, name);
    let hit = ROLE_SCHEMA_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generation)
            .map(|(_, role)| role.clone())
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let loaded = match db.get(ROLE_COLLECTION, &key.1) {
        Ok(Some(record)) => Some(std::sync::Arc::new(serde_json::from_value::<RoleSchema>(
            record.metadata.clone(),
        )?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    ROLE_SCHEMA_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= AUTHZ_MEMO_MAX_ENTRIES {
            memo.clear();
        }
        memo.insert(key, (generation, loaded.clone()));
    });
    Ok(loaded)
}

pub(crate) fn load_role_schema(db: &BicDb, name: &str) -> Result<Option<RoleSchema>> {
    Ok(role_schema_shared(db, name)?.map(|role| (*role).clone()))
}

// The set of roles whose privileges `user` holds through membership,
// including `user` itself. Each edge's INHERIT option controls expansion;
// legacy membership metadata falls back to the member role's attribute.
pub(crate) fn role_privilege_closure(db: &BicDb, user: &str) -> Result<BTreeSet<String>> {
    let user = normalize_role_name(user);
    let mut closure = BTreeSet::new();
    closure.insert(user.clone());
    let memberships = list_role_memberships(db)?;
    if memberships.is_empty() {
        return Ok(closure);
    }
    let mut pending = vec![user];
    while let Some(member) = pending.pop() {
        let inherits = load_role_schema(db, &member)?.is_none_or(|role| role.inherit);
        for membership in &memberships {
            if membership.inherit_option.unwrap_or(inherits)
                && normalize_role_name(&membership.member) == member
            {
                let role = normalize_role_name(&membership.role);
                if closure.insert(role.clone()) {
                    pending.push(role);
                }
            }
        }
    }
    Ok(closure)
}

// Roles `user` could SET ROLE to: every edge on the path must allow SET,
// independently of INHERIT.
pub(crate) fn settable_role_closure(db: &BicDb, user: &str) -> Result<BTreeSet<String>> {
    membership_closure(db, user, true)
}

fn membership_closure(db: &BicDb, user: &str, require_set: bool) -> Result<BTreeSet<String>> {
    let user = normalize_role_name(user);
    let mut closure = BTreeSet::new();
    closure.insert(user.clone());
    let memberships = list_role_memberships(db)?;
    let mut pending = vec![user];
    while let Some(member) = pending.pop() {
        for membership in &memberships {
            if (!require_set || membership.set_option)
                && normalize_role_name(&membership.member) == member
            {
                let role = normalize_role_name(&membership.role);
                if closure.insert(role.clone()) {
                    pending.push(role);
                }
            }
        }
    }
    Ok(closure)
}

pub(crate) fn privilege_names(
    privileges: &Privileges,
    objects: &GrantObjects,
) -> Result<Vec<String>> {
    let names = match privileges {
        Privileges::All { .. } => match objects {
            GrantObjects::Databases(_) => vec!["CONNECT", "CREATE", "TEMPORARY"],
            GrantObjects::Schemas(_) => vec!["CREATE", "USAGE"],
            GrantObjects::Tables(_) | GrantObjects::AllTablesInSchema { .. } => vec![
                "DELETE",
                "INSERT",
                "REFERENCES",
                "SELECT",
                "TRIGGER",
                "TRUNCATE",
                "UPDATE",
            ],
            GrantObjects::Sequences(_) | GrantObjects::AllSequencesInSchema { .. } => {
                vec!["SELECT", "UPDATE", "USAGE"]
            }
            GrantObjects::Function { .. } | GrantObjects::AllFunctionsInSchema { .. } => {
                vec!["EXECUTE"]
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "privileges on {other} are not supported"
                )));
            }
        },
        Privileges::Actions(actions) => actions
            .iter()
            .map(privilege_action_name)
            .collect::<Result<Vec<_>>>()?,
    };
    Ok(names.into_iter().map(str::to_string).collect())
}

pub(crate) fn privilege_action_name(action: &Action) -> Result<&'static str> {
    match action {
        Action::Connect => Ok("CONNECT"),
        Action::Create { obj_type: None } => Ok("CREATE"),
        Action::Delete => Ok("DELETE"),
        Action::Insert { columns: None } => Ok("INSERT"),
        Action::References { columns: None } => Ok("REFERENCES"),
        Action::Select { columns: None } => Ok("SELECT"),
        Action::Trigger => Ok("TRIGGER"),
        Action::Truncate => Ok("TRUNCATE"),
        Action::Update { columns: None } => Ok("UPDATE"),
        Action::Usage => Ok("USAGE"),
        Action::Temporary => Ok("TEMPORARY"),
        Action::Execute { obj_type: None } | Action::Exec { obj_type: None } => Ok("EXECUTE"),
        Action::Insert { columns: Some(_) }
        | Action::References { columns: Some(_) }
        | Action::Select { columns: Some(_) }
        | Action::Update { columns: Some(_) } => Err(SqlError::Unsupported(
            "column-level privileges are not supported".to_string(),
        )),
        other => Err(SqlError::Unsupported(format!(
            "privilege {other} is not supported"
        ))),
    }
}

pub(crate) fn grant_targets(
    db: &BicDb,
    objects: &GrantObjects,
) -> Result<Vec<(PrivilegeObjectType, String)>> {
    match objects {
        GrantObjects::Databases(databases) => databases
            .iter()
            .map(|database| {
                let name = normalize_object_name(&object_name(database)?);
                if !list_databases(db)?
                    .into_iter()
                    .any(|candidate| candidate.name.eq_ignore_ascii_case(&name))
                {
                    return Err(SqlError::InvalidSql(format!(
                        "database `{name}` does not exist"
                    )));
                }
                Ok((PrivilegeObjectType::Database, name))
            })
            .collect(),
        GrantObjects::Schemas(schemas) => schemas
            .iter()
            .map(|schema| {
                let name = schema_name(schema)?;
                if !is_known_schema(&name) && load_namespace(db, &name)?.is_none() {
                    return Err(SqlError::InvalidCollection(name));
                }
                Ok((PrivilegeObjectType::Schema, name))
            })
            .collect(),
        GrantObjects::Tables(tables) => tables
            .iter()
            .map(|table| {
                let name = relation_name(table)?;
                if !relation_exists(db, &name) {
                    return Err(SqlError::InvalidCollection(name));
                }
                Ok((PrivilegeObjectType::Table, name))
            })
            .collect(),
        GrantObjects::AllTablesInSchema { schemas } => {
            for schema in schemas {
                let name = schema_name(schema)?;
                if !name.eq_ignore_ascii_case("public") {
                    return Err(SqlError::InvalidCollection(name));
                }
            }
            Ok(catalog_table_names(db)
                .into_iter()
                .map(|table| (PrivilegeObjectType::Table, table))
                .collect())
        }
        GrantObjects::Sequences(sequences) => sequences
            .iter()
            .map(|sequence| {
                let name = relation_name(sequence)?;
                if load_sequence(db, &name)?.is_none() {
                    return Err(SqlError::InvalidSql(format!(
                        "relation \"{name}\" does not exist"
                    )));
                }
                Ok((PrivilegeObjectType::Sequence, name))
            })
            .collect(),
        GrantObjects::AllSequencesInSchema { schemas } => {
            for schema in schemas {
                let name = schema_name(schema)?;
                if !name.eq_ignore_ascii_case("public") {
                    return Err(SqlError::InvalidCollection(name));
                }
            }
            Ok(list_sequences(db)?
                .into_iter()
                .map(|sequence| (PrivilegeObjectType::Sequence, sequence.name))
                .collect())
        }
        // Overload argument types are accepted but not used for resolution:
        // routines are keyed by bare name (see `routine_key`), so BicDB has
        // no overloading to disambiguate. `relation_name` must not be used
        // here — it hashes non-public schemas into a scoped relation key.
        GrantObjects::Function { name, .. } => {
            let (_, name) = routine_schema_and_name(&object_name(name)?);
            if load_routine(db, RoutineKind::Function, &name)?.is_none() {
                return Err(SqlError::InvalidSql(format!(
                    "function \"{name}\" does not exist"
                )));
            }
            Ok(vec![(PrivilegeObjectType::Function, name)])
        }
        GrantObjects::AllFunctionsInSchema { schemas } => {
            let mut names = Vec::new();
            for schema in schemas {
                let schema = schema_name(schema)?;
                if !is_known_schema(&schema) && load_namespace(db, &schema)?.is_none() {
                    return Err(SqlError::InvalidCollection(schema));
                }
                names.push(schema);
            }
            Ok(list_routines(db)?
                .iter()
                .filter(|routine| routine.kind == RoutineKind::Function)
                .filter(|routine| {
                    let schema = routine_schema_name(routine);
                    names.iter().any(|name| name.eq_ignore_ascii_case(schema))
                })
                .map(|routine| (PrivilegeObjectType::Function, routine.name.clone()))
                .collect())
        }
        other => Err(SqlError::Unsupported(format!(
            "privileges on {other} are not supported"
        ))),
    }
}

pub(crate) fn relation_exists(db: &BicDb, name: &str) -> bool {
    resolve_relation_name_memoized(db, name).is_some()
}

pub(crate) fn resolve_session_relation_name(db: &BicDb, name: &str) -> Result<String> {
    resolve_relation_name_memoized(db, name)
        .ok_or_else(|| SqlError::InvalidCollection(name.to_string()))
}

pub(crate) fn resolve_session_relation_name_if_exists(db: &BicDb, name: &str) -> String {
    resolve_relation_name_memoized(db, name).unwrap_or_else(|| name.to_string())
}

/// PostgreSQL identifier folding happens while extracting an AST ObjectName.
/// Relation lookup is exact so a quoted mixed-case name remains distinct from
/// its unquoted, lowercase spelling.
///
/// The uncached path builds and
/// sorts the ENTIRE catalog name list (cloning every CollectionMeta and loading
/// every view record) per lookup, which ran once or more per statement and
/// profiled at ~5% of transactional-workload CPU. Entries are validated against the schema/view
/// catalog generations plus the raw collection count, so any DDL (including
/// legacy document-API collection creation) invalidates stale names.
pub(crate) struct RelationNameMemoEntry {
    pub(crate) schema_generation: u64,
    pub(crate) view_generation: u64,
    pub(crate) collection_count: usize,
    pub(crate) resolved: Option<String>,
}

thread_local! {
    pub(crate) static RELATION_NAME_MEMO: RefCell<FxHashMap<(usize, String), RelationNameMemoEntry>> =
        RefCell::new(FxHashMap::default());
}

pub(crate) const RELATION_NAME_MEMO_CAP: usize = 1024;

pub(crate) fn resolve_relation_name_memoized(db: &BicDb, name: &str) -> Option<String> {
    // Keyed by the database instance address as well as the name: one thread
    // can serve several BicDb instances (tests, embedded use), and generations
    // alone do not distinguish them. A reused allocation address combined with
    // identical catalog generations AND collection count is the only aliasing
    // risk, and any DDL difference breaks it.
    let db_key = db as *const BicDb as usize;
    let schema_generation = db.collection_generation(SCHEMA_COLLECTION);
    let view_generation = db.collection_generation(VIEW_COLLECTION);
    let collection_count = db.collection_count();
    let key = (db_key, name.to_string());
    let hit = RELATION_NAME_MEMO.with(|memo| {
        memo.borrow().get(&key).and_then(|entry| {
            (entry.schema_generation == schema_generation
                && entry.view_generation == view_generation
                && entry.collection_count == collection_count)
                .then(|| entry.resolved.clone())
        })
    });
    if let Some(resolved) = hit {
        return resolved;
    }
    let resolved = catalog_table_names(db)
        .into_iter()
        .find(|table| table == name);
    RELATION_NAME_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= RELATION_NAME_MEMO_CAP {
            memo.clear();
        }
        memo.insert(
            key,
            RelationNameMemoEntry {
                schema_generation,
                view_generation,
                collection_count,
                resolved: resolved.clone(),
            },
        );
    });
    resolved
}

pub(crate) fn is_known_schema(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "public" | "pg_catalog" | "information_schema"
    )
}

pub(crate) fn schema_name(name: &ObjectName) -> Result<String> {
    object_name(name)
}

pub(crate) fn grantee_role_name(grantee: &Grantee) -> Result<String> {
    match grantee.grantee_type {
        GranteesType::None | GranteesType::Role | GranteesType::User => {}
        GranteesType::Public => return Ok("public".to_string()),
        _ => {
            return Err(SqlError::Unsupported(format!(
                "grantee type {:?} is not supported",
                grantee.grantee_type
            )));
        }
    }
    let Some(name) = grantee.name.as_ref() else {
        return Err(SqlError::InvalidSql("missing grantee name".to_string()));
    };
    match name {
        GranteeName::ObjectName(name) => Ok(normalize_role_name(&object_name(name)?)),
        GranteeName::UserHost { user, .. } => Ok(normalize_role_name(&user.value)),
    }
}

pub(crate) fn role_oid(name: &str) -> i64 {
    if name == current_role_name() {
        return 10;
    }
    let mut hash = 30_000_i64;
    for byte in name.bytes() {
        hash = (hash * 31 + i64::from(byte)) % 1_000_000;
    }
    hash
}

pub(crate) fn schema_acl_value(db: &BicDb, schema: &str) -> Result<SqlValue> {
    let privileges = list_privileges(db)?;
    schema_acl_value_from_privileges(&privileges, schema)
}

pub(crate) fn schema_acl_value_from_privileges(
    privileges: &[PrivilegeGrant],
    schema: &str,
) -> Result<SqlValue> {
    acl_value(
        privileges
            .iter()
            .filter(|grant| {
                grant.object_type == PrivilegeObjectType::Schema && grant.object_name == schema
            })
            .cloned()
            .collect(),
    )
}

pub(crate) fn table_acl_value(db: &BicDb, table: &str) -> Result<SqlValue> {
    let privileges = list_privileges(db)?;
    table_acl_value_from_privileges(&privileges, table)
}

pub(crate) fn table_acl_value_from_privileges(
    privileges: &[PrivilegeGrant],
    table: &str,
) -> Result<SqlValue> {
    acl_value(
        privileges
            .iter()
            .filter(|grant| {
                grant.object_type == PrivilegeObjectType::Table
                    && grant.column.is_none()
                    && grant.object_name == table
            })
            .cloned()
            .collect(),
    )
}

pub(crate) fn user_type_privilege_name(schema_name: &str, name: &str) -> String {
    format!("{schema_name}.{name}")
}

pub(crate) fn type_acl_value_from_privileges(
    privileges: &[PrivilegeGrant],
    user_type: &UserTypeSchema,
) -> Result<SqlValue> {
    if !user_type.acl_explicit {
        return Ok(SqlValue::Null);
    }
    let object_name = user_type_privilege_name(&user_type.schema_name, &user_type.name);
    let grants = privileges
        .iter()
        .filter(|grant| {
            grant.object_type == PrivilegeObjectType::Type && grant.object_name == object_name
        })
        .cloned()
        .collect::<Vec<_>>();
    acl_value_with_grantor(grants, &user_type.owner)
}

pub(crate) fn acl_value(grants: Vec<PrivilegeGrant>) -> Result<SqlValue> {
    acl_value_with_grantor(grants, &current_role_name())
}

pub(crate) fn acl_value_with_grantor(
    grants: Vec<PrivilegeGrant>,
    grantor: &str,
) -> Result<SqlValue> {
    if grants.is_empty() {
        return Ok(SqlValue::Null);
    }
    let mut by_grantee = BTreeMap::<String, BTreeSet<String>>::new();
    for grant in grants {
        by_grantee
            .entry(grant.grantee)
            .or_default()
            .insert(privilege_acl_code(&grant.privilege).to_string());
    }
    let privilege_order = "arwdDxtXUCTc";
    let grantor = pg_quote_ident(grantor);
    let entries = by_grantee
        .into_iter()
        .map(|(grantee, privileges)| {
            let mut privileges = privileges.into_iter().collect::<Vec<_>>();
            privileges.sort_by_key(|privilege| {
                privilege_order
                    .find(privilege.as_str())
                    .unwrap_or(privilege_order.len())
            });
            let grantee = if grantee.eq_ignore_ascii_case("public") {
                String::new()
            } else {
                pg_quote_ident(&grantee)
            };
            format!("{grantee}={}/{grantor}", privileges.concat())
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok(SqlValue::String(format!("{{{entries}}}")))
}

pub(crate) fn privilege_acl_code(privilege: &str) -> &str {
    match privilege.to_ascii_uppercase().as_str() {
        "INSERT" => "a",
        "SELECT" => "r",
        "UPDATE" => "w",
        "DELETE" => "d",
        "TRUNCATE" => "D",
        "REFERENCES" => "x",
        "TRIGGER" => "t",
        "EXECUTE" => "X",
        "USAGE" => "U",
        "CREATE" => "C",
        "TEMPORARY" => "T",
        "CONNECT" => "c",
        _ => privilege,
    }
}

pub(crate) fn information_schema_role_table_grants(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_privileges(db)?
        .into_iter()
        .filter(|grant| grant.object_type == PrivilegeObjectType::Table && grant.column.is_none())
        .map(|grant| {
            virtual_row([
                ("grantor", SqlValue::String(current_role_name())),
                ("grantee", SqlValue::String(grant.grantee)),
                ("table_catalog", SqlValue::String("bicdb".to_string())),
                ("table_schema", SqlValue::String("public".to_string())),
                ("table_name", SqlValue::String(grant.object_name)),
                ("privilege_type", SqlValue::String(grant.privilege)),
                ("is_grantable", SqlValue::String("NO".to_string())),
                ("with_hierarchy", SqlValue::String("NO".to_string())),
            ])
        })
        .collect())
}

pub(crate) fn has_schema_privilege(db: &BicDb, args: &[SqlValue]) -> Result<bool> {
    let (role, schema, privilege) = match args {
        [schema, privilege] => (
            Some(current_role_name()),
            sql_value_text(schema),
            sql_value_text(privilege),
        ),
        [role, schema, privilege] => (
            sql_value_text(role),
            sql_value_text(schema),
            sql_value_text(privilege),
        ),
        _ => {
            return Err(SqlError::InvalidSql(
                "has_schema_privilege expects schema/privilege or role/schema/privilege"
                    .to_string(),
            ));
        }
    };
    let Some(schema) = schema.map(|value| value.to_ascii_lowercase()) else {
        return Ok(false);
    };
    let Some(privilege) = privilege.map(|value| value.to_ascii_uppercase()) else {
        return Ok(false);
    };
    let role = role.unwrap_or_else(current_role_name);
    if role == current_role_name() && is_known_schema(&schema) {
        return Ok(true);
    }
    Ok(list_privileges(db)?.into_iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Schema
            && grant.grantee.eq_ignore_ascii_case(&role)
            && grant.object_name.eq_ignore_ascii_case(&schema)
            && grant.privilege.eq_ignore_ascii_case(&privilege)
    }))
}

pub(crate) fn has_table_privilege(db: &BicDb, args: &[SqlValue]) -> Result<bool> {
    let (role, table, privilege) = match args {
        [table, privilege] => (
            Some(current_role_name()),
            sql_value_text(table),
            sql_value_text(privilege),
        ),
        [role, table, privilege] => (
            sql_value_text(role),
            sql_value_text(table),
            sql_value_text(privilege),
        ),
        _ => {
            return Err(SqlError::InvalidSql(
                "has_table_privilege expects table/privilege or role/table/privilege".to_string(),
            ));
        }
    };
    let Some(table) = table else {
        return Ok(false);
    };
    let Some(privilege) = privilege else {
        return Ok(false);
    };
    let privileges = privilege
        .split(',')
        .map(str::trim)
        .filter(|privilege| !privilege.is_empty())
        .collect::<Vec<_>>();
    if privileges.is_empty() {
        return Ok(false);
    }
    let role = role.unwrap_or_else(current_role_name);
    for privilege in privileges {
        if role_has_table_privilege(db, &role, &table, privilege)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn has_function_privilege(db: &BicDb, args: &[SqlValue]) -> Result<bool> {
    let (role, function, privilege) = match args {
        [function, privilege] => (Some(current_role_name()), function, privilege),
        [role, function, privilege] => (role_name_from_value(db, role)?, function, privilege),
        _ => {
            return Err(SqlError::InvalidSql(
                "has_function_privilege expects function/privilege or role/function/privilege"
                    .to_string(),
            ));
        }
    };
    let Some(role) = role else {
        return Ok(false);
    };
    let Some(privilege) = sql_value_text(privilege) else {
        return Ok(false);
    };
    if !privilege.eq_ignore_ascii_case("EXECUTE") {
        return Err(SqlError::InvalidSql(format!(
            "unrecognized privilege type: {privilege}"
        )));
    }
    let routine = if let Some(oid) = sql_value_i64(function) {
        routine_for_oid(db, oid)?
    } else {
        let Some(identity) = sql_value_text(function) else {
            return Ok(false);
        };
        let name = normalize_object_name(identity.split('(').next().unwrap_or(&identity).trim());
        list_routines(db)?.into_iter().find(|routine| {
            routine.kind == RoutineKind::Function
                && normalize_object_name(&routine.name).eq_ignore_ascii_case(&name)
        })
    };
    let Some(routine) = routine.filter(|routine| routine.kind == RoutineKind::Function) else {
        return Ok(false);
    };
    if routine.owner().eq_ignore_ascii_case(&role) {
        return Ok(true);
    }
    role_can_execute_routine(db, &role, &routine.name)
}

pub(crate) fn has_type_privilege(
    db: &BicDb,
    args: &[SqlValue],
    current_user: &str,
) -> Result<bool> {
    let (role, type_name, privilege) = match args {
        [type_name, privilege] => (current_user.to_string(), type_name, privilege),
        [role, type_name, privilege] => (
            role_name_from_value(db, role)?.unwrap_or_default(),
            type_name,
            privilege,
        ),
        _ => {
            return Err(SqlError::InvalidSql(
                "has_type_privilege expects type/privilege or role/type/privilege".to_string(),
            ));
        }
    };
    let Some(privilege) = sql_value_text(privilege) else {
        return Ok(false);
    };
    if !privilege.eq_ignore_ascii_case("USAGE") {
        return Err(SqlError::InvalidSql(format!(
            "unrecognized privilege type: {privilege}"
        )));
    }
    let user_type = match type_name {
        SqlValue::Int(oid) => load_user_type_by_oid(db, *oid)?,
        _ => {
            let Some(type_name) = sql_value_text(type_name) else {
                return Ok(false);
            };
            let parts = sql_identifier_parts(&type_name)?;
            let (schema_name, name) = match parts.as_slice() {
                [name] => ("public", name.as_str()),
                [schema_name, name] => (schema_name.as_str(), name.as_str()),
                _ => return Ok(false),
            };
            load_user_type(db, &schema_name, &name)?
        }
    };
    let Some(user_type) = user_type else {
        return Ok(false);
    };
    role_can_use_user_type(db, &role, &user_type)
}

pub(crate) fn role_can_use_user_type(
    db: &BicDb,
    role: &str,
    user_type: &UserTypeSchema,
) -> Result<bool> {
    let role = normalize_role_name(role);
    if role == BOOTSTRAP_ROLE_NAME
        || load_role_schema(db, &role)?.is_some_and(|role| role.superuser)
        || user_type.owner.eq_ignore_ascii_case(&role)
    {
        return Ok(true);
    }
    if !user_type.acl_explicit {
        return Ok(true);
    }
    let inherited = role_privilege_closure(db, &role)?;
    if inherited.contains(&normalize_role_name(&user_type.owner)) {
        return Ok(true);
    }
    let object_name = user_type_privilege_name(&user_type.schema_name, &user_type.name);
    Ok(list_privileges(db)?.into_iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Type
            && grant.object_name == object_name
            && grant.privilege.eq_ignore_ascii_case("USAGE")
            && (grant.grantee.eq_ignore_ascii_case("public")
                || inherited.contains(&normalize_role_name(&grant.grantee)))
    }))
}

pub(crate) fn pg_has_role(db: &BicDb, args: &[SqlValue], current_user: &str) -> Result<bool> {
    let (user, role, privilege) = match args {
        [role, privilege] => (Some(current_user.to_string()), role, privilege),
        [user, role, privilege] => (role_name_from_value(db, user)?, role, privilege),
        _ => {
            return Err(SqlError::InvalidSql(
                "pg_has_role expects role/privilege or user/role/privilege".to_string(),
            ));
        }
    };
    let Some(user) = user else {
        return Ok(false);
    };
    let Some(role) = role_name_from_value(db, role)? else {
        return Ok(false);
    };
    let Some(privilege) = sql_value_text(privilege) else {
        return Ok(false);
    };
    if load_role_schema(db, &user)?.is_some_and(|role| role.superuser) {
        return Ok(true);
    }
    match privilege.to_ascii_uppercase().as_str() {
        "MEMBER" => Ok(membership_closure(db, &user, false)?.contains(&role)),
        "SET" => Ok(settable_role_closure(db, &user)?.contains(&role)),
        "USAGE" => Ok(role_privilege_closure(db, &user)?.contains(&role)),
        privilege => Err(SqlError::InvalidSql(format!(
            "unrecognized privilege type: {privilege}"
        ))),
    }
}

pub(crate) fn role_name_from_value(db: &BicDb, value: &SqlValue) -> Result<Option<String>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    if let Some(oid) = sql_value_i64(value) {
        return Ok(list_roles(db)?
            .into_iter()
            .find(|role| role_oid(&role.name) == oid)
            .map(|role| normalize_role_name(&role.name)));
    }
    Ok(sql_value_text(value).map(|role| normalize_role_name(&role)))
}

/// Per-thread memo of `(role, table, privilege)` verdicts, validated against
/// every catalog the decision reads (roles, table schemas, views, grants,
/// memberships). Every INSERT/UPDATE/DELETE and every RETURNING clause asks
/// this per statement; uncached it resolves the relation, loads the role,
/// loads the schema and, for non-owners, scans every grant and membership.
pub(crate) fn role_has_table_privilege(
    db: &BicDb,
    role: &str,
    table: &str,
    privilege: &str,
) -> Result<bool> {
    let generations = [
        db.collection_generation(ROLE_COLLECTION),
        db.collection_generation(SCHEMA_COLLECTION),
        db.collection_generation(VIEW_COLLECTION),
        db.collection_generation(PRIVILEGE_COLLECTION),
        db.collection_generation(ROLE_MEMBERSHIP_COLLECTION),
    ];
    let key = (
        db.instance_id(),
        normalize_role_name(role),
        table.to_string(),
        privilege.to_ascii_uppercase(),
    );
    let hit = TABLE_PRIVILEGE_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generations)
            .map(|(_, allowed)| *allowed)
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let allowed = role_has_table_privilege_uncached(db, &key.1, table, privilege)?;
    TABLE_PRIVILEGE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= AUTHZ_MEMO_MAX_ENTRIES {
            memo.clear();
        }
        memo.insert(key, (generations, allowed));
    });
    Ok(allowed)
}

thread_local! {
    static TABLE_PRIVILEGE_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<(u64, String, String, String), ([u64; 5], bool)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

fn role_has_table_privilege_uncached(
    db: &BicDb,
    role: &str,
    table: &str,
    privilege: &str,
) -> Result<bool> {
    let role = normalize_role_name(role);
    let table = if relation_exists(db, table) {
        table.to_string()
    } else {
        let parts = sql_identifier_parts(table)?;
        relation_name_from_parts(&parts)?
    };
    if role == BOOTSTRAP_ROLE_NAME
        || load_role_schema(db, &role)?.is_some_and(|role| role.superuser)
    {
        return Ok(relation_exists(db, &table));
    }
    let owner = if let Some(schema) = load_schema_shared(db, &table)? {
        schema.owner.clone()
    } else {
        load_view(db, &table)?.and_then(|view| view.owner)
    }
    .unwrap_or_else(current_role_name);
    if owner.eq_ignore_ascii_case(&role) {
        return Ok(true);
    }
    let inherited = role_privilege_closure(db, &role)?;
    if inherited.contains(&normalize_role_name(&owner)) {
        return Ok(true);
    }
    Ok(list_privileges(db)?.into_iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Table
            && grant.column.is_none()
            && grant.object_name == table
            && grant.privilege.eq_ignore_ascii_case(privilege)
            && (grant.grantee.eq_ignore_ascii_case("public")
                || inherited.contains(&normalize_role_name(&grant.grantee)))
    }))
}

/// EXECUTE decision for `(role, routine)`, memoized per thread and validated
/// against the generations of every catalog it reads. Before this, each
/// stored-function call re-read the role, re-read the routine (twice when
/// the name is not a function) and, for non-owners, re-scanned every grant
/// and membership.
pub(crate) fn role_can_execute_routine(db: &BicDb, role: &str, routine: &str) -> Result<bool> {
    let generations = [
        db.collection_generation(ROLE_COLLECTION),
        db.collection_generation(ROUTINE_COLLECTION),
        db.collection_generation(PRIVILEGE_COLLECTION),
        db.collection_generation(ROLE_MEMBERSHIP_COLLECTION),
    ];
    let key = (
        db as *const BicDb as usize,
        normalize_role_name(role),
        normalize_object_name(routine),
    );
    let hit = ROUTINE_EXECUTE_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generations)
            .map(|(_, allowed)| *allowed)
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let allowed = role_can_execute_routine_uncached(db, &key.1, routine)?;
    ROUTINE_EXECUTE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= AUTHZ_MEMO_MAX_ENTRIES {
            memo.clear();
        }
        memo.insert(key, (generations, allowed));
    });
    Ok(allowed)
}

fn role_can_execute_routine_uncached(db: &BicDb, role: &str, routine: &str) -> Result<bool> {
    let role = normalize_role_name(role);
    if role == BOOTSTRAP_ROLE_NAME
        || role_schema_shared(db, &role)?.is_some_and(|role| role.superuser)
    {
        return Ok(true);
    }
    let routine_schema = match load_routine(db, RoutineKind::Function, routine)? {
        Some(routine) => Some(routine),
        None => load_routine(db, RoutineKind::Procedure, routine)?,
    };
    let inherited = role_privilege_closure(db, &role)?;
    if routine_schema
        .is_some_and(|routine| inherited.contains(&normalize_role_name(routine.owner())))
    {
        return Ok(true);
    }
    let routine = normalize_object_name(routine);
    Ok(list_privileges(db)?.into_iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Function
            && grant.object_name.eq_ignore_ascii_case(&routine)
            && grant.privilege.eq_ignore_ascii_case("EXECUTE")
            && (grant.grantee.eq_ignore_ascii_case("public")
                || inherited.contains(&normalize_role_name(&grant.grantee)))
    }))
}

pub(crate) fn role_can_use_sequence(
    db: &BicDb,
    role: &str,
    sequence: &SequenceSchema,
    privileges: &[&str],
) -> Result<bool> {
    let role = normalize_role_name(role);
    if role == BOOTSTRAP_ROLE_NAME
        || sequence.owner.eq_ignore_ascii_case(&role)
        || load_role_schema(db, &role)?.is_some_and(|role| role.superuser)
    {
        return Ok(true);
    }
    let inherited = role_privilege_closure(db, &role)?;
    if inherited.contains(&normalize_role_name(&sequence.owner)) {
        return Ok(true);
    }
    let sequence_name = &sequence.name;
    Ok(list_privileges(db)?.into_iter().any(|grant| {
        grant.object_type == PrivilegeObjectType::Sequence
            && grant.object_name == *sequence_name
            && privileges
                .iter()
                .any(|privilege| grant.privilege.eq_ignore_ascii_case(privilege))
            && (grant.grantee.eq_ignore_ascii_case("public")
                || inherited.contains(&normalize_role_name(&grant.grantee)))
    }))
}

pub(crate) fn apply_carrier_function_dependency_grants(
    db: &mut BicDb,
    runtime_role: &str,
) -> Result<()> {
    let runtime_role = normalize_role_name(runtime_role);
    let mut patterns = HashMap::<String, regex::Regex>::new();
    ensure_known_role(&list_roles(db)?, &runtime_role)?;
    let routines = list_routines(db)?;
    let schemas = list_schemas(db)?
        .into_iter()
        .filter(|schema| matches!(schema.schema_name.as_str(), "public" | "carrier_private"))
        .collect::<Vec<_>>();
    let views = list_views(db)?;

    let mut function_work = Vec::<(String, String)>::new();
    let mut function_seen = BTreeSet::<(String, String)>::new();
    for routine in &routines {
        if role_can_execute_routine(db, &runtime_role, &routine.name)? {
            enqueue_function_dependency(
                &mut function_work,
                &mut function_seen,
                &routine.name,
                &runtime_role,
            );
        }
    }

    for trigger in list_triggers(db)? {
        let privilege = match trigger.event.to_ascii_uppercase().as_str() {
            "INSERT" => "INSERT",
            "UPDATE" => "UPDATE",
            "DELETE" => "DELETE",
            _ => continue,
        };
        if role_has_table_privilege(db, &runtime_role, &trigger.table_name, privilege)? {
            enqueue_function_dependency(
                &mut function_work,
                &mut function_seen,
                &trigger.function_name,
                &runtime_role,
            );
        }
    }

    for schema in &schemas {
        let policy_is_reachable = ["SELECT", "INSERT", "UPDATE", "DELETE"]
            .into_iter()
            .map(|privilege| role_has_table_privilege(db, &runtime_role, &schema.name, privilege))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|allowed| allowed);
        if !policy_is_reachable {
            continue;
        }
        for policy in &schema.policies {
            let source = format!(
                "{} {}",
                policy.using_expr.as_deref().unwrap_or_default(),
                policy.check_expr.as_deref().unwrap_or_default()
            );
            for routine in &routines {
                if sql_mentions_routine(&mut patterns, &source, &routine.name)? {
                    save_runtime_privilege(
                        db,
                        PrivilegeObjectType::Function,
                        &routine.name,
                        &runtime_role,
                        "EXECUTE",
                    )?;
                    enqueue_function_dependency(
                        &mut function_work,
                        &mut function_seen,
                        &routine.name,
                        &runtime_role,
                    );
                }
            }
        }
    }

    let mut relation_work = Vec::<(String, String, String)>::new();
    let mut relation_seen = BTreeSet::<(String, String, String)>::new();
    for view in &views {
        for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            if role_has_table_privilege(db, &runtime_role, &view.name, privilege)? {
                enqueue_relation_dependency(
                    &mut relation_work,
                    &mut relation_seen,
                    &view.name,
                    &runtime_role,
                    privilege,
                );
            }
        }
    }

    let mut function_cursor = 0;
    while function_cursor < function_work.len() {
        let (caller_name, caller_executor) = function_work[function_cursor].clone();
        function_cursor += 1;
        let Some(caller) = routines
            .iter()
            .find(|routine| routine.name.eq_ignore_ascii_case(&caller_name))
        else {
            continue;
        };
        if !matches!(caller.language.as_str(), "sql" | "plpgsql") {
            continue;
        }
        // Dependency analysis mirrors execution: a definer routine with no
        // recorded owner runs as the invoker, so its dependencies resolve
        // against the caller too.
        let dependency_executor = match (caller.security_definer, caller.definer_owner()) {
            (true, Some(owner)) => owner,
            _ => caller_executor.as_str(),
        };

        for candidate in &routines {
            if candidate.name.eq_ignore_ascii_case(&caller.name)
                || !sql_mentions_routine(&mut patterns, &caller.definition, &candidate.name)?
            {
                continue;
            }
            save_runtime_privilege(
                db,
                PrivilegeObjectType::Schema,
                routine_schema_name(candidate),
                dependency_executor,
                "USAGE",
            )?;
            save_runtime_privilege(
                db,
                PrivilegeObjectType::Function,
                &candidate.name,
                dependency_executor,
                "EXECUTE",
            )?;
            enqueue_function_dependency(
                &mut function_work,
                &mut function_seen,
                &candidate.name,
                dependency_executor,
            );
        }

        for schema in &schemas {
            for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
                if !sql_mentions_relation_operation(
                    &mut patterns,
                    &caller.definition,
                    &schema.name,
                    privilege,
                )? {
                    continue;
                }
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Schema,
                    &schema.schema_name,
                    dependency_executor,
                    "USAGE",
                )?;
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Table,
                    &schema.name,
                    dependency_executor,
                    privilege,
                )?;
            }
        }
        for view in &views {
            for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
                if !sql_mentions_relation_operation(
                    &mut patterns,
                    &caller.definition,
                    &view.name,
                    privilege,
                )? {
                    continue;
                }
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Schema,
                    "public",
                    dependency_executor,
                    "USAGE",
                )?;
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Table,
                    &view.name,
                    dependency_executor,
                    privilege,
                )?;
                enqueue_relation_dependency(
                    &mut relation_work,
                    &mut relation_seen,
                    &view.name,
                    dependency_executor,
                    privilege,
                );
            }
        }
    }

    let mut relation_cursor = 0;
    while relation_cursor < relation_work.len() {
        let (view_name, caller_executor, privilege) = relation_work[relation_cursor].clone();
        relation_cursor += 1;
        let Some(view) = views
            .iter()
            .find(|view| view.name.eq_ignore_ascii_case(&view_name))
        else {
            continue;
        };
        let dependency_executor = if view.security_invoker {
            caller_executor.as_str()
        } else {
            view.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME)
        };
        for schema in &schemas {
            if sql_mentions_relation_operation(
                &mut patterns,
                &view.query_sql,
                &schema.name,
                &privilege,
            )? {
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Schema,
                    &schema.schema_name,
                    dependency_executor,
                    "USAGE",
                )?;
                save_runtime_privilege(
                    db,
                    PrivilegeObjectType::Table,
                    &schema.name,
                    dependency_executor,
                    &privilege,
                )?;
            }
        }
        for candidate in &views {
            if candidate.name.eq_ignore_ascii_case(&view.name)
                || !sql_mentions_relation_operation(
                    &mut patterns,
                    &view.query_sql,
                    &candidate.name,
                    &privilege,
                )?
            {
                continue;
            }
            save_runtime_privilege(
                db,
                PrivilegeObjectType::Table,
                &candidate.name,
                dependency_executor,
                &privilege,
            )?;
            enqueue_relation_dependency(
                &mut relation_work,
                &mut relation_seen,
                &candidate.name,
                dependency_executor,
                &privilege,
            );
        }
    }
    Ok(())
}

fn enqueue_function_dependency(
    work: &mut Vec<(String, String)>,
    seen: &mut BTreeSet<(String, String)>,
    routine: &str,
    executor: &str,
) {
    let item = (
        normalize_object_name(routine),
        normalize_role_name(executor),
    );
    if seen.insert(item.clone()) {
        work.push(item);
    }
}

fn enqueue_relation_dependency(
    work: &mut Vec<(String, String, String)>,
    seen: &mut BTreeSet<(String, String, String)>,
    relation: &str,
    executor: &str,
    privilege: &str,
) {
    let item = (
        unqualified_relation(relation),
        normalize_role_name(executor),
        privilege.to_ascii_uppercase(),
    );
    if seen.insert(item.clone()) {
        work.push(item);
    }
}

fn save_runtime_privilege(
    db: &mut BicDb,
    object_type: PrivilegeObjectType,
    object_name: &str,
    grantee: &str,
    privilege: &str,
) -> Result<()> {
    save_privilege(
        db,
        &PrivilegeGrant {
            column: None,
            object_type,
            object_name: object_name
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(object_name)
                .trim_matches('"')
                .to_string(),
            grantee: normalize_role_name(grantee),
            privilege: privilege.to_ascii_uppercase(),
        },
    )
}

pub(crate) fn routine_schema_name(routine: &RoutineSchema) -> &str {
    if !routine.schema.is_empty() {
        return &routine.schema;
    }
    // Records written before routines carried their schema: recover it from
    // the definition text as before.
    let lower = routine.definition.to_ascii_lowercase();
    if lower.contains("carrier_private.") {
        "carrier_private"
    } else {
        "public"
    }
}

/// Split a possibly schema-qualified routine name into its schema
/// (defaulting to `public`) and its bare name. Every routine store/lookup
/// path keys on the bare name, so the schema travels as data.
pub(crate) fn routine_schema_and_name(name: &str) -> (String, String) {
    let bare = normalize_object_name(name);
    let schema = match name.rsplit_once('.') {
        Some((schema, _)) if !schema.is_empty() => normalize_object_name(schema),
        _ => "public".to_string(),
    };
    (schema, bare)
}

fn sql_mentions_routine(
    patterns: &mut HashMap<String, regex::Regex>,
    sql: &str,
    routine: &str,
) -> Result<bool> {
    let name = regex::escape(&unqualified_relation(routine));
    reviewed_dependency_regex(
        patterns,
        &format!(r"(^|[^[:alnum:]_$])((public|carrier_private)[.])?{name}[[:space:]]*[(]"),
        sql,
    )
}

fn sql_mentions_relation_operation(
    patterns: &mut HashMap<String, regex::Regex>,
    sql: &str,
    relation: &str,
    privilege: &str,
) -> Result<bool> {
    let relation = regex::escape(&unqualified_relation(relation));
    let prefix = match privilege.to_ascii_uppercase().as_str() {
        "SELECT" => r"(FROM|JOIN)[[:space:]]+(ONLY[[:space:]]+|LATERAL[[:space:]]+)*",
        "INSERT" => r"INSERT[[:space:]]+INTO[[:space:]]+(ONLY[[:space:]]+)*",
        "UPDATE" => r"UPDATE[[:space:]]+(ONLY[[:space:]]+)*",
        "DELETE" => r"DELETE[[:space:]]+FROM[[:space:]]+(ONLY[[:space:]]+)*",
        _ => return Ok(false),
    };
    reviewed_dependency_regex(
        patterns,
        &format!(
            r"(^|[^[:alnum:]_$]){prefix}((public|carrier_private)[.])?{relation}([^[:alnum:]_$]|$)"
        ),
        sql,
    )
}

// Keep compiled patterns only for this provisioning operation. The bound
// prevents large catalogs from creating an unbounded cache.
fn reviewed_dependency_regex(
    patterns: &mut HashMap<String, regex::Regex>,
    pattern: &str,
    sql: &str,
) -> Result<bool> {
    if let Some(compiled) = patterns.get(pattern) {
        return Ok(compiled.is_match(sql));
    }
    let compiled = RegexBuilder::new(pattern)
        .case_insensitive(true)
        .build()
        .map_err(|error| SqlError::InvalidSql(format!("invalid dependency pattern: {error}")))?;
    let matches = compiled.is_match(sql);
    if patterns.len() >= 4096 {
        patterns.clear();
    }
    patterns.insert(pattern.to_owned(), compiled);
    Ok(matches)
}

pub(crate) fn sql_value_text(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::String(value) => Some(value.clone()),
        SqlValue::Int(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(crate) fn unqualified_relation(name: &str) -> String {
    name.trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

pub(crate) fn load_view(db: &BicDb, view: &str) -> Result<Option<ViewSchema>> {
    match db.get(VIEW_COLLECTION, view) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) => Ok(None),
        Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_views(db: &BicDb) -> Result<Vec<ViewSchema>> {
    let records = match db.scan_collection(VIEW_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut views = records
        .into_iter()
        .map(|record| serde_json::from_value::<ViewSchema>(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<_>>>()?;
    views.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(views)
}

pub(crate) fn save_view(db: &mut BicDb, view: &ViewSchema) -> Result<()> {
    ensure_view_collection(db)?;
    db.insert(
        VIEW_COLLECTION,
        Record::new(&view.name).with_metadata(serde_json::to_value(view)?),
    )?;
    Ok(())
}

pub(crate) fn delete_view(db: &mut BicDb, view: &str) -> Result<bool> {
    match db.delete(VIEW_COLLECTION, view) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn routine_key(kind: RoutineKind, name: &str) -> String {
    let prefix = match kind {
        RoutineKind::Function => "f",
        RoutineKind::Procedure => "p",
    };
    format!("{prefix}:{}", normalize_object_name(name))
}

pub(crate) fn load_routine(
    db: &BicDb,
    kind: RoutineKind,
    name: &str,
) -> Result<Option<RoutineSchema>> {
    match db.get(ROUTINE_COLLECTION, &routine_key(kind, name)) {
        Ok(Some(record)) => Ok(Some(
            serde_json::from_value::<RoutineSchema>(record.metadata.clone())?
                .with_legacy_security_metadata(),
        )),
        Ok(None) => Ok(None),
        Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// A routine resolved once: its deserialized schema plus its compiled IR. Both
/// are shared (`Arc`) so callers clone cheaply.
#[derive(Clone)]
pub(crate) struct CachedRoutine {
    pub(crate) schema: Arc<RoutineSchema>,
    pub(crate) ir: Arc<RoutineIR>,
}

thread_local! {
    /// Caches the deserialized schema AND compiled IR per `(kind, lowercased
    /// name)`, tagged with the routine collection's generation so it is
    /// invalidated on any routine DDL (create/replace/drop). Without this, every
    /// stored procedure/function call re-fetched and re-deserialized the entire
    /// routine definition from JSON and re-hashed it to look up the IR — the
    /// dominant per-call allocation/hashing cost under procedure-heavy workloads
    /// (e.g. procedure-heavy workloads). Cached `None` records a confirmed absence so repeated calls
    /// to an undefined routine stay cheap.
    pub(crate) static ROUTINE_CACHE: RefCell<FxHashMap<String, RoutineCacheEntry>> =
        RefCell::new(FxHashMap::default());
}

/// Resolves a routine to its shared schema + compiled IR, caching the result and
/// invalidating it the moment the routine collection changes. Safe for any
/// workload; avoids a per-call JSON deserialize and definition re-hash.
pub(crate) fn resolve_routine_cached(
    db: &BicDb,
    kind: RoutineKind,
    name: &str,
) -> Result<Option<CachedRoutine>> {
    let generation = db.collection_generation(ROUTINE_COLLECTION);
    // Borrow-key lookup: the cache is keyed by the lowercase name alone (one
    // entry holding both kinds), so the hot miss/hit path allocates nothing —
    // the old `(kind, name.to_ascii_lowercase())` tuple key paid a String per
    // probe, and this resolver runs last in the function-eval chain for every
    // builtin call.
    let lowered: Cow<'_, str> = if name.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(name.to_ascii_lowercase())
    } else {
        Cow::Borrowed(name)
    };
    if let Some(cached) = ROUTINE_CACHE.with(|cache| {
        cache.borrow().get(lowered.as_ref()).and_then(|entry| {
            if entry.generation != generation {
                return None;
            }
            match kind {
                RoutineKind::Function => entry.function.clone(),
                RoutineKind::Procedure => entry.procedure.clone(),
            }
        })
    }) {
        return Ok(cached);
    }
    let resolved = match load_routine(db, kind, name)? {
        Some(schema) => {
            let schema = Arc::new(schema);
            let ir = compile_cached_routine_ir(&schema)?;
            Some(CachedRoutine { schema, ir })
        }
        None => None,
    };
    ROUTINE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let entry = cache
            .entry(lowered.into_owned())
            .or_insert_with(|| RoutineCacheEntry {
                generation,
                function: None,
                procedure: None,
            });
        if entry.generation != generation {
            entry.generation = generation;
            entry.function = None;
            entry.procedure = None;
        }
        match kind {
            RoutineKind::Function => entry.function = Some(resolved.clone()),
            RoutineKind::Procedure => entry.procedure = Some(resolved.clone()),
        }
    });
    Ok(resolved)
}

/// Per-name routine cache entry; `function`/`procedure` are `Some(resolution)`
/// once that kind has been looked up under `generation` (inner `None` = the
/// routine does not exist — negative results are cached too).
pub(crate) struct RoutineCacheEntry {
    pub(crate) generation: u64,
    pub(crate) function: Option<Option<CachedRoutine>>,
    pub(crate) procedure: Option<Option<CachedRoutine>>,
}

impl RoutineFrame {
    #[allow(dead_code)]
    pub(crate) fn new(params: &[RoutineParam], args: &[SqlValue]) -> Result<Self> {
        let symbol_names = routine_symbol_names_for_params(params);
        Self::new_with_symbols(params, args, &symbol_names)
    }

    pub(crate) fn new_with_symbols(
        params: &[RoutineParam],
        args: &[SqlValue],
        symbol_names: &[String],
    ) -> Result<Self> {
        let template = RoutineFrameTemplate::build(params, symbol_names);
        Self::from_template(&template, args)
    }

    /// A frame for one call, from the routine's shared template.
    pub(crate) fn from_template(
        template: &RoutineFrameTemplate,
        args: &[SqlValue],
    ) -> Result<Self> {
        let slot_count = template.slot_names.len();
        let mut frame = Self {
            assignment_types: Arc::clone(&template.assignment_types),
            values: Arc::new(BTreeMap::new()),
            slot_values: std::rc::Rc::new(vec![SqlValue::Null; slot_count]),
            slot_names: Arc::clone(&template.slot_names),
            slot_ids: Arc::clone(&template.slot_ids),
            values_dirty: false,
            dirty_slots: vec![false; slot_count],
            full_resync: true,
            positional: args.to_vec(),
            output_names: Vec::new(),
            cursors: BTreeMap::new(),
            returned_set: None,
        };

        let mut input_idx = 0usize;
        for param in &template.params {
            let value = match param.mode {
                RoutineArgMode::In | RoutineArgMode::InOut => {
                    let value = args
                        .get(input_idx)
                        .cloned()
                        .map(Ok)
                        .or_else(|| param.default_expr.as_ref().map(eval_constant_expr))
                        .unwrap_or_else(|| {
                            Err(SqlError::InvalidSql(format!(
                                "stored routine argument {} is missing",
                                input_idx + 1
                            )))
                        })?;
                    if input_idx >= frame.positional.len() {
                        frame.positional.push(value.clone());
                    }
                    input_idx += 1;
                    value
                }
                RoutineArgMode::Out => SqlValue::Null,
            };
            match &param.name {
                Some(name) => {
                    frame.set_known(&param.positional_key, value.clone());
                    frame.set(name, value);
                }
                None => frame.set_known(&param.positional_key, value),
            }
        }
        frame.output_names = template.output_names.clone();
        Ok(frame)
    }

    /// User-visible assignment checks the declared type before changing a slot.
    /// Internal values such as FOUND continue to use the raw frame setters.
    pub(crate) fn assign(&mut self, name: &str, value: SqlValue) -> Result<()> {
        let key = normalized_object_name_ref(name);
        let value = match self.assignment_types.get(key.as_ref()) {
            Some(schema) => coerce_routine_assignment(value, schema)?,
            None => value,
        };
        self.set(name, value);
        Ok(())
    }

    /// `set` for a key that is already normalized (template keys).
    fn set_known(&mut self, key: &str, value: SqlValue) {
        if let Some(id) = self.slot_ids.get(key).copied() {
            if let Some(slot) = std::rc::Rc::make_mut(&mut self.slot_values).get_mut(id.0) {
                *slot = value;
                if let Some(dirty) = self.dirty_slots.get_mut(id.0) {
                    *dirty = true;
                }
                self.values_dirty = true;
                return;
            }
        }
        self.set(key, value);
    }

    pub(crate) fn set(&mut self, name: &str, value: SqlValue) {
        let key = normalized_object_name_ref(name);
        // Inline slot write (not set_slot_by_key) so the hot path moves the
        // value instead of cloning it for a fallback that rarely runs.
        if let Some(id) = self.slot_ids.get(key.as_ref()).copied() {
            if let Some(slot) = std::rc::Rc::make_mut(&mut self.slot_values).get_mut(id.0) {
                *slot = value;
                if let Some(dirty) = self.dirty_slots.get_mut(id.0) {
                    *dirty = true;
                }
                self.values_dirty = true;
                return;
            }
        }
        self.sync_values();
        #[cfg(test)]
        SQL_ROUTINE_FRAME_MAP_SYNCS.with(|syncs| *syncs.borrow_mut() += 1);
        Arc::make_mut(&mut self.values).insert(key.into_owned(), value);
    }

    pub(crate) fn set_slot_by_key(&mut self, key: &str, value: SqlValue) -> bool {
        if let Some(id) = self.slot_ids.get(key).copied() {
            if let Some(slot) = std::rc::Rc::make_mut(&mut self.slot_values).get_mut(id.0) {
                *slot = value;
                if let Some(dirty) = self.dirty_slots.get_mut(id.0) {
                    *dirty = true;
                }
                return true;
            }
        }
        false
    }

    pub(crate) fn slot_values(&self) -> &[SqlValue] {
        &self.slot_values
    }

    /// The frame's slot layout and current values, shared with the engine for
    /// the duration of an embedded statement: bound expressions resolve
    /// routine variables to slot ids and read the values in place, instead
    /// of the per-statement snapshot of the string-keyed map.
    pub(crate) fn slot_binding(&self) -> crate::engine::RoutineSlotBinding {
        crate::engine::RoutineSlotBinding {
            ids: Arc::clone(&self.slot_ids),
            values: std::rc::Rc::clone(&self.slot_values),
        }
    }

    pub(crate) fn slot_value_by_key(&self, key: &str) -> Option<SqlValue> {
        let value = self
            .slot_ids
            .get(key)
            .and_then(|id| self.slot_values.get(id.0))
            .cloned();
        #[cfg(test)]
        if matches!(value, Some(SqlValue::Json(JsonValue::Array(_)))) {
            SQL_ROUTINE_SLOT_ARRAY_CLONES.with(|clones| *clones.borrow_mut() += 1);
        }
        value
    }

    pub(crate) fn sync_values(&mut self) {
        if !self.values_dirty {
            return;
        }
        #[cfg(test)]
        SQL_ROUTINE_FRAME_MAP_SYNCS.with(|syncs| *syncs.borrow_mut() += 1);
        let values = Arc::make_mut(&mut self.values);
        if self.full_resync {
            // First materialization: populate every slot so the string-keyed
            // map mirrors the slots, then switch to incremental updates.
            for (key, value) in self.slot_names.iter().zip(self.slot_values.iter()) {
                values.insert(key.clone(), value.clone());
            }
            self.full_resync = false;
            for dirty in self.dirty_slots.iter_mut() {
                *dirty = false;
            }
        } else {
            for (id, dirty) in self.dirty_slots.iter_mut().enumerate() {
                if *dirty {
                    values.insert(self.slot_names[id].clone(), self.slot_values[id].clone());
                    *dirty = false;
                }
            }
        }
        self.values_dirty = false;
    }

    pub(crate) fn materialized_values(&mut self) -> Arc<BTreeMap<String, SqlValue>> {
        self.sync_values();
        self.values.clone()
    }

    pub(crate) fn set_array_element(
        &mut self,
        name: &str,
        offset: usize,
        value: SqlValue,
    ) -> Result<()> {
        let key = normalized_object_name_ref(name);
        let value = match self.assignment_types.get(key.as_ref()) {
            Some(schema) => {
                let element_type = schema.pg_type.strip_suffix("[]").ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "cannot assign array element on non-array routine variable {name}"
                    ))
                })?;
                let value = cast_value_to_pg_type_fast(value, element_type)?;
                apply_pg_type_modifier_with_context(
                    value,
                    element_type,
                    schema.type_modifier.as_ref(),
                    false,
                )?
            }
            None => value,
        };
        if let Some(id) = self.slot_ids.get(key.as_ref()).copied() {
            if let Some(slot) = std::rc::Rc::make_mut(&mut self.slot_values).get_mut(id.0) {
                routine_array_set_value(name, slot, offset, value)?;
                self.values_dirty = true;
                if let Some(dirty) = self.dirty_slots.get_mut(id.0) {
                    *dirty = true;
                }
                return Ok(());
            }
        }
        self.sync_values();
        #[cfg(test)]
        SQL_ROUTINE_FRAME_MAP_SYNCS.with(|syncs| *syncs.borrow_mut() += 1);
        let values = Arc::make_mut(&mut self.values);
        let slot = values.entry(key.into_owned()).or_insert(SqlValue::Null);
        routine_array_set_value(name, slot, offset, value)?;
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> SqlValue {
        let key = normalized_object_name_ref(name);
        self.slot_value_by_key(&key)
            .or_else(|| routine_var_from_name(&self.values, name))
            .unwrap_or(SqlValue::Null)
    }
}

impl RoutineFrameTemplate {
    pub(crate) fn build(params: &[RoutineParam], symbol_names: &[String]) -> Self {
        Self::build_with_declarations(params, symbol_names, &[])
    }

    pub(crate) fn build_with_declarations(
        params: &[RoutineParam],
        symbol_names: &[String],
        declarations: &[RoutineDecl],
    ) -> Self {
        let mut assignment_types = FxHashMap::default();
        for param in params {
            if let Some(schema) = &param.type_schema {
                assignment_types.insert(format!("${}", param.index + 1), schema.clone());
                if let Some(name) = &param.name {
                    assignment_types.insert(normalize_object_name(name), schema.clone());
                }
            }
        }
        for declaration in declarations {
            match declaration {
                RoutineDecl::Variable {
                    name,
                    pg_type: Some(pg_type),
                    type_modifier,
                    ..
                } => {
                    assignment_types.insert(
                        normalize_object_name(name),
                        RoutineTypeSchema {
                            pg_type: pg_type.clone(),
                            type_modifier: type_modifier.clone(),
                        },
                    );
                }
                RoutineDecl::Alias { name, position } => {
                    if let Some(schema) = assignment_types.get(&format!("${position}")).cloned() {
                        assignment_types.insert(normalize_object_name(name), schema);
                    }
                }
                _ => {}
            }
        }
        let mut slot_ids = FxHashMap::default();
        let mut slot_names = Vec::new();
        for name in symbol_names
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(ROUTINE_FOUND_VAR))
        {
            let key = normalize_object_name(name);
            if key.is_empty() || slot_ids.contains_key(&key) {
                continue;
            }
            slot_ids.insert(key.clone(), VarId(slot_names.len()));
            slot_names.push(key);
        }
        let mut output_names = Vec::new();
        let mut template_params = Vec::with_capacity(params.len());
        for param in params {
            if matches!(param.mode, RoutineArgMode::Out | RoutineArgMode::InOut) {
                let name = param
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("arg{}", param.index + 1));
                output_names.push(normalize_object_name(&name));
            }
            template_params.push(RoutineFrameTemplateParam {
                positional_key: format!("${}", param.index + 1),
                name: param.name.clone(),
                mode: param.mode,
                default_expr: param.default_expr.clone(),
            });
        }
        Self {
            assignment_types: Arc::new(assignment_types),
            slot_names: Arc::new(slot_names),
            slot_ids: Arc::new(slot_ids),
            params: template_params,
            output_names,
        }
    }
}

fn coerce_routine_assignment(value: SqlValue, schema: &RoutineTypeSchema) -> Result<SqlValue> {
    if matches!(schema.pg_type.as_str(), "record" | "trigger" | "void") {
        return Ok(value);
    }
    let value = cast_value_to_pg_type_fast(value, &schema.pg_type)?;
    apply_pg_type_modifier_with_context(
        value,
        &schema.pg_type,
        schema.type_modifier.as_ref(),
        false,
    )
}

pub(crate) fn routine_array_set_value(
    name: &str,
    slot: &mut SqlValue,
    offset: usize,
    value: SqlValue,
) -> Result<()> {
    let current = std::mem::replace(slot, SqlValue::Null);
    let mut array = match current {
        SqlValue::Null => Vec::new(),
        SqlValue::Json(JsonValue::Array(values)) => values,
        SqlValue::String(text) => {
            let Some(values) = pg_int_vector_values(&text) else {
                *slot = SqlValue::String(text);
                return Err(SqlError::InvalidSql(format!(
                    "cannot assign array element on non-array routine variable {name}"
                )));
            };
            values.into_iter().map(sql_value_to_json).collect()
        }
        other => {
            *slot = other;
            return Err(SqlError::InvalidSql(format!(
                "cannot assign array element on non-array routine variable {name}"
            )));
        }
    };
    if array.len() <= offset {
        array.resize(offset + 1, JsonValue::Null);
    }
    array[offset] = sql_value_to_json(value);
    *slot = SqlValue::Json(JsonValue::Array(array));
    Ok(())
}

/// The implicit plpgsql FOUND variable, kept as an ordinary frame variable so
/// `IF NOT FOUND` resolves through the same routine-variable lookup as any
/// declared name. Routine variables shadow columns inside a body, exactly as
/// they do in PostgreSQL.
pub(crate) const ROUTINE_FOUND_VAR: &str = "found";

/// FOUND semantics for a bare SQL statement inside a routine: rows returned,
/// or a command tag reporting at least one affected row ("UPDATE 2",
/// "INSERT 0 1", "DELETE 1"). `INSERT ... ON CONFLICT DO NOTHING` that skips
/// its row reports "INSERT 0 0" and therefore FOUND = false, which is what
/// posting-rule idempotency keys on.
pub(crate) fn sql_result_found(result: &SqlResult) -> bool {
    if !result.rows.is_empty() {
        return true;
    }
    result
        .command_tag
        .as_deref()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse::<i64>().ok())
        .is_some_and(|count| count > 0)
}

pub(crate) fn assign_routine_targets(
    frame: &mut RoutineFrame,
    targets: &[String],
    row: Option<&Vec<SqlValue>>,
) -> Result<()> {
    assign_routine_targets_with_columns(frame, targets, &[], row.cloned())
}

/// Column-aware INTO assignment. One target receiving a multi-column row is a
/// row variable (`record` / `table%ROWTYPE`): it gets a JSON object keyed by
/// the result's column names, so `rowvar.field` afterwards resolves through
/// the same dotted-path lookup NEW/OLD use. Multiple targets stay positional
/// scalars, exactly as before.
pub(crate) fn assign_routine_targets_with_columns(
    frame: &mut RoutineFrame,
    targets: &[String],
    columns: &[String],
    row: Option<Vec<SqlValue>>,
) -> Result<()> {
    if row.is_none() && plpgsql_into_no_data_enabled() {
        return Err(SqlError::NoDataFound);
    }
    if targets.len() == 1 && columns.len() > 1 {
        let value = match row.as_ref() {
            Some(row) => SqlValue::Json(JsonValue::Object(
                columns
                    .iter()
                    .zip(row.iter())
                    .map(|(column, value)| (column.clone(), crate::jsonb::sql_value_to_json(value)))
                    .collect(),
            )),
            None => SqlValue::Null,
        };
        frame.assign(&targets[0], value)?;
        return Ok(());
    }
    assign_routine_targets_owned_nullable(frame, targets, row)?;
    Ok(())
}

/// INTO consumes the SQL result, so move scalar and array values into the frame
/// rather than deep-cloning them immediately before their result row is dropped.
pub(crate) fn assign_routine_targets_owned_nullable(
    frame: &mut RoutineFrame,
    targets: &[String],
    row: Option<Vec<SqlValue>>,
) -> Result<()> {
    let mut values = row.into_iter().flatten();
    for target in targets {
        frame.assign(target, values.next().unwrap_or(SqlValue::Null))?;
    }
    Ok(())
}

pub(crate) fn assign_routine_targets_nullable(
    frame: &mut RoutineFrame,
    targets: &[String],
    row: Option<&Vec<SqlValue>>,
) -> Result<()> {
    for (idx, target) in targets.iter().enumerate() {
        let value = row
            .and_then(|row| row.get(idx))
            .cloned()
            .unwrap_or(SqlValue::Null);
        frame.assign(target, value)?;
    }
    Ok(())
}

pub(crate) fn delete_role_record(db: &mut BicDb, name: &str) -> Result<()> {
    match db.delete(ROLE_COLLECTION, &normalize_role_name(name)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Maximum expression nesting depth accepted from a parsed statement, from
/// `BICDB_MAX_EXPRESSION_DEPTH` (default 500). A deeply nested expression —
/// e.g. `SELECT 1+1+1+…+1` with thousands of terms — parses iteratively into a
/// deep AST, but every later pass (the precedence-repair visitor, the recursive
/// evaluator, even the AST's own Drop) recurses over it and overflows the
/// thread stack, which aborts the WHOLE PROCESS. Any authenticated role could
/// crash the server with one query; this bound refuses it first.
pub(crate) fn max_expression_depth() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_EXPRESSION_DEPTH")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|depth| *depth > 0)
            .unwrap_or(500)
    })
}

/// Maximum TOTAL expression nodes accepted across a parsed statement, from
/// `BICDB_MAX_EXPRESSION_NODES` (default 100_000). A flat-but-enormous
/// expression — a 500k-element `IN (...)` list or `ARRAY[...]` literal — is
/// shallow (so the depth guard passes) but drives O(n) planning/evaluation that
/// takes many seconds; this bounds the breadth as well.
pub(crate) fn max_sql_bytes() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_SQL_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|bytes| *bytes > 0)
            .unwrap_or(16 * 1024 * 1024)
    })
}

pub(crate) fn reject_oversized_sql(sql: &str) -> Result<()> {
    let limit = max_sql_bytes();
    if sql.len() > limit {
        return Err(SqlError::resource_limit(
            "54000",
            format!(
                "SQL statement is {} bytes, over the limit of {limit}; split it or raise                  BICDB_MAX_SQL_BYTES",
                sql.len()
            ),
        ));
    }
    Ok(())
}

pub(crate) fn max_expression_nodes() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_EXPRESSION_NODES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|nodes| *nodes > 0)
            .unwrap_or(100_000)
    })
}

/// Iteratively (heap stack — never recursively, which would itself overflow on
/// a hostile AST) verify that no expression in `statements` nests deeper than
/// the limit. Covers the arithmetic/boolean/cast/parenthesis chains that are the
/// stack-overflow vectors and descends through subqueries.
pub(crate) fn reject_deep_expressions(statements: &[Statement]) -> Result<()> {
    use sqlparser::ast::{Expr, Query, SetExpr};

    let limit = max_expression_depth();
    let node_limit = max_expression_nodes();
    let mut total_nodes: usize = 0;

    // Push every child expression of `expr` onto the DFS stack at depth+1, and
    // queue any nested subquery for its own expression scan.
    fn push_children<'a>(
        expr: &'a Expr,
        depth: usize,
        exprs: &mut Vec<(&'a Expr, usize)>,
        queries: &mut Vec<&'a Query>,
    ) {
        let mut child = |e: &'a Expr| exprs.push((e, depth + 1));
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                child(left);
                child(right);
            }
            Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Collate { expr, .. } => {
                child(expr)
            }
            Expr::Cast { expr, .. } => child(expr),
            Expr::IsFalse(e)
            | Expr::IsNotFalse(e)
            | Expr::IsTrue(e)
            | Expr::IsNotTrue(e)
            | Expr::IsNull(e)
            | Expr::IsNotNull(e)
            | Expr::IsUnknown(e)
            | Expr::IsNotUnknown(e) => child(e),
            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                child(a);
                child(b);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                child(expr);
                child(low);
                child(high);
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                child(expr);
                child(pattern);
            }
            Expr::InList { expr, list, .. } => {
                child(expr);
                for item in list {
                    child(item);
                }
            }
            Expr::Tuple(items) => {
                for item in items {
                    child(item);
                }
            }
            Expr::Array(array) => {
                for item in &array.elem {
                    child(item);
                }
            }
            Expr::Function(function) => {
                if let sqlparser::ast::FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        let arg_expr = match arg {
                            sqlparser::ast::FunctionArg::Named { arg, .. }
                            | sqlparser::ast::FunctionArg::ExprNamed { arg, .. }
                            | sqlparser::ast::FunctionArg::Unnamed(arg) => arg,
                        };
                        if let sqlparser::ast::FunctionArgExpr::Expr(inner) = arg_expr {
                            child(inner);
                        }
                    }
                }
            }
            Expr::Subquery(query)
            | Expr::Exists {
                subquery: query, ..
            }
            | Expr::InSubquery {
                subquery: query, ..
            } => queries.push(query),
            _ => {}
        }
    }

    // Seed the query worklist from the top-level statements.
    let mut queries: Vec<&Query> = Vec::new();
    for statement in statements {
        match statement {
            Statement::Query(query) => queries.push(query),
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    queries.push(source);
                }
            }
            _ => {}
        }
    }

    let mut scratch: Vec<(&Expr, usize)> = Vec::new();
    while let Some(query) = queries.pop() {
        // Only Select bodies carry the expression surface we bound; other set
        // expressions (UNION arms) are themselves Queries reached via recursion
        // through `SetExpr`, so unfold them iteratively too.
        let mut bodies: Vec<&SetExpr> = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            let select = match body {
                SetExpr::Select(select) => select,
                SetExpr::Query(inner) => {
                    queries.push(inner);
                    continue;
                }
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left);
                    bodies.push(right);
                    continue;
                }
                _ => continue,
            };
            // Collect every root expression this Select exposes directly.
            let mut roots: Vec<&Expr> = Vec::new();
            for item in &select.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => roots.push(expr),
                    _ => {}
                }
            }
            if let Some(selection) = &select.selection {
                roots.push(selection);
            }
            if let Some(having) = &select.having {
                roots.push(having);
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                roots.extend(exprs.iter());
            }
            for table in &select.from {
                for join in &table.joins {
                    if let sqlparser::ast::JoinOperator::Inner(
                        sqlparser::ast::JoinConstraint::On(on),
                    )
                    | sqlparser::ast::JoinOperator::LeftOuter(
                        sqlparser::ast::JoinConstraint::On(on),
                    )
                    | sqlparser::ast::JoinOperator::RightOuter(
                        sqlparser::ast::JoinConstraint::On(on),
                    )
                    | sqlparser::ast::JoinOperator::FullOuter(
                        sqlparser::ast::JoinConstraint::On(on),
                    ) = &join.join_operator
                    {
                        roots.push(on);
                    }
                }
            }

            // Bound each root expression's depth iteratively.
            for root in roots {
                scratch.clear();
                scratch.push((root, 1));
                while let Some((expr, depth)) = scratch.pop() {
                    total_nodes += 1;
                    if total_nodes > node_limit {
                        return Err(SqlError::resource_limit(
                            "54001",
                            format!(
                                "statement has more than {node_limit} expression nodes;                                  shorten the query (e.g. a smaller IN list or array) or                                  raise BICDB_MAX_EXPRESSION_NODES"
                            ),
                        ));
                    }
                    if depth > limit {
                        return Err(SqlError::resource_limit(
                            "54001",
                            format!(
                                "expression nests deeper than the limit of {limit}; simplify \
                                 the query or raise BICDB_MAX_EXPRESSION_DEPTH"
                            ),
                        ));
                    }
                    push_children(expr, depth, &mut scratch, &mut queries);
                }
            }
        }
    }
    Ok(())
}

/// Maximum parenthesis/bracket nesting accepted in raw SQL, from
/// `BICDB_MAX_PARSE_NESTING_DEPTH` (default 40). Some grammar shapes make
/// sqlparser parse in EXPONENTIAL time as nesting grows — a ~1.3 KB
/// `CAST(CAST(…CAST(1 AS INT)…))` at depth 50 hangs the parser for tens of
/// seconds (a per-connection CPU denial of service on a tiny input), and deep
/// nesting can also overflow the parser stack. This bound rejects such input
/// with a cheap O(len) pre-scan BEFORE `Parser::parse_sql` ever sees it.
pub(crate) fn max_parse_nesting_depth() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_PARSE_NESTING_DEPTH")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|depth| *depth > 0)
            .unwrap_or(40)
    })
}

/// Cheap pre-parse guard: scan raw SQL for `(`/`[` nesting deeper than the
/// limit, ignoring parens inside string literals, quoted identifiers,
/// dollar-quoted strings, and comments. Runs before the parser so a
/// pathological nesting cannot make the parser hang or overflow.
pub(crate) fn reject_deep_parse_nesting(sql: &str) -> Result<()> {
    let limit = max_parse_nesting_depth();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut depth: usize = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                // single-quoted string; '' is an escaped quote
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                // quoted identifier; "" is an escaped quote
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                // dollar-quoted string: $tag$ ... $tag$
                let start = i;
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'$' {
                    let tag = &bytes[start..=j]; // includes both $
                    let mut k = j + 1;
                    let mut closed = false;
                    while k + tag.len() <= bytes.len() {
                        if &bytes[k..k + tag.len()] == tag {
                            i = k + tag.len();
                            closed = true;
                            break;
                        }
                        k += 1;
                    }
                    if !closed {
                        i = bytes.len();
                    }
                    continue;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
                continue;
            }
            b'(' | b'[' => {
                depth += 1;
                if depth > limit {
                    return Err(SqlError::resource_limit(
                        "54001",
                        format!(
                            "SQL nests parentheses/brackets deeper than the limit of {limit}; \
                             simplify the query or raise BICDB_MAX_PARSE_NESTING_DEPTH"
                        ),
                    ));
                }
            }
            b')' | b']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
        i += 1;
    }
    Ok(())
}

/// Maximum length of a user-supplied regular-expression pattern, from
/// `BICDB_MAX_REGEX_PATTERN_BYTES` (default 8192). Compiling a regex is roughly
/// linear in pattern length but with a large constant: a ~1 MiB pattern such as
/// `(a?)` repeated hundreds of thousands of times takes ~12 s to compile before
/// the engine even reports it invalid — a per-query CPU denial of service any
/// authenticated role can send through `~`, `~*`, or a jsonpath `like_regex`.
pub(crate) fn max_regex_pattern_bytes() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_REGEX_PATTERN_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|bytes| *bytes > 0)
            .unwrap_or(8192)
    })
}

/// Refuse a regex pattern longer than the limit before it reaches the regex
/// compiler. Callers must also cap compiled size via `RegexBuilder::size_limit`.
pub(crate) fn reject_oversized_regex(pattern: &str) -> Result<()> {
    let limit = max_regex_pattern_bytes();
    if pattern.len() > limit {
        return Err(SqlError::resource_limit(
            "54000",
            format!(
                "regular expression pattern is {} bytes, over the limit of {limit}; \
                 shorten it or raise BICDB_MAX_REGEX_PATTERN_BYTES",
                pattern.len()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod leading_keyword_tests {
    use super::{leading_keyword_bypasses_raw_probes, leading_sql_keyword};

    #[test]
    fn first_keyword_skips_whitespace_semicolons_and_comments() {
        assert_eq!(leading_sql_keyword("CALL neword(1, 2)"), Some("CALL"));
        assert_eq!(
            leading_sql_keyword("  ;; -- note\n /* c */ update t set a=1"),
            Some("update")
        );
        assert_eq!(leading_sql_keyword("/* unterminated"), None);
        assert_eq!(leading_sql_keyword("   "), None);
        assert_eq!(leading_sql_keyword("$1"), None);
        assert_eq!(leading_sql_keyword("pg_catalog.now()"), Some("pg_catalog"));
    }

    #[test]
    fn only_dml_and_call_bypass_the_raw_probes() {
        for sql in [
            "CALL p()",
            "insert into t values (1)",
            "UPDATE t SET a = 1",
            "delete from t",
        ] {
            assert!(leading_keyword_bypasses_raw_probes(sql), "{sql}");
        }
        for sql in [
            "SELECT 1",
            "CREATE TABLE t (a int)",
            "ANALYZE",
            "VACUUM",
            "RESET all",
            "COMMENT ON TABLE t IS 'x'",
            "DO $$ begin end $$",
            "GRANT SELECT ON t TO r",
            "MERGE MATERIALIZED AGGREGATE PROJECTION p",
            "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
            "",
        ] {
            assert!(!leading_keyword_bypasses_raw_probes(sql), "{sql}");
        }
    }
}
