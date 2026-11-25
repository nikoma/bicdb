//! Matched-update MERGE uses the ordinary authorized UPDATE executor, with
//! statement-wide target cardinality rules instead of UPDATE FROM's first match.
use crate::*;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Merge, MergeAction, MergeClauseKind, MergeInsertKind, OutputClause, Update,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::Token;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpdateMatchPolicy {
    First,
    PostgresMerge,
    OracleMerge,
}

impl UpdateMatchPolicy {
    pub(super) fn duplicate_error(self) -> SqlError {
        SqlError::data_exception(
            "21000",
            if self == Self::OracleMerge {
                "ORA-30926: MERGE cannot update the same target row twice"
            } else {
                "MERGE command cannot affect row a second time"
            },
            None,
        )
    }
}

impl SqlSession<'_> {
    pub(super) fn execute_merge(&mut self, merge: &Merge) -> Result<SqlResult> {
        let oracle = oracle::preview_routine_language_enabled(self.db_ref())?;
        if !oracle && !is_single_matched_update(merge) {
            let owned = std::mem::replace(&mut self.ir_owned_statement, false);
            let result = self.execute_postgres_merge(merge);
            self.ir_owned_statement = owned;
            return result;
        }
        self.execute_matched_update_merge(merge, oracle)
    }

    /// One `WHEN MATCHED [AND ...] THEN UPDATE` clause: the authorized UPDATE
    /// executor with statement-wide target cardinality rules. Both profiles
    /// use it (it also covers partitioned and row-security targets).
    fn execute_matched_update_merge(&mut self, merge: &Merge, oracle: bool) -> Result<SqlResult> {
        // Validate the complete shape before evaluating the source or making
        // changes. Unsupported actions must never be silently discarded.
        let [clause] = merge.clauses.as_slice() else {
            return Err(SqlError::Unsupported(
                "MERGE currently requires one matched UPDATE clause".into(),
            ));
        };
        let MergeAction::Update(action) = &clause.action else {
            return Err(SqlError::Unsupported(
                "MERGE currently supports matched UPDATE actions".into(),
            ));
        };
        if clause.clause_kind != MergeClauseKind::Matched
            || action.delete_predicate.is_some()
            || merge.output.is_some()
        {
            return Err(SqlError::Unsupported(
                "MERGE action shape is not supported".into(),
            ));
        }
        if !oracle && action.update_predicate.is_some() {
            return Err(SqlError::InvalidSql(
                "PostgreSQL MERGE uses WHEN MATCHED AND, not UPDATE WHERE".into(),
            ));
        }
        let target = TableWithJoins {
            relation: merge.table.clone(),
            joins: Vec::new(),
        };
        let (table, _) = table_with_joins_name_and_alias(&target)?;
        let TableFactor::Table {
            name: target_name,
            alias: target_alias,
            ..
        } = &merge.table
        else {
            return Err(SqlError::Unsupported(
                "MERGE requires a table target".into(),
            ));
        };
        let target_parts = object_name_parts(target_name);
        let target_qualifiers = if let Some(alias) = target_alias {
            let name = ident_value(&alias.name);
            let mut qualifiers = vec![vec![name.clone()]];
            // Oracle namespace binding supplies the visible table name as
            // an alias for an otherwise unaliased relation. Its original
            // schema.table qualification still identifies the target.
            if target_parts.last() == Some(&name) {
                qualifiers.push(target_parts.clone());
            }
            qualifiers
        } else {
            vec![
                target_parts.clone(),
                target_parts.last().cloned().into_iter().collect(),
            ]
        };
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let schema = load_schema_shared(self.db_ref(), &resolved)?;
        // The current adapter materializes target/source rows. Require a
        // table read grant until per-column MERGE scope authorization exists.
        self.require_table_privilege(&resolved, "SELECT")?;
        let mut assignments = action.assignments.clone();
        for assignment in &mut assignments {
            let AssignmentTarget::ColumnName(name) = &mut assignment.target else {
                return Err(SqlError::Unsupported(
                    "MERGE supports single-column assignment targets".into(),
                ));
            };
            if name.0.len() > 1 {
                let parts = object_name_parts(name);
                if !oracle
                    || !target_qualifiers
                        .iter()
                        .any(|qualifier| qualifier.as_slice() == &parts[..parts.len() - 1])
                {
                    return Err(SqlError::InvalidSql(
                        "invalid qualified MERGE assignment target".into(),
                    ));
                }
                name.0 = vec![name.0.last().unwrap().clone()];
            }
        }
        if oracle {
            if expr_contains_subquery(&merge.on) {
                return Err(SqlError::Unsupported(
                    "MERGE ON subqueries require scoped target-column analysis".into(),
                ));
            }
            let mut references = BTreeSet::new();
            let _ = sqlparser::ast::visit_expressions(merge.on.as_ref(), |expr| {
                match expr {
                    Expr::Identifier(id)
                        if schema
                            .as_ref()
                            .is_some_and(|schema| schema.column(&ident_value(id)).is_some()) =>
                    {
                        references.insert(ident_value(id));
                    }
                    Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                        let qualifier = parts[..parts.len() - 1]
                            .iter()
                            .map(ident_value)
                            .collect::<Vec<_>>();
                        if target_qualifiers.contains(&qualifier) {
                            references.insert(ident_value(parts.last().unwrap()));
                        }
                    }
                    _ => {}
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            for assignment in &assignments {
                if let AssignmentTarget::ColumnName(name) = &assignment.target {
                    let name = relation_name(name)?;
                    if references.contains(&name) {
                        return Err(SqlError::data_exception(
                            "42000",
                            "ORA-38104: Columns referenced in the ON clause cannot be updated",
                            None,
                        ));
                    }
                }
            }
        }
        let mut selection = *merge.on.clone();
        for predicate in [clause.predicate.as_ref(), action.update_predicate.as_ref()]
            .into_iter()
            .flatten()
        {
            selection = and_expr(selection, predicate.clone());
        }
        let update = Update {
            update_token: action.update_token.clone(),
            optimizer_hints: merge.optimizer_hints.clone(),
            table: target,
            assignments,
            from: Some(UpdateTableFromKind::AfterSet(vec![TableWithJoins {
                relation: merge.source.clone(),
                joins: Vec::new(),
            }])),
            selection: Some(selection),
            returning: None,
            output: None,
            or: None,
            order_by: Vec::new(),
            limit: None,
        };
        // The UPDATE is a transient adapter, not an IR-owned AST. Its heap
        // addresses must never become persistent routine memo identities.
        let owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = self.execute_update_with_match_policy(
            &update,
            BTreeMap::new(),
            if oracle {
                UpdateMatchPolicy::OracleMerge
            } else {
                UpdateMatchPolicy::PostgresMerge
            },
            None,
        );
        self.ir_owned_statement = owned;
        let mut result = result?;
        if let Some(tag) = result.command_tag.as_mut() {
            *tag = tag.replacen("UPDATE ", "MERGE ", 1);
        }
        Ok(result)
    }
}

/// Placeholder columns the parse front end writes for the PostgreSQL MERGE
/// actions the SQL parser does not read: `DO NOTHING` and
/// `INSERT DEFAULT VALUES` (see `rewrite_merge_parse_compat`).
pub(crate) const MERGE_DO_NOTHING_COLUMN: &str = "bicdb merge do nothing";
pub(crate) const MERGE_DEFAULT_VALUES_COLUMN: &str = "bicdb merge default values";
/// Hidden slots of the MERGE join rows. They carry no dot, so no qualified
/// or unqualified user reference can resolve to them.
const SOURCE_ROW_COLUMN: &str = "\u{0}merge_source_row";
const ITEM_COLUMN: &str = "\u{0}merge_item";
const ACTION_COLUMN: &str = "\u{0}merge_action";
const SOURCE_CTE: &str = "bicdb merge source";

/// Target rows one MERGE action applies to, chosen from the statement's join
/// snapshot, and the slot rows the action's executor reports back.
pub(crate) struct MergeActionRows {
    pub(crate) source_columns: Vec<String>,
    pub(crate) update_candidates: Vec<UpdateAssignmentCandidate>,
    pub(crate) returned_rows: Vec<SlotRow>,
    pub(crate) returned_columns: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergeWhen {
    Matched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

enum MergeStep<'a> {
    Update(&'a [Assignment]),
    Delete,
    /// `values: None` is `DEFAULT VALUES`.
    Insert {
        columns: Vec<String>,
        values: Option<&'a [Expr]>,
    },
    Nothing,
}

struct MergeWhenClause<'a> {
    when: MergeWhen,
    predicate: Option<&'a Expr>,
    step: MergeStep<'a>,
}

struct MergeItem {
    clause: usize,
    candidate: Option<UpdateAssignmentCandidate>,
    source_row: usize,
}

fn is_placeholder_ident(ident: &Ident, name: &str) -> bool {
    ident.quote_style == Some('"') && ident.value == name
}

fn is_do_nothing_update(update: &sqlparser::ast::MergeUpdateExpr) -> bool {
    matches!(update.assignments.as_slice(), [assignment]
        if matches!(&assignment.target, AssignmentTarget::ColumnName(name)
            if matches!(name.0.as_slice(), [sqlparser::ast::ObjectNamePart::Identifier(ident)]
                if is_placeholder_ident(ident, MERGE_DO_NOTHING_COLUMN))))
}

fn insert_placeholder(insert: &sqlparser::ast::MergeInsertExpr) -> Option<&'static str> {
    let [column] = insert.columns.as_slice() else {
        return None;
    };
    let [sqlparser::ast::ObjectNamePart::Identifier(ident)] = column.0.as_slice() else {
        return None;
    };
    [MERGE_DO_NOTHING_COLUMN, MERGE_DEFAULT_VALUES_COLUMN]
        .into_iter()
        .find(|name| is_placeholder_ident(ident, name))
}

/// The legacy single-clause shape keeps its dedicated adapter (it also
/// serves partitioned and row-security targets and the preview profile).
fn is_single_matched_update(merge: &Merge) -> bool {
    merge.output.is_none()
        && matches!(merge.clauses.as_slice(), [clause]
            if clause.clause_kind == MergeClauseKind::Matched
                && matches!(&clause.action, MergeAction::Update(update)
                    if !is_do_nothing_update(update)))
}

fn merge_when_clauses(merge: &Merge) -> Result<Vec<MergeWhenClause<'_>>> {
    let mut clauses = Vec::with_capacity(merge.clauses.len());
    for clause in &merge.clauses {
        let when = match clause.clause_kind {
            MergeClauseKind::Matched => MergeWhen::Matched,
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => {
                MergeWhen::NotMatchedByTarget
            }
            MergeClauseKind::NotMatchedBySource => MergeWhen::NotMatchedBySource,
        };
        let step = match &clause.action {
            MergeAction::Update(update) => {
                if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                    return Err(SqlError::InvalidSql(
                        "syntax error at or near \"WHERE\"".into(),
                    ));
                }
                if is_do_nothing_update(update) {
                    MergeStep::Nothing
                } else {
                    MergeStep::Update(&update.assignments)
                }
            }
            MergeAction::Delete { .. } => MergeStep::Delete,
            MergeAction::Insert(insert) => {
                if insert.insert_predicate.is_some() {
                    return Err(SqlError::InvalidSql(
                        "syntax error at or near \"WHERE\"".into(),
                    ));
                }
                match insert_placeholder(insert) {
                    Some(MERGE_DO_NOTHING_COLUMN) => MergeStep::Nothing,
                    Some(_) => MergeStep::Insert {
                        columns: Vec::new(),
                        values: None,
                    },
                    None => {
                        let MergeInsertKind::Values(values) = &insert.kind else {
                            return Err(SqlError::InvalidSql(
                                "syntax error at or near \"ROW\"".into(),
                            ));
                        };
                        let [row] = values.rows.as_slice() else {
                            return Err(SqlError::InvalidSql(
                                "syntax error at or near \",\"".into(),
                            ));
                        };
                        let columns = insert
                            .columns
                            .iter()
                            .map(|name| {
                                object_name_parts(name)
                                    .pop()
                                    .ok_or_else(|| SqlError::InvalidSql("empty column".into()))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        MergeStep::Insert {
                            columns,
                            values: Some(row.content.as_slice()),
                        }
                    }
                }
            }
        };
        clauses.push(MergeWhenClause {
            when,
            predicate: clause.predicate.as_ref(),
            step,
        });
    }
    for when in [
        MergeWhen::Matched,
        MergeWhen::NotMatchedByTarget,
        MergeWhen::NotMatchedBySource,
    ] {
        let mut unconditional = false;
        for clause in clauses.iter().filter(|clause| clause.when == when) {
            if unconditional {
                return Err(SqlError::data_exception(
                    "42601",
                    "unreachable WHEN clause specified after unconditional WHEN clause",
                    None,
                ));
            }
            unconditional |= clause.predicate.is_none();
        }
    }
    Ok(clauses)
}

/// The name the source relation's columns are qualified with.
fn merge_source_qualifier(source: &TableFactor) -> Result<String> {
    match source {
        TableFactor::Table {
            alias: Some(alias), ..
        }
        | TableFactor::Derived {
            alias: Some(alias), ..
        }
        | TableFactor::Function {
            alias: Some(alias), ..
        }
        | TableFactor::UNNEST {
            alias: Some(alias), ..
        } => Ok(ident_value(&alias.name)),
        TableFactor::Table { name, .. } => object_name_parts(name)
            .pop()
            .ok_or_else(|| SqlError::InvalidSql("empty MERGE source name".into())),
        _ => Err(SqlError::Unsupported(
            "MERGE source must be a table, a subquery or VALUES with an alias".into(),
        )),
    }
}

/// PostgreSQL scopes each WHEN clause: NOT MATCHED [BY TARGET] sees only the
/// source, NOT MATCHED BY SOURCE only the target.
fn reject_hidden_relation_reference(expr: &Expr, hidden: &str) -> Result<()> {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(expr, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() >= 2 && ident_value(&parts[parts.len() - 2]) == hidden {
                found = true;
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    if found {
        return Err(SqlError::data_exception(
            "42P01",
            format!("invalid reference to FROM-clause entry for table \"{hidden}\""),
            None,
        ));
    }
    Ok(())
}

fn item_index(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Int(index) => *index,
        _ => -1,
    }
}

fn command_count(result: &SqlResult) -> usize {
    result
        .command_tag
        .as_deref()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(result.rows.len())
}

/// RETURNING may name `merge_action()`; it reads the hidden action slot.
fn rewrite_merge_action_calls(items: &mut [SelectItem]) {
    fn is_merge_action(expr: &Expr) -> bool {
        let Expr::Function(function) = expr else {
            return false;
        };
        object_name_parts(&function.name).last().map(String::as_str) == Some("merge_action")
            && match &function.args {
                FunctionArguments::None => true,
                FunctionArguments::List(list) => list.args.is_empty(),
                FunctionArguments::Subquery(_) => false,
            }
    }
    for item in items.iter_mut() {
        if let SelectItem::UnnamedExpr(expr) = item {
            if is_merge_action(expr) {
                *item = SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: Ident::new("merge_action"),
                };
            }
        }
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let _ = visit_expressions_mut(expr, |expr| {
            if is_merge_action(expr) {
                *expr = Expr::Identifier(Ident::new(ACTION_COLUMN));
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
}

impl SqlSession<'_> {
    /// PostgreSQL MERGE: one join of source and target decides, per joined
    /// row, the first WHEN clause whose kind and condition hold. The chosen
    /// rows then go through the ordinary authorized UPDATE, DELETE and INSERT
    /// executors (constraints, triggers, defaults, foreign keys), in source
    /// order, batching consecutive rows of one clause.
    fn execute_postgres_merge(&mut self, merge: &Merge) -> Result<SqlResult> {
        let clauses = merge_when_clauses(merge)?;
        let returning = match &merge.output {
            None => None,
            Some(OutputClause::Returning { select_items, .. }) => Some(select_items.clone()),
            Some(OutputClause::Output { .. }) => {
                return Err(SqlError::InvalidSql(
                    "syntax error at or near \"OUTPUT\"".into(),
                ))
            }
        };
        let TableFactor::Table {
            name: target_name, ..
        } = &merge.table
        else {
            return Err(SqlError::Unsupported(
                "MERGE requires a table target".into(),
            ));
        };
        let target = TableWithJoins {
            relation: merge.table.clone(),
            joins: Vec::new(),
        };
        let (table, alias) = table_with_joins_name_and_alias(&target)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        if load_view(self.db_ref(), &table)?.is_some() {
            return Err(SqlError::Unsupported(
                "MERGE into views is not supported".into(),
            ));
        }
        let Some(schema) = load_schema_shared(self.db_ref(), &table)? else {
            return Err(SqlError::Unsupported(
                "MERGE requires a target table with a schema".into(),
            ));
        };
        if schema.rls_enabled || schema.rls_forced {
            return Err(SqlError::Unsupported(
                "MERGE on a table with row-level security supports only a single WHEN MATCHED THEN UPDATE clause".into(),
            ));
        }
        if schema.requires_row_move_plan()
            || schema.partitioning.is_some()
            || schema.partition_of.is_some()
        {
            return Err(SqlError::Unsupported(
                "MERGE on a partitioned table supports only a single WHEN MATCHED THEN UPDATE clause"
                    .into(),
            ));
        }
        // PostgreSQL checks every action's privilege up front, whether or
        // not any row reaches the action.
        self.require_table_privilege(&table, "SELECT")?;
        for clause in &clauses {
            match &clause.step {
                MergeStep::Update(assignments) => {
                    self.require_update_privileges(&table, assignments)?
                }
                MergeStep::Delete => self.require_table_privilege(&table, "DELETE")?,
                MergeStep::Insert { .. } => self.require_table_privilege(&table, "INSERT")?,
                MergeStep::Nothing => {}
            }
        }
        let source_qualifier = merge_source_qualifier(&merge.source)?;
        if source_qualifier == alias {
            return Err(SqlError::data_exception(
                "42712",
                format!("table name \"{alias}\" specified more than once"),
                None,
            ));
        }
        for clause in &clauses {
            let hidden = match clause.when {
                MergeWhen::Matched => continue,
                MergeWhen::NotMatchedByTarget => &alias,
                MergeWhen::NotMatchedBySource => &source_qualifier,
            };
            if let Some(predicate) = clause.predicate {
                reject_hidden_relation_reference(predicate, hidden)?;
            }
            match &clause.step {
                MergeStep::Update(assignments) => {
                    for assignment in assignments.iter() {
                        reject_hidden_relation_reference(&assignment.value, hidden)?;
                    }
                }
                MergeStep::Insert {
                    values: Some(values),
                    ..
                } => {
                    for value in values.iter() {
                        reject_hidden_relation_reference(value, hidden)?;
                    }
                }
                _ => {}
            }
        }
        for clause in &clauses {
            if let MergeStep::Update(assignments) = &clause.step {
                for assignment in assignments.iter() {
                    if let AssignmentTarget::ColumnName(name) = &assignment.target {
                        if name.0.len() > 1 {
                            return Err(SqlError::InvalidSql(
                                "MERGE UPDATE assignment targets cannot be qualified".into(),
                            ));
                        }
                    }
                }
            }
        }
        let ctes = BTreeMap::new();

        // The join snapshot: every source row, with its ordinal, and each
        // target row the ON condition pairs it with.
        let mut source = self
            .update_from_row_set(
                Some(&UpdateTableFromKind::AfterSet(vec![TableWithJoins {
                    relation: merge.source.clone(),
                    joins: Vec::new(),
                }])),
                &ctes,
            )?
            .unwrap_or(RowSet {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        let source_width = source.columns.len();
        source.columns.push(SOURCE_ROW_COLUMN.into());
        for (ordinal, row) in source.rows.iter_mut().enumerate() {
            row.push(SqlValue::Int(ordinal as i64));
        }
        let target_columns = Arc::new(row_output_columns(&table, &alias, Some(&schema)));
        let target_width = FieldRef::wildcard(Some(&schema)).len();
        let mut pair_columns =
            merge_row_set_columns(target_columns.as_ref().clone(), source.columns.clone());
        let source_row_slot = pair_columns
            .iter()
            .position(|column| column == SOURCE_ROW_COLUMN)
            .expect("source ordinal slot");
        let item_slot = pair_columns.len();
        pair_columns.push(ITEM_COLUMN.into());
        let pair_columns = Arc::new(pair_columns);
        let matched = self.update_assignment_candidates_from_rows(
            &table,
            &alias,
            Some(&schema),
            Some(&merge.on),
            &source,
            target_columns.clone(),
            &ctes,
            UpdateMatchPolicy::PostgresMerge,
        )?;
        let mut matched_by_source: Vec<Vec<UpdateAssignmentCandidate>> =
            (0..source.rows.len()).map(|_| Vec::new()).collect();
        let mut matched_targets = BTreeSet::new();
        for mut candidate in matched {
            let Some(SqlValue::Int(ordinal)) = candidate.row.get(source_row_slot).cloned() else {
                return Err(SqlError::data_exception(
                    "XX000",
                    "MERGE join row lost its source ordinal",
                    None,
                ));
            };
            matched_targets.insert(candidate.record.id.clone());
            candidate.row.push(SqlValue::Null);
            candidate.columns = pair_columns.clone();
            matched_by_source[ordinal as usize].push(candidate);
        }

        // Each joined row takes the first WHEN clause of its kind whose
        // condition holds. A target row may be changed only once.
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let (_pair_scope, pair_context) = engine.bound_row_context(pair_columns.as_ref());
        let (_source_scope, source_context) = engine.bound_row_context(&source.columns);
        let (_target_scope, target_context) = engine.bound_row_context(target_columns.as_ref());
        let choose = |when: MergeWhen, row: &SlotRow, context: &BoundRowContext| {
            for (index, clause) in clauses.iter().enumerate() {
                if clause.when != when {
                    continue;
                }
                let holds = match clause.predicate {
                    None => true,
                    Some(predicate) => engine.eval_slot_row_predicate(row, context, predicate)?,
                };
                if holds {
                    return Ok(Some(index));
                }
            }
            Ok::<_, SqlError>(None)
        };
        let mut items: Vec<MergeItem> = Vec::new();
        let mut changed_targets = BTreeSet::new();
        for (ordinal, pairs) in matched_by_source.into_iter().enumerate() {
            if ordinal % 1024 == 0 {
                self.cancellation.check()?;
            }
            if pairs.is_empty() {
                if let Some(index) = choose(
                    MergeWhen::NotMatchedByTarget,
                    &source.rows[ordinal],
                    &source_context,
                )? {
                    if !matches!(clauses[index].step, MergeStep::Nothing) {
                        items.push(MergeItem {
                            clause: index,
                            candidate: None,
                            source_row: ordinal,
                        });
                    }
                }
                continue;
            }
            for mut candidate in pairs {
                let Some(index) = choose(MergeWhen::Matched, &candidate.row, &pair_context)? else {
                    continue;
                };
                if matches!(clauses[index].step, MergeStep::Nothing) {
                    continue;
                }
                if !changed_targets.insert(candidate.record.id.clone()) {
                    return Err(UpdateMatchPolicy::PostgresMerge.duplicate_error());
                }
                candidate.row[item_slot] = SqlValue::Int(items.len() as i64);
                items.push(MergeItem {
                    clause: index,
                    candidate: Some(candidate),
                    source_row: ordinal,
                });
            }
        }
        if clauses
            .iter()
            .any(|clause| clause.when == MergeWhen::NotMatchedBySource)
        {
            let null_source = vec![SqlValue::Null; source.columns.len()];
            let targets = self.update_assignment_candidates_without_from(
                &table,
                &alias,
                Some(&schema),
                None,
                target_columns.clone(),
                &ctes,
            )?;
            for (idx, target) in targets.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.cancellation.check()?;
                }
                if matched_targets.contains(&target.record.id) {
                    continue;
                }
                let Some(index) =
                    choose(MergeWhen::NotMatchedBySource, &target.row, &target_context)?
                else {
                    continue;
                };
                if matches!(clauses[index].step, MergeStep::Nothing) {
                    continue;
                }
                let mut row = merge_slot_rows(
                    &target.row,
                    target_columns.as_ref(),
                    &null_source,
                    &source.columns,
                );
                row.push(SqlValue::Int(items.len() as i64));
                items.push(MergeItem {
                    clause: index,
                    candidate: Some(UpdateAssignmentCandidate {
                        physical_table: None,
                        record: target.record,
                        columns: pair_columns.clone(),
                        row,
                    }),
                    source_row: usize::MAX,
                });
            }
        }
        drop(engine);

        // RETURNING rows: the join row (target slots holding the new row, or
        // the old one for DELETE), the action name, then old.* and new.*.
        let target_fields = FieldRef::wildcard(Some(&schema))
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        let old_new = !["old", "new"]
            .iter()
            .any(|name| alias == *name || source_qualifier == *name);
        let mut returning_columns = pair_columns.as_ref().clone();
        returning_columns.push(ACTION_COLUMN.into());
        if old_new {
            for prefix in ["old", "new"] {
                returning_columns
                    .extend(target_fields.iter().map(|name| format!("{prefix}.{name}")));
            }
        }
        let returning_row =
            |mut row: SlotRow, action: &str, old: Option<&[SqlValue]>, new: Option<&[SqlValue]>| {
                row.push(SqlValue::String(action.into()));
                if old_new {
                    for image in [old, new] {
                        match image {
                            Some(values) => row.extend(values[..target_width].iter().cloned()),
                            None => row.extend(std::iter::repeat_n(SqlValue::Null, target_width)),
                        }
                    }
                }
                row
            };
        let source_cte = merge_source_cte(&source, source_width, &source_qualifier);
        let mut originals: HashMap<i64, SlotRow> = HashMap::new();
        let mut returned = Vec::new();
        let mut total = 0usize;
        let mut items = items.into_iter().peekable();
        while let Some(first) = items.next() {
            let index = first.clause;
            let mut run = vec![first];
            while let Some(next) = items.next_if(|item| item.clause == index) {
                run.push(next);
            }
            self.cancellation.check()?;
            match &clauses[index].step {
                MergeStep::Update(assignments) => {
                    let candidates = run
                        .into_iter()
                        .filter_map(|item| item.candidate)
                        .inspect(|candidate| {
                            if returning.is_some() {
                                originals.insert(
                                    item_index(&candidate.row[item_slot]),
                                    candidate.row[..target_width].to_vec(),
                                );
                            }
                        })
                        .collect();
                    let update = Update {
                        update_token: AttachedToken::empty(),
                        optimizer_hints: Vec::new(),
                        table: target.clone(),
                        assignments: assignments.to_vec(),
                        from: Some(UpdateTableFromKind::AfterSet(vec![TableWithJoins {
                            relation: merge.source.clone(),
                            joins: Vec::new(),
                        }])),
                        selection: None,
                        returning: returning.as_ref().map(|_| {
                            vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())]
                        }),
                        output: None,
                        or: None,
                        order_by: Vec::new(),
                        limit: None,
                    };
                    let mut rows = MergeActionRows {
                        source_columns: source.columns.clone(),
                        update_candidates: candidates,
                        returned_rows: Vec::new(),
                        returned_columns: None,
                    };
                    let result = self.execute_update_with_match_policy(
                        &update,
                        ctes.clone(),
                        UpdateMatchPolicy::PostgresMerge,
                        Some(&mut rows),
                    )?;
                    total += command_count(&result);
                    for row in rows.returned_rows {
                        let old = originals.get(&item_index(&row[item_slot])).cloned();
                        let new = row[..target_width].to_vec();
                        returned.push(returning_row(row, "UPDATE", old.as_deref(), Some(&new)));
                    }
                }
                MergeStep::Delete => {
                    let Statement::Delete(delete) =
                        parse_statements(&format!("DELETE FROM {}", merge.table))?
                            .pop()
                            .ok_or_else(|| {
                                SqlError::data_exception("XX000", "MERGE DELETE adapter", None)
                            })?
                    else {
                        return Err(SqlError::data_exception(
                            "XX000",
                            "MERGE DELETE adapter",
                            None,
                        ));
                    };
                    let mut rows = MergeActionRows {
                        source_columns: source.columns.clone(),
                        update_candidates: run
                            .into_iter()
                            .filter_map(|item| item.candidate)
                            .collect(),
                        returned_rows: Vec::new(),
                        returned_columns: None,
                    };
                    let result = self.execute_delete_with_merge_rows(
                        &delete,
                        ctes.clone(),
                        Some(&mut rows),
                    )?;
                    total += command_count(&result);
                    if returning.is_some() {
                        for row in rows.returned_rows {
                            let old = row[..target_width].to_vec();
                            returned.push(returning_row(row, "DELETE", Some(&old), None));
                        }
                    }
                }
                MergeStep::Insert { columns, values } => {
                    let ordinals = run.iter().map(|item| item.source_row).collect::<Vec<_>>();
                    let inserted = self.execute_merge_insert(
                        target_name,
                        &schema,
                        columns,
                        *values,
                        &source_cte,
                        &ordinals,
                        returning.is_some(),
                    )?;
                    total += inserted.0;
                    for (ordinal, values) in inserted.1 {
                        let mut target_row = values;
                        if target_columns.len() == target_row.len() * 2 {
                            target_row.extend(target_row.clone());
                        }
                        let mut row = merge_slot_rows(
                            &target_row,
                            target_columns.as_ref(),
                            &source.rows[ordinal],
                            &source.columns,
                        );
                        row.push(SqlValue::Null);
                        let new = target_row[..target_width].to_vec();
                        returned.push(returning_row(row, "INSERT", None, Some(&new)));
                    }
                }
                MergeStep::Nothing => {}
            }
        }
        let Some(mut items) = returning else {
            return Ok(SqlResult::command(format!("MERGE {total}")));
        };
        rewrite_merge_action_calls(&mut items);
        // `*` is the source's columns, then the target's (the join order).
        let mut wildcard = source_cte
            .slots
            .iter()
            .map(|slot| source.columns[*slot].clone())
            .collect::<Vec<_>>();
        wildcard.extend(target_columns[..target_width].iter().cloned());
        let mut result = self.project_update_from_returning_slot_rows(
            &table,
            Some(&schema),
            &items,
            &returned,
            &returning_columns,
            &wildcard,
        )?;
        result.command_tag = Some(format!("MERGE {total}"));
        Ok(result)
    }

    /// One MERGE INSERT action for the given source rows, as an ordinary
    /// `INSERT ... SELECT` over the materialized source (so defaults,
    /// identity, generated columns, constraints and triggers apply). With
    /// RETURNING, rows are inserted one at a time so each inserted row is
    /// paired with its source row.
    #[allow(clippy::too_many_arguments)]
    fn execute_merge_insert(
        &mut self,
        target_name: &ObjectName,
        schema: &TableSchema,
        columns: &[String],
        values: Option<&[Expr]>,
        source: &MergeSourceCte,
        ordinals: &[usize],
        returning: bool,
    ) -> Result<(usize, Vec<(usize, SlotRow)>)> {
        let mut insert_columns = Vec::new();
        let mut expressions = Vec::new();
        if let Some(values) = values {
            let names = if columns.is_empty() {
                schema
                    .columns
                    .iter()
                    .filter(|column| !column.hidden)
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            } else {
                columns.to_vec()
            };
            if values.len() > names.len() {
                return Err(SqlError::InvalidSql(
                    "INSERT has more expressions than target columns".into(),
                ));
            }
            if !columns.is_empty() && values.len() < names.len() {
                return Err(SqlError::InvalidSql(
                    "INSERT has more target columns than expressions".into(),
                ));
            }
            for (name, value) in names.into_iter().zip(values) {
                if expr_is_default(value) {
                    continue;
                }
                insert_columns.push(name);
                expressions.push(value.clone());
            }
        }
        let returning_sql = if returning { " RETURNING *" } else { "" };
        let sql = if insert_columns.is_empty() {
            format!("INSERT INTO {target_name} DEFAULT VALUES{returning_sql}")
        } else {
            format!(
                "INSERT INTO {target_name} ({}) SELECT NULL FROM \"{SOURCE_CTE}\" AS {}{returning_sql}",
                insert_columns
                    .iter()
                    .map(|name| Ident::with_quote('"', name).to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                Ident::with_quote('"', &source.qualifier),
            )
        };
        let Some(Statement::Insert(mut insert)) = parse_statements(&sql)?.pop() else {
            return Err(SqlError::data_exception(
                "XX000",
                "MERGE INSERT adapter",
                None,
            ));
        };
        if let Some(query) = insert.source.as_mut() {
            let SetExpr::Select(select) = query.body.as_mut() else {
                return Err(SqlError::data_exception(
                    "XX000",
                    "MERGE INSERT adapter",
                    None,
                ));
            };
            select.projection = expressions
                .into_iter()
                .map(SelectItem::UnnamedExpr)
                .collect();
        }
        let batches: Vec<&[usize]> = if returning || insert.source.is_none() {
            ordinals.chunks(1).collect()
        } else {
            vec![ordinals]
        };
        let mut count = 0;
        let mut returned = Vec::new();
        for batch in batches {
            self.cancellation.check()?;
            let mut ctes = BTreeMap::new();
            ctes.insert(
                cte_key(SOURCE_CTE),
                CteResult::new(
                    SOURCE_CTE.to_string(),
                    source.names.clone(),
                    batch
                        .iter()
                        .map(|ordinal| source.rows[*ordinal].clone())
                        .collect(),
                ),
            );
            let result = self.execute_insert_with_ctes(&insert, ctes)?;
            if returning {
                count += result.rows.len();
                returned.extend(result.rows.into_iter().map(|row| (batch[0], row)));
            } else {
                count += command_count(&result);
            }
        }
        Ok((count, returned))
    }
}

/// The source relation's own columns (as `s.*` names them) over the
/// materialized source rows, for the INSERT adapter's `SELECT ... FROM`.
struct MergeSourceCte {
    qualifier: String,
    slots: Vec<usize>,
    names: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
}

fn merge_source_cte(source: &RowSet, width: usize, qualifier: &str) -> MergeSourceCte {
    let mut slots = Vec::new();
    let mut names = Vec::new();
    for (slot, column) in source.columns[..width].iter().enumerate() {
        if let Some((relation, name)) = column.rsplit_once('.') {
            let relation = relation.rsplit('.').next().unwrap_or(relation);
            if relation == qualifier {
                slots.push(slot);
                names.push(name.to_string());
            }
        }
    }
    if slots.is_empty() {
        let mut seen = BTreeSet::new();
        for (slot, column) in source.columns[..width].iter().enumerate() {
            let name = column.rsplit('.').next().unwrap_or(column).to_string();
            if seen.insert(name.clone()) {
                slots.push(slot);
                names.push(name);
            }
        }
    }
    let rows = source
        .rows
        .iter()
        .map(|row| slots.iter().map(|slot| row[*slot].clone()).collect())
        .collect();
    MergeSourceCte {
        qualifier: qualifier.to_string(),
        slots,
        names,
        rows,
    }
}

/// sqlparser reads neither `WHEN ... THEN DO NOTHING` nor
/// `THEN INSERT DEFAULT VALUES` in MERGE. Both become placeholder actions
/// (a quoted column no table can be expected to have) that the MERGE
/// executor recognizes and maps back. Works on tokens, so strings, comments
/// and dollar-quoted bodies are untouched.
pub(crate) fn rewrite_merge_parse_compat(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let contains = |needle: &[u8]| {
        bytes
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
    };
    if !contains(b"merge") || !(contains(b"nothing") || contains(b"default")) {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let word = |index: usize| -> Option<String> {
        match source.get(index).map(|(token, _)| token) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                Some(word.value.to_ascii_uppercase())
            }
            _ => None,
        }
    };
    let next = |from: usize| -> Option<usize> {
        (from..source.len()).find(|index| !matches!(source[*index].0, Token::Whitespace(_)))
    };
    if !(0..source.len()).any(|index| word(index).as_deref() == Some("MERGE")) {
        return None;
    }
    let mut out = String::with_capacity(input.len() + 64);
    let mut changed = false;
    let mut depth = 0i32;
    let mut not_matched = false;
    let mut index = 0;
    while index < source.len() {
        match &source[index].0 {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ => {}
        }
        let current = word(index);
        if depth == 0 && current.as_deref() == Some("WHEN") {
            // Only `NOT MATCHED [BY TARGET]` takes the INSERT placeholder;
            // MATCHED and NOT MATCHED BY SOURCE take the UPDATE one.
            let after = next(index + 1);
            if after.and_then(word).as_deref() == Some("NOT") {
                let by = after
                    .and_then(|not| next(not + 1))
                    .and_then(|matched| next(matched + 1));
                let side = by.and_then(|by| next(by + 1));
                not_matched = !(by.and_then(word).as_deref() == Some("BY")
                    && side.and_then(word).as_deref() == Some("SOURCE"));
            } else if after.and_then(word).as_deref() == Some("MATCHED") {
                not_matched = false;
            }
        }
        if depth == 0 && current.as_deref() == Some("THEN") {
            if let Some(first) = next(index + 1) {
                let second = next(first + 1);
                let first_word = word(first);
                let second_word = second.and_then(word);
                let replacement = match (first_word.as_deref(), second_word.as_deref()) {
                    (Some("DO"), Some("NOTHING")) => Some(if not_matched {
                        format!("THEN INSERT (\"{MERGE_DO_NOTHING_COLUMN}\") VALUES (NULL)")
                    } else {
                        format!("THEN UPDATE SET \"{MERGE_DO_NOTHING_COLUMN}\" = NULL")
                    }),
                    (Some("INSERT"), Some("DEFAULT")) => second
                        .and_then(|default| next(default + 1))
                        .filter(|values| word(*values).as_deref() == Some("VALUES"))
                        .map(|_| {
                            format!("THEN INSERT (\"{MERGE_DEFAULT_VALUES_COLUMN}\") VALUES (NULL)")
                        }),
                    _ => None,
                };
                if let Some(replacement) = replacement {
                    let end = if first_word.as_deref() == Some("DO") {
                        second.unwrap()
                    } else {
                        next(second.unwrap() + 1).unwrap()
                    };
                    out.push_str(&replacement);
                    index = end + 1;
                    changed = true;
                    continue;
                }
            }
        }
        out.push_str(source[index].1);
        index += 1;
    }
    changed.then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(session: &mut SqlSession<'_>, oracle: bool, source: &str) -> Result<SqlResult> {
        let sql = if oracle {
            oracle::lower_session_sql(
                session.db_ref(),
                source,
                oracle::ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )?
        } else {
            source.to_string()
        };
        session.execute(&sql)
    }

    #[test]
    fn matched_merge_preserves_each_profiles_repeated_target_rules() {
        for oracle in [false, true] {
            for (values, oracle_value) in [
                (vec![0, 1], Some(1)),
                (vec![0, 0], Some(0)),
                (vec![1, 1], None),
                (vec![1, 0], None),
                (vec![0, 0, 1], Some(1)),
            ] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open(root.path()).unwrap();
                if oracle {
                    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE)
                        .unwrap();
                }
                let mut session = SqlSession::new(&mut db);
                for source in [
                    "CREATE TABLE merge_target(id INTEGER PRIMARY KEY, n INTEGER)",
                    "CREATE TABLE merge_source(seq INTEGER PRIMARY KEY, id INTEGER, n INTEGER)",
                    "INSERT INTO merge_target VALUES(1,0)",
                ] {
                    execute(&mut session, oracle, source).unwrap();
                }
                for (i, n) in values.iter().enumerate() {
                    execute(
                        &mut session,
                        oracle,
                        &format!("INSERT INTO merge_source VALUES({i},1,{n})"),
                    )
                    .unwrap();
                }
                let result = execute(&mut session, oracle, "MERGE INTO merge_target t USING (SELECT * FROM merge_source ORDER BY seq) s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n");
                let expected = if oracle { oracle_value } else { None };
                match expected {
                    Some(n) => {
                        assert_eq!(
                            result.unwrap().command_tag,
                            Some(format!("MERGE {}", values.len()))
                        );
                        assert_eq!(
                            execute(&mut session, oracle, "SELECT n FROM merge_target")
                                .unwrap()
                                .rows[0][0]
                                .to_cell(),
                            n.to_string()
                        );
                    }
                    None => {
                        assert_eq!(
                            result.unwrap_err().sqlstate(),
                            "21000",
                            "oracle={oracle} values={values:?}"
                        );
                        assert_eq!(
                            execute(&mut session, oracle, "SELECT n FROM merge_target")
                                .unwrap()
                                .rows[0][0]
                                .to_cell(),
                            "0"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn merge_filters_and_statement_rollback_keep_normal_update_from_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER CHECK(n>=0))")
            .unwrap();
        session
            .execute("CREATE TABLE merge_source(seq INTEGER PRIMARY KEY,id INTEGER,n INTEGER)")
            .unwrap();
        session
            .execute("INSERT INTO merge_target VALUES(1,0),(2,0)")
            .unwrap();
        session
            .execute("INSERT INTO merge_source VALUES(1,1,10),(2,2,-1)")
            .unwrap();
        let failed = session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n");
        assert_eq!(failed.unwrap_err().sqlstate(), "23514");
        assert_eq!(
            session
                .execute("SELECT SUM(n) FROM merge_target")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "0"
        );
        let result = session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED AND s.n>=0 THEN UPDATE SET n=s.n").unwrap();
        assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
        session.execute("DELETE FROM merge_source").unwrap();
        session
            .execute("INSERT INTO merge_source VALUES(1,1,11),(2,1,12)")
            .unwrap();
        session.execute("UPDATE merge_target AS t SET n=s.n FROM (SELECT * FROM merge_source ORDER BY seq) s WHERE t.id=s.id").unwrap();
        assert_eq!(
            session
                .execute("SELECT n FROM merge_target WHERE id=1")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "11"
        );
    }

    #[test]
    fn matched_merge_requires_target_update_and_source_read_authority() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE merge_writer LOGIN",
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
            "GRANT UPDATE(n) ON merge_target TO merge_writer",
            "SET SESSION AUTHORIZATION merge_writer",
        ] {
            session.execute(sql).unwrap();
        }
        let sql = "MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n";
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42501");
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
        session
            .execute("GRANT SELECT ON merge_target TO merge_writer")
            .unwrap();
        session
            .execute("SET SESSION AUTHORIZATION merge_writer")
            .unwrap();
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42501");
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
        session
            .execute("GRANT SELECT ON merge_source TO merge_writer")
            .unwrap();
        session
            .execute("SET SESSION AUTHORIZATION merge_writer")
            .unwrap();
        assert_eq!(
            session.execute(sql).unwrap().command_tag.as_deref(),
            Some("MERGE 1")
        );
        assert_eq!(session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET id=2").unwrap_err().sqlstate(),"42501");
    }

    #[test]
    fn transient_merge_adapters_do_not_reuse_routine_node_addresses() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
            "CREATE FUNCTION merge_twice(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n+p; MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n+p*2; RETURN 1; END$$",
        ] { session.execute(sql).unwrap(); }
        for p in [1, 2, 3] {
            session
                .execute(&format!("SELECT merge_twice({p})"))
                .unwrap();
            assert_eq!(
                session.execute("SELECT n FROM merge_target").unwrap().rows[0][0].to_cell(),
                (10 + p * 2).to_string()
            );
        }
    }

    #[test]
    fn oracle_on_columns_keep_quoted_case_and_schema_qualification() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE SCHEMA \"Scope\"",
            "CREATE TABLE \"Scope\".\"Target\"(\"Key\" INTEGER,\"key\" INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO \"Scope\".\"Target\" VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,9)",
        ] {
            execute(&mut session, true, source).unwrap();
        }
        execute(&mut session,true,"MERGE INTO \"Scope\".\"Target\" t USING merge_source s ON(t.\"Key\"=s.id) WHEN MATCHED THEN UPDATE SET t.\"key\"=s.n").unwrap();
        let error=execute(&mut session,true,"MERGE INTO \"Scope\".\"Target\" USING merge_source s ON(\"Scope\".\"Target\".\"Key\"=s.id) WHEN MATCHED THEN UPDATE SET \"Key\"=s.n").unwrap_err();
        assert!(error.to_string().contains("ORA-38104"));
        assert_eq!(
            execute(
                &mut session,
                true,
                "SELECT \"Key\",\"key\" FROM \"Scope\".\"Target\""
            )
            .unwrap()
            .rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["1", "9"]
        );
    }

    #[test]
    fn oracle_update_where_and_on_column_restriction_are_explicit() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
        ] {
            execute(&mut session, true, source).unwrap();
        }
        let result = execute(&mut session,true,"MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET t.n=s.n WHERE t.n=0").unwrap();
        assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
        let error = execute(&mut session,true,"MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET t.id=2").unwrap_err();
        assert_eq!(error.sqlstate(), "42000");
        assert!(error.to_string().contains("ORA-38104"));
        assert_eq!(
            execute(&mut session, true, "SELECT id FROM merge_target")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
    }
}
