//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_STORED_INSERT_HITS: std::cell::RefCell<usize> = const { std::cell::RefCell::new(0) };
}

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_raw_create_function(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let prefix = if lower.starts_with("create function ") {
            "create function "
        } else if lower.starts_with("create or replace function ") {
            "create or replace function "
        } else {
            return Ok(None);
        };
        let rest = trimmed[prefix.len()..].trim_start();
        let name_end = rest
            .find(|ch: char| ch == '(' || ch.is_whitespace())
            .ok_or_else(|| SqlError::InvalidSql("CREATE FUNCTION requires a name".to_string()))?;
        let (raw_function_schema, name) = routine_schema_and_name(&rest[..name_end]);
        let (return_type, returns_set) = raw_routine_return_type(trimmed)?;
        let language = raw_routine_language(trimmed)?;
        match language.as_str() {
            "sql" | "plpgsql" => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "procedural language {other} is not supported"
                )));
            }
        }
        let args = raw_parenthesized_args(rest);
        let arg_types = raw_routine_argument_type_schemas(&args)?;
        let return_type_schema = raw_routine_type_schema(&return_type)?;
        let return_type_declaration = return_type_schema.formatted();
        let (input_types, mut output_types) = raw_routine_argument_directions(&args, &arg_types);
        if lower.contains(" returns ") {
            output_types.push(return_type_schema.clone());
        }
        let trigger_return = matches!(return_type.as_str(), "trigger" | "event_trigger")
            .then_some(return_type.as_str());
        validate_routine_pseudo_types(
            &input_types,
            &output_types,
            arg_types.len(),
            trigger_return,
            &language,
        )?;
        let owner = current_user_from_gucs(&self.session_gucs);
        self.save_session_routine_if_missing(
            RoutineSchema {
                name,
                schema: raw_function_schema,
                kind: RoutineKind::Function,
                args,
                arg_types,
                return_type,
                return_type_modifier: return_type_schema.type_modifier,
                return_type_declaration: Some(return_type_declaration),
                returns_set,
                language,
                definition: trimmed.to_string(),
                internal_symbol: None,
                owner: Some(owner),
                security_definer: routine_definition_is_security_definer(trimmed),
            },
            false,
            prefix.contains("replace"),
        )?;
        Ok(Some(SqlResult::command("CREATE FUNCTION")))
    }

    pub(crate) fn execute_alter_function(
        &mut self,
        alter_function: &AlterFunction,
    ) -> Result<SqlResult> {
        if alter_function.kind != AlterFunctionKind::Function {
            return Err(SqlError::Unsupported(
                "ALTER AGGREGATE is not supported".to_string(),
            ));
        }

        // Routines are stored under their normalized qualified name
        // (`carrier_private.search_x`), not the schema-isolated relation
        // encoding — resolving through `relation_name` mangled every
        // non-public function into `__bicdb_s_...` and "did not exist".
        let name = normalize_object_name(&object_name(&alter_function.function.name)?);
        let Some(mut routine) = load_routine(self.db_ref(), RoutineKind::Function, &name)? else {
            return Err(SqlError::InvalidSql(format!(
                "function \"{name}\" does not exist"
            )));
        };

        match &alter_function.operation {
            AlterFunctionOperation::OwnerTo(owner) => {
                let current_user = current_user_from_gucs(&self.session_gucs);
                // The bootstrap role is superuser by construction, the same
                // reading every other privilege check applies; it has no
                // stored role schema to look up.
                let superuser = current_user == BOOTSTRAP_ROLE_NAME
                    || load_role_schema(self.db_ref(), &current_user)?
                        .is_some_and(|role| role.superuser);
                if !routine.owner().eq_ignore_ascii_case(&current_user) && !superuser {
                    return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                        "must be owner of function {name}"
                    ))));
                }
                let owner = match owner {
                    Owner::Ident(owner) => normalize_role_name(&ident_value(owner)),
                    Owner::CurrentRole | Owner::CurrentUser => current_user.clone(),
                    Owner::SessionUser => session_user_from_gucs(&self.session_gucs),
                };
                ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                if !superuser
                    && !settable_role_closure(self.db_ref(), &current_user)?.contains(&owner)
                {
                    return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                        "permission denied to change function owner to \"{owner}\""
                    ))));
                }
                routine.owner = Some(owner);
                self.save_session_routine(&routine)?;
                Ok(SqlResult::command("ALTER FUNCTION"))
            }
            AlterFunctionOperation::Actions { actions, .. }
                if actions
                    .iter()
                    .all(|action| matches!(action, AlterFunctionAction::Reset(_))) =>
            {
                Ok(SqlResult::command("ALTER FUNCTION"))
            }
            operation => Err(SqlError::Unsupported(format!(
                "ALTER FUNCTION {operation} is not supported"
            ))),
        }
    }

    pub(crate) fn execute_create_procedure(
        &mut self,
        or_alter: bool,
        name: &ObjectName,
        params: Option<&Vec<ProcedureParam>>,
        language: Option<&Ident>,
        body: &ConditionalStatements,
    ) -> Result<SqlResult> {
        if or_alter {
            return Err(SqlError::Unsupported(
                "CREATE OR ALTER PROCEDURE is not supported".to_string(),
            ));
        }
        let arg_types = procedure_argument_type_schemas(params)?;
        let language = routine_language(language, "void")?;
        validate_routine_pseudo_types(&arg_types, &[], arg_types.len(), None, &language)?;
        let (procedure_schema, procedure_name) = routine_schema_and_name(&object_name(name)?);
        let routine = RoutineSchema {
            name: procedure_name,
            schema: procedure_schema,
            kind: RoutineKind::Procedure,
            args: params
                .map(|params| params.iter().map(ToString::to_string).collect())
                .unwrap_or_default(),
            arg_types,
            return_type: "void".to_string(),
            return_type_modifier: None,
            return_type_declaration: Some("void".to_string()),
            returns_set: false,
            language,
            definition: format!("CREATE PROCEDURE {name} {body}"),
            internal_symbol: None,
            owner: Some(current_user_from_gucs(&self.session_gucs)),
            security_definer: false,
        };
        self.save_session_routine_if_missing(routine, false, false)?;
        Ok(SqlResult::command("CREATE PROCEDURE"))
    }

    pub(crate) fn execute_raw_create_procedure(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let prefix = if lower.starts_with("create procedure ") {
            "create procedure "
        } else if lower.starts_with("create or replace procedure ") {
            "create or replace procedure "
        } else {
            return Ok(None);
        };
        let rest = trimmed[prefix.len()..].trim_start();
        let name_end = rest
            .find(|ch: char| ch == '(' || ch.is_whitespace())
            .ok_or_else(|| SqlError::InvalidSql("CREATE PROCEDURE requires a name".to_string()))?;
        let (raw_procedure_schema, name) = routine_schema_and_name(&rest[..name_end]);
        let language = raw_routine_language(trimmed)?;
        match language.as_str() {
            "sql" | "plpgsql" => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "procedural language {other} is not supported"
                )));
            }
        }
        let args = raw_parenthesized_args(rest);
        let arg_types = raw_routine_argument_type_schemas(&args)?;
        let (input_types, output_types) = raw_routine_argument_directions(&args, &arg_types);
        validate_routine_pseudo_types(
            &input_types,
            &output_types,
            arg_types.len(),
            None,
            &language,
        )?;
        let owner = current_user_from_gucs(&self.session_gucs);
        self.save_session_routine_if_missing(
            RoutineSchema {
                name,
                schema: raw_procedure_schema,
                kind: RoutineKind::Procedure,
                args,
                arg_types,
                return_type: "void".to_string(),
                return_type_modifier: None,
                return_type_declaration: Some("void".to_string()),
                returns_set: false,
                language,
                definition: trimmed.to_string(),
                internal_symbol: None,
                owner: Some(owner),
                security_definer: routine_definition_is_security_definer(trimmed),
            },
            false,
            prefix.contains("replace"),
        )?;
        Ok(Some(SqlResult::command("CREATE PROCEDURE")))
    }

    pub(crate) fn execute_call(&mut self, function: &Function) -> Result<SqlResult> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let routine = resolve_routine_cached(self.db_ref(), RoutineKind::Procedure, &name)?
            .ok_or_else(|| SqlError::InvalidSql(format!("procedure \"{name}\" does not exist")))?;
        self.ensure_routine_execute_privilege(&name)?;
        // Evaluate the arguments in place: `function_args` deep-clones every
        // argument expression (TPC-C's NEW_ORDER call carries eleven, one a
        // nested cast) only for them to be evaluated once and dropped.
        // The arguments are typed under the callee's expression-type scope:
        // a cached CALL keeps its argument nodes at stable addresses, so the
        // per-node type memo (source types of the casts every TPC-C call
        // carries) hits from the second call on. Without the scope every
        // call re-inferred them from scratch.
        let args = {
            let _types = self.enter_routine_expr_type_scope(&routine.ir);
            match unnamed_function_arg_exprs(function) {
                Some(exprs) => exprs
                    .map(|arg| self.eval_session_expr(arg))
                    .collect::<Result<Vec<_>>>()?,
                None => function_args(function)
                    .iter()
                    .map(|arg| self.eval_session_expr(arg))
                    .collect::<Result<Vec<_>>>()?,
            }
        };

        if routine.schema.security_definer {
            return self.execute_with_routine_security(&routine.schema, |session| {
                session.execute_plpgsql_procedure(&routine.ir, &args)
            });
        }

        // Adaptive execution engine (Slice 1): observe-only. When disabled this
        // is a single relaxed atomic load and the original call path is taken
        // verbatim. When enabled, record per-signature hotness and run any
        // registered specialized module in shadow against the interpreter's
        // result. The interpreter remains authoritative.
        if adaptive::enabled() {
            let schema_gen = self.db_ref().collection_generation(SCHEMA_COLLECTION);
            let routine_gen = self.db_ref().collection_generation(ROUTINE_COLLECTION);
            let sig = adaptive::routine_signature(&name, &args);

            // WASM-authoritative whole-proc execution (Slice C). Gated by
            // `BICDB_AEE_WASM`; engaged only for procedures whose control flow +
            // expressions fully lower (integer subset) and whose args are all
            // integers. Embedded SQL re-enters this very session (preserving the
            // live transaction) via the host. Anything unsupported -> `None`/
            // run error -> the interpreter below runs unchanged.
            #[cfg(feature = "adaptive-procs")]
            if adaptive::lower::wasm_procs_enabled()
                && adaptive::lower::proc_is_all_integer(&routine.schema.args)
            {
                if let Some(int_args) = adaptive::lower::all_int_args(&args) {
                    if let Some(compiled) = adaptive::lower::get_or_build(sig, &routine.ir) {
                        // Mirror execute_plpgsql_procedure's atomicity: a write
                        // proc with no active transaction runs in an auto-
                        // transaction (commit on success / rollback on error).
                        let needs_auto_tx = self.tx.is_none()
                            && (routine_declarations_may_write(&routine.ir.declarations)
                                || routine_block_may_write(&routine.ir.statements)
                                || routine_handlers_may_write(&routine.ir.exception_handlers));
                        if needs_auto_tx {
                            return self
                                .run_wasm_procedure_in_auto_transaction(&int_args, &compiled);
                        }
                        return self.run_wasm_procedure(&int_args, &compiled);
                    }
                }
            }

            let start = Instant::now();
            let result = self.execute_plpgsql_procedure(&routine.ir, &args);
            let latency_ns = start.elapsed().as_nanos() as u64;
            if let Ok(ref produced) = result {
                let rows = produced.rows.len() as u64;
                let argc = args.len();
                let label_name = name.clone();
                adaptive::record(sig, latency_ns, rows, || {
                    format!("proc:{label_name}/{argc}")
                });
                adaptive::verify_shadow(sig, produced, &args, schema_gen, routine_gen);
            }
            return result;
        }

        self.execute_plpgsql_procedure(&routine.ir, &args)
    }

    pub(crate) fn execute_raw_create_trigger(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((trigger, or_replace)) = parse_raw_create_trigger(sql)? else {
            return Ok(None);
        };
        if load_schema(self.db_ref(), &trigger.table_name)?.is_none()
            && user_collection_names(self.db_ref())
                .iter()
                .all(|name| !name.eq_ignore_ascii_case(&trigger.table_name))
        {
            return Err(SqlError::InvalidCollection(trigger.table_name));
        }
        // A trigger runs its function with the TABLE OWNER's data flowing
        // through NEW/OLD, so attaching one to a table is a privilege on that
        // table, not on the trigger. Without this, any authenticated role
        // could attach a capturing function to another tenant's table and
        // exfiltrate every row the owner later writes.
        self.require_table_ownership(&trigger.table_name, "create a trigger on it")?;
        let function = load_routine(self.db_ref(), RoutineKind::Function, &trigger.function_name)?
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "function \"{}\" does not exist",
                    trigger.function_name
                ))
            })?;
        if function.return_type != "trigger" {
            return Err(SqlError::InvalidSql(format!(
                "function \"{}\" must return trigger",
                trigger.function_name
            )));
        }
        self.require_trigger_function_execute(&trigger.function_name)?;
        self.create_session_trigger_if_missing(trigger, or_replace)?;
        Ok(Some(SqlResult::command("CREATE TRIGGER")))
    }

    pub(crate) fn execute_create_trigger(
        &mut self,
        create_trigger: &CreateTrigger,
    ) -> Result<SqlResult> {
        if create_trigger.temporary {
            return Err(SqlError::Unsupported(
                "temporary triggers are not supported".to_string(),
            ));
        }
        if create_trigger.or_alter {
            return Err(SqlError::Unsupported(
                "CREATE OR ALTER TRIGGER is not supported".to_string(),
            ));
        }
        let table_name = relation_name(&create_trigger.table_name)?;
        if load_schema(self.db_ref(), &table_name)?.is_none()
            && user_collection_names(self.db_ref())
                .iter()
                .all(|name| !name.eq_ignore_ascii_case(&table_name))
        {
            return Err(SqlError::InvalidCollection(table_name));
        }
        self.require_table_ownership(&table_name, "create a trigger on it")?;
        let exec_body = create_trigger.exec_body.as_ref().ok_or_else(|| {
            SqlError::Unsupported("trigger statement bodies are not supported".to_string())
        })?;
        let (_, function_name) = routine_schema_and_name(&object_name(&exec_body.func_desc.name)?);
        let function = load_routine(self.db_ref(), RoutineKind::Function, &function_name)?
            .ok_or_else(|| {
                SqlError::InvalidSql(format!("function \"{function_name}\" does not exist"))
            })?;
        if function.return_type != "trigger" {
            return Err(SqlError::InvalidSql(format!(
                "function \"{function_name}\" must return trigger"
            )));
        }
        self.require_trigger_function_execute(&function_name)?;
        // DEFERRABLE INITIALLY DEFERRED constraint triggers queue on the
        // transaction and fire at COMMIT; everything else fires in-statement.
        let initially_deferred = create_trigger.is_constraint
            && create_trigger
                .characteristics
                .as_ref()
                .is_some_and(|characteristics| {
                    characteristics.deferrable.unwrap_or(false)
                        && matches!(
                            characteristics.initially,
                            Some(sqlparser::ast::DeferrableInitial::Deferred)
                        )
                });
        let mut for_each = trigger_for_each_name(create_trigger);
        if create_trigger.is_constraint && create_trigger.trigger_object.is_none() {
            // PostgreSQL: constraint triggers default to FOR EACH ROW.
            for_each = "row".to_string();
        }
        let definition = create_trigger.to_string();
        let arguments = parse_raw_create_trigger(&definition)?
            .map(|(trigger, _)| trigger.arguments)
            .unwrap_or_default();
        let trigger = TriggerSchema {
            name: relation_name(&create_trigger.name)?,
            table_name,
            function_name,
            arguments,
            definition,
            event: trigger_event_name(create_trigger),
            timing: trigger_timing_name(create_trigger),
            for_each,
            enabled: true,
            enabled_mode: TriggerEnabledMode::Origin,
            is_constraint: create_trigger.is_constraint,
            initially_deferred,
        };
        self.create_session_trigger_if_missing(trigger, create_trigger.or_replace)?;
        Ok(SqlResult::command("CREATE TRIGGER"))
    }

    pub(crate) fn execute_drop_function(
        &mut self,
        drop_function: &DropFunction,
    ) -> Result<SqlResult> {
        let cascade = matches!(
            drop_function.drop_behavior,
            Some(sqlparser::ast::DropBehavior::Cascade)
        );
        for desc in &drop_function.func_desc {
            let name = relation_name(&desc.name)?;
            let existed_before =
                load_routine(self.db_ref(), RoutineKind::Function, &name)?.is_some();
            let dependents = list_user_types(self.db_ref())?
                .into_iter()
                .filter(|user_type| base_type_uses_routine(user_type, &name))
                .collect::<Vec<_>>();
            if let Some(dependent) = dependents.first().filter(|_| !cascade) {
                return Err(SqlError::dependent_objects_still_exist(format!(
                    "cannot drop function {name} because type {}.{} depends on it",
                    dependent.schema_name, dependent.name
                )));
            }
            if cascade {
                let mut dropped = BTreeSet::new();
                for dependent in dependents {
                    self.drop_user_type_definition(&dependent, true, &mut dropped)?;
                }
            }
            let existed =
                delete_routine(self.db_mut()?, RoutineKind::Function, &name)? || existed_before;
            if !existed && !drop_function.if_exists {
                return Err(SqlError::InvalidSql(format!(
                    "function \"{name}\" does not exist"
                )));
            }
        }
        Ok(SqlResult::command("DROP FUNCTION"))
    }

    pub(crate) fn execute_drop_procedure(
        &mut self,
        if_exists: bool,
        proc_desc: &[FunctionDesc],
    ) -> Result<SqlResult> {
        for desc in proc_desc {
            let name = relation_name(&desc.name)?;
            let existed = delete_routine(self.db_mut()?, RoutineKind::Procedure, &name)?;
            if !existed && !if_exists {
                return Err(SqlError::InvalidSql(format!(
                    "procedure \"{name}\" does not exist"
                )));
            }
        }
        Ok(SqlResult::command("DROP PROCEDURE"))
    }

    pub(crate) fn execute_drop_trigger(&mut self, drop_trigger: &DropTrigger) -> Result<SqlResult> {
        let name = relation_name(&drop_trigger.trigger_name)?;
        let table_name = drop_trigger
            .table_name
            .as_ref()
            .map(relation_name)
            .transpose()?;
        // Dropping a trigger is likewise an ownership privilege on the table
        // it guards: an unprivileged role must not be able to disarm another
        // tenant's audit or integrity triggers.
        for owned_table in self.trigger_tables_for_drop(&name, table_name.as_deref())? {
            self.require_table_ownership(&owned_table, "drop a trigger on it")?;
        }
        let existed = self.delete_session_trigger(&name, table_name.as_deref())?;
        if !existed && !drop_trigger.if_exists {
            return Err(SqlError::InvalidSql(format!(
                "trigger \"{name}\" does not exist"
            )));
        }
        Ok(SqlResult::command("DROP TRIGGER"))
    }

    /// EXECUTE authority on the function a trigger fires. Trigger bodies run
    /// with the writing session's data in NEW/OLD; binding one requires the
    /// same right as calling it directly.
    pub(crate) fn require_trigger_function_execute(&self, function_name: &str) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_can_execute_routine(self.db_ref(), &role, function_name)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for function {function_name}"
        ))))
    }

    /// Tables whose triggers a DROP TRIGGER would remove. When the statement
    /// names a table, that table alone; otherwise every table carrying a
    /// trigger of that name (the name-only form the engine also accepts).
    fn trigger_tables_for_drop(&self, name: &str, table: Option<&str>) -> Result<Vec<String>> {
        if let Some(table) = table {
            return Ok(vec![table.to_string()]);
        }
        Ok(list_triggers(self.db_ref())?
            .into_iter()
            .filter(|trigger| trigger.name.eq_ignore_ascii_case(name))
            .map(|trigger| trigger.table_name)
            .collect())
    }

    pub(crate) fn execute_insert(&mut self, insert: &Insert) -> Result<SqlResult> {
        self.execute_insert_with_ctes(insert, BTreeMap::new())
    }

    pub(crate) fn execute_insert_with_ctes(
        &mut self,
        insert: &Insert,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        let TableObject::TableName(table_name) = &insert.table else {
            return Err(SqlError::Unsupported(
                "INSERT supports only table names".to_string(),
            ));
        };
        let table =
            resolve_session_relation_name_if_exists(self.db_ref(), &relation_name(table_name)?);
        self.require_table_privilege(&table, "INSERT")?;
        if insert.returning.is_some() {
            self.require_table_privilege(&table, "SELECT")?;
        }
        if load_view(self.db_ref(), &table)?.is_some() {
            return Err(SqlError::Unsupported(
                "INSERT into views is not supported".to_string(),
            ));
        }
        let schema = load_schema_shared(self.db_ref(), &table)?;
        let mut columns = insert
            .columns
            .iter()
            .map(object_name)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|name| name.rsplit('.').next().unwrap_or(&name).to_string())
            .collect::<Vec<_>>();
        // Rows built through the insert template carry their cast values:
        // the local constraints are checked on those instead of decoding the
        // freshly encoded row, unless a BEFORE trigger may have changed it.
        let mut prechecked: Option<(Rc<InsertValuesTemplate>, Vec<Vec<Option<SqlValue>>>)> = None;
        let mut records = if insert.source.is_none() {
            if !columns.is_empty() {
                return Err(SqlError::InvalidSql(
                    "INSERT DEFAULT VALUES does not accept a column list".to_string(),
                ));
            }
            self.records_from_insert_default_values(&table, schema.as_deref())?
        } else {
            if columns.is_empty() {
                let Some(schema) = schema.as_deref() else {
                    return Err(SqlError::Unsupported(
                        "INSERT without a column list requires a table schema".to_string(),
                    ));
                };
                columns = schema
                    .columns
                    .iter()
                    .filter(|column| !column.hidden && column.generated_expr.is_none())
                    .map(|column| column.name.clone())
                    .collect();
            }
            let source = insert.source.as_ref().expect("checked source above");
            if let Some(schema) = schema.as_deref() {
                for column in &columns {
                    ensure_schema_column(&table, schema, column)?;
                }
                validate_generated_always_insert(schema, &columns, source)?;
            }
            // Stored-form INSERT (typed resident rows phase 3): the template
            // renders the row's JSON text directly and the row is buffered in
            // its resident form; nothing here builds a Value tree.
            if let (SetExpr::Values(values), Some(schema)) = (source.body.as_ref(), schema.as_ref())
            {
                if source.with.is_none() && ctes.is_empty() {
                    if let Some(result) =
                        self.try_execute_insert_stored(insert, &table, schema, &columns, values)?
                    {
                        return Ok(result);
                    }
                }
            }
            // INSERT ... SELECT: the SELECT's rows through the same stored form
            // (the template built for the column list, no expressions);
            // eligibility and the template are settled before the query runs.
            if let (SetExpr::Select(_), Some(schema)) = (source.body.as_ref(), schema.as_ref()) {
                if ctes.is_empty() && self.stored_insert_eligible(insert, &table, schema)? {
                    if let Some(template) =
                        self.stored_insert_rows_template(schema, &columns, source)
                    {
                        let result = self.execute_query(source)?;
                        return self.insert_stored_rows(
                            &table,
                            schema,
                            &columns,
                            &template,
                            result.rows,
                        );
                    }
                }
            }
            let templated = match (source.body.as_ref(), schema.as_ref()) {
                (SetExpr::Values(values), Some(schema))
                    if source.with.is_none() && ctes.is_empty() =>
                {
                    self.records_from_insert_values_template(&table, schema, &columns, values)?
                }
                _ => None,
            };
            match templated {
                Some((records, template, cells)) => {
                    prechecked = Some((template, cells));
                    records
                }
                None => self.records_from_insert_source_with_ctes(
                    &table,
                    schema.as_deref(),
                    &columns,
                    source,
                    ctes,
                )?,
            }
        };
        if let Some(schema) = schema.as_deref() {
            self.materialize_generated_columns(&table, schema, &mut records)?;
        }
        // BEFORE INSERT row triggers run before conflict arbitration and
        // constraint validation, on the exact rows about to be written.
        let triggered;
        (records, triggered) =
            self.apply_before_insert_row_triggers_reporting(&table, schema.as_deref(), records)?;
        if triggered {
            prechecked = None;
        }
        let count = records.len();

        if let Some(on_insert) = &insert.on {
            return self.execute_insert_on_conflict(
                &table,
                schema.as_deref(),
                records,
                on_insert,
                insert.returning.as_deref(),
            );
        }
        if let Some(schema) = schema.as_deref() {
            match prechecked
                .as_ref()
                .filter(|(_, cells)| cells.len() == records.len())
            {
                Some((template, cells)) => {
                    for (record, cells) in records.iter().zip(cells) {
                        template.check_row(&table, record, cells)?;
                    }
                    validate_records_for_write_prechecked(
                        self.db_ref(),
                        self.tx.as_ref(),
                        &table,
                        schema,
                        &records,
                        false,
                    )?;
                }
                None => validate_records_for_write(
                    self.db_ref(),
                    self.tx.as_ref(),
                    &table,
                    schema,
                    &records,
                    false,
                )?,
            }
        }
        self.enforce_rls_checks(&table, schema.as_deref(), PolicyAction::Insert, &records)?;
        self.insert_session_records(&table, records.clone())?;
        self.fire_after_insert_triggers(&table, schema.as_deref(), &records)?;
        self.insert_result(
            &table,
            schema.as_deref(),
            insert.returning.as_deref(),
            &records,
            count,
        )
    }

    /// Whether an INSERT into `table` may take the stored-form path (see
    /// `try_execute_insert_stored`); independent of the source rows.
    fn stored_insert_eligible(
        &self,
        insert: &Insert,
        table: &str,
        schema: &Arc<TableSchema>,
    ) -> Result<bool> {
        if !stored_update::enabled()
            || self.tx.is_none()
            || insert.on.is_some()
            || insert.returning.is_some()
            || schema.rls_enabled
            || !schema
                .constraints
                .iter()
                .all(|constraint| matches!(constraint, ConstraintSchema::Unique { .. }))
            || schema
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some())
        {
            return Ok(false);
        }
        let arbiters = unique_arbiters_for_table(self.db_ref(), table, schema);
        let primary_key_only = match arbiters.as_slice() {
            [] => true,
            [arbiter] => {
                primary_key_columns_for_unique_arbiter(schema, arbiter).is_some()
                    && !primary_key_requires_typed_identity(schema)
            }
            _ => false,
        };
        if !primary_key_only {
            return Ok(false);
        }
        if self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required)
            || index_definitions_shared(self.db_ref()).iter().any(|index| {
                index.collection == table
                    && matches!(
                        index.kind,
                        IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array
                    )
            })
            || self.has_row_triggers(table, "before", "insert")?
            || self.has_row_triggers(table, "after", "insert")?
            || self.has_extension_database_event_bindings(table, DatabaseOperation::Insert)?
        {
            return Ok(false);
        }
        Ok(true)
    }

    /// The rows template for an INSERT ... SELECT, kept per IR node of the
    /// source query; `None` when the table or column list declines it.
    fn stored_insert_rows_template(
        &self,
        schema: &Arc<TableSchema>,
        columns: &[String],
        source: &Query,
    ) -> Option<Rc<InsertValuesTemplate>> {
        let key = self
            .sql_engine()
            .ir_plan_node_key(source as *const Query as usize)?;
        match sql_insert_plan_node_cache_get(key) {
            Some(Some(template)) if Arc::ptr_eq(&template.schema, schema) => Some(template),
            Some(None) => None,
            _ => {
                let built = InsertValuesTemplate::build_for_rows(schema, columns).map(Rc::new);
                sql_insert_plan_node_cache_set(key, built.clone());
                built
            }
        }
    }

    /// Already-evaluated rows (INSERT ... SELECT) through the stored form:
    /// build, NOT NULL, primary-key probe, buffer.
    fn insert_stored_rows(
        &mut self,
        table: &str,
        schema: &Arc<TableSchema>,
        columns: &[String],
        template: &InsertValuesTemplate,
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<SqlResult> {
        let mut built = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if row.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "INSERT expected {} values, got {}",
                    columns.len(),
                    row.len()
                )));
            }
            built.push(template.build_stored(table, row)?);
        }
        self.write_stored_insert_rows(table, schema, template, built)
    }

    /// NOT NULL, the primary-key probe and the buffered write for built
    /// stored rows (shared by the VALUES and SELECT stored paths).
    fn write_stored_insert_rows(
        &mut self,
        table: &str,
        schema: &Arc<TableSchema>,
        template: &InsertValuesTemplate,
        rows: Vec<(String, String, Vec<Option<SqlValue>>)>,
    ) -> Result<SqlResult> {
        let arbiters = unique_arbiters_for_table(self.db_ref(), table, schema);
        for (_, _, cells) in &rows {
            template.check_cells_not_null(table, cells)?;
        }
        if let [arbiter] = arbiters.as_slice() {
            let mut seen = BTreeSet::new();
            for (id, _, _) in &rows {
                if !seen.insert(id.as_str()) {
                    return Err(unique_violation(&arbiter.name));
                }
                sql_profile_index_lookup();
            }
            if let Some(tx) = self.tx.as_ref() {
                let existing = tx.contains_for_integrity_check(
                    table,
                    rows.iter().map(|(id, _, _)| id.as_str()),
                )?;
                if existing.into_iter().any(|exists| exists) {
                    return Err(unique_violation(&arbiter.name));
                }
            } else {
                for (id, _, _) in &rows {
                    if self.db_ref().get_unchecked(table, id)?.is_some() {
                        return Err(unique_violation(&arbiter.name));
                    }
                }
            }
        }
        let count = rows.len();
        let stored = rows
            .into_iter()
            .map(|(id, text, _)| {
                Ok((
                    Arc::new(
                        bicdb_core::StoredRecord::from_parts(id, text).map_err(SqlError::from)?,
                    ),
                    0u64,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // With a primary-key arbiter the preceding absence check and commit's
        // atomic unique-key claim make statement-time row locks redundant.
        self.insert_session_stored_records(table, stored, arbiters.len() == 1)?;
        #[cfg(test)]
        SQL_STORED_INSERT_HITS.with(|hits| *hits.borrow_mut() += 1);
        Ok(SqlResult::command(format!("INSERT 0 {count}")))
    }

    /// INSERT ... VALUES written in the stored form, for the simple shape: a
    /// transaction; no ON CONFLICT or RETURNING; a table with only unique
    /// constraints whose sole unique arbiter is the primary key (or none),
    /// no generated columns, RLS, protection policy, full-text/JSONB/array
    /// projections, INSERT row triggers or extension bindings; and a row the
    /// insert template models. `None` (before any write) runs the generic path.
    fn try_execute_insert_stored(
        &mut self,
        insert: &Insert,
        table: &str,
        schema: &Arc<TableSchema>,
        columns: &[String],
        values: &sqlparser::ast::Values,
    ) -> Result<Option<SqlResult>> {
        if !self.stored_insert_eligible(insert, table, schema)? {
            return Ok(None);
        }
        let Some(key) = self
            .sql_engine()
            .ir_plan_node_key(values as *const sqlparser::ast::Values as usize)
        else {
            return Ok(None);
        };
        let template = match sql_insert_plan_node_cache_get(key) {
            Some(Some(template)) if Arc::ptr_eq(&template.schema, schema) => Some(template),
            Some(None) => None,
            _ => {
                let (scope, _) = self.sql_engine().bound_row_context(&[]);
                let built =
                    InsertValuesTemplate::build(schema, columns, values, &scope).map(Rc::new);
                sql_insert_plan_node_cache_set(key, built.clone());
                built
            }
        };
        let Some(template) = template else {
            return Ok(None);
        };
        let var_values = self.sql_engine().bound_row_context(&[]).1.var_values;
        let mut rows = Vec::with_capacity(values.rows.len());
        for (idx, (row, bound_row)) in values.rows.iter().zip(&template.rows).enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut evaluated = Vec::with_capacity(row.len());
            for (expr, bound) in row.iter().zip(bound_row) {
                evaluated.push(match bound {
                    Some(bound) => bound.eval(&BoundExprFrame {
                        db: self.db_ref(),
                        columns: BoundExprColumns::Values(&[]),
                        vars: &var_values,
                        user_calls: &[],
                    })?,
                    None => self.eval_session_expr(expr)?,
                });
            }
            let (id, text, cells) = template.build_stored(table, evaluated)?;
            rows.push((id, text, cells));
        }
        self.write_stored_insert_rows(table, schema, &template, rows)
            .map(Some)
    }

    /// Routine-owned `INSERT ... VALUES` through the per-IR-node template
    /// (`InsertValuesTemplate`): `None` when the statement or the table
    /// declines it and the generic path runs. The cast column values of every
    /// row come back with the records for the caller's local-constraint check.
    #[allow(clippy::type_complexity)]
    pub(crate) fn records_from_insert_values_template(
        &mut self,
        table: &str,
        schema: &Arc<TableSchema>,
        columns: &[String],
        values: &sqlparser::ast::Values,
    ) -> Result<
        Option<(
            Vec<Record>,
            Rc<InsertValuesTemplate>,
            Vec<Vec<Option<SqlValue>>>,
        )>,
    > {
        if !ir_insert_plan::enabled() {
            return Ok(None);
        }
        let Some(key) = self
            .sql_engine()
            .ir_plan_node_key(values as *const sqlparser::ast::Values as usize)
        else {
            return Ok(None);
        };
        let template = match sql_insert_plan_node_cache_get(key) {
            Some(Some(template)) if Arc::ptr_eq(&template.schema, schema) => Some(template),
            Some(None) => None,
            _ => {
                let (scope, _) = self.sql_engine().bound_row_context(&[]);
                let built =
                    InsertValuesTemplate::build(schema, columns, values, &scope).map(Rc::new);
                sql_insert_plan_node_cache_set(key, built.clone());
                built
            }
        };
        let Some(template) = template else {
            return Ok(None);
        };
        let var_values = self.sql_engine().bound_row_context(&[]).1.var_values;
        let mut records = Vec::with_capacity(values.rows.len());
        let mut cells = Vec::with_capacity(values.rows.len());
        for (idx, (row, bound_row)) in values.rows.iter().zip(&template.rows).enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut evaluated = Vec::with_capacity(row.len());
            for (expr, bound) in row.iter().zip(bound_row) {
                evaluated.push(match bound {
                    Some(bound) => bound.eval(&BoundExprFrame {
                        db: self.db_ref(),
                        columns: BoundExprColumns::Values(&[]),
                        vars: &var_values,
                        user_calls: &[],
                    })?,
                    None => self.eval_session_expr(expr)?,
                });
            }
            let (record, row_cells) = template.build_record(table, evaluated)?;
            records.push(record);
            cells.push(row_cells);
        }
        Ok(Some((records, template, cells)))
    }

    pub(crate) fn records_from_insert_default_values(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Record>> {
        let defaults = self.bulk_missing_insert_defaults(schema, &[], 1)?;
        let mut fields = BTreeMap::new();
        self.apply_bulk_insert_defaults(schema, &defaults, 0, &mut fields)?;
        Ok(vec![record_from_fields_with_db(
            self.db_ref(),
            table,
            schema,
            fields,
        )?])
    }

    pub(crate) fn records_from_insert_source_with_ctes(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        columns: &[String],
        source: &Query,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<Vec<Record>> {
        if let SetExpr::Values(values) = source.body.as_ref() {
            if source.with.is_some() || !ctes.is_empty() {
                return Err(SqlError::Unsupported(
                    "CTEs feeding INSERT VALUES are not supported".to_string(),
                ));
            }
            return self.records_from_insert_values(table, schema, columns, values);
        }

        if matches!(source.body.as_ref(), SetExpr::Select(_)) {
            let result = if ctes.is_empty() {
                self.execute_query(source)?
            } else {
                self.sql_engine_with_ctes(ctes).execute_query(source)?
            };
            return self.records_from_insert_result(table, schema, columns, result.rows);
        }

        Err(SqlError::Unsupported(
            "INSERT supports only VALUES or SELECT source".to_string(),
        ))
    }

    pub(crate) fn records_from_insert_values(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        columns: &[String],
        values: &sqlparser::ast::Values,
    ) -> Result<Vec<Record>> {
        let defaults = self.bulk_missing_insert_defaults(schema, columns, values.rows.len())?;
        let mut records = Vec::new();
        for (idx, row) in values.rows.iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if row.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "INSERT expected {} values, got {}",
                    columns.len(),
                    row.len()
                )));
            }
            let mut fields = BTreeMap::new();
            for (column, expr) in columns.iter().zip(row.iter()) {
                let value = if expr_is_default(expr) {
                    self.default_value_for_column(schema, column)?
                        .unwrap_or(SqlValue::Null)
                } else {
                    self.eval_session_expr(expr)?
                };
                fields.insert(column.clone(), value);
            }
            self.apply_bulk_insert_defaults(schema, &defaults, idx, &mut fields)?;
            records.push(record_from_fields_with_db(
                self.db_ref(),
                table,
                schema,
                fields,
            )?);
        }
        Ok(records)
    }

    pub(crate) fn records_from_insert_result(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        columns: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<Vec<Record>> {
        let defaults = self.bulk_missing_insert_defaults(schema, columns, rows.len())?;
        let mut records = Vec::new();
        for (idx, row) in rows.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if row.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "INSERT expected {} values, got {}",
                    columns.len(),
                    row.len()
                )));
            }
            let mut fields = BTreeMap::new();
            for (column, value) in columns.iter().zip(row) {
                fields.insert(column.clone(), value);
            }
            self.apply_bulk_insert_defaults(schema, &defaults, idx, &mut fields)?;
            records.push(record_from_fields_with_db(
                self.db_ref(),
                table,
                schema,
                fields,
            )?);
        }
        Ok(records)
    }

    pub(crate) fn bulk_missing_insert_defaults(
        &mut self,
        schema: Option<&TableSchema>,
        insert_columns: &[String],
        row_count: usize,
    ) -> Result<Vec<BulkInsertDefault>> {
        let Some(schema) = schema else {
            return Ok(Vec::new());
        };
        let mut defaults = Vec::new();
        for column in &schema.columns {
            if insert_columns.iter().any(|name| name == &column.name) {
                continue;
            }
            if let Some(sequence) = column.default_sequence.as_ref() {
                defaults.push(BulkInsertDefault {
                    column: column.name.clone(),
                    values: BulkInsertDefaultValues::Sequence(self.nextvals(sequence, row_count)?),
                });
            } else if let Some(value) = column.default_value.clone() {
                defaults.push(BulkInsertDefault {
                    column: column.name.clone(),
                    values: BulkInsertDefaultValues::Static(value),
                });
            } else if let Some(expr) = column.effective_default_expr() {
                defaults.push(BulkInsertDefault {
                    column: column.name.clone(),
                    values: BulkInsertDefaultValues::Expr {
                        expr: Self::parse_default_expr(&expr)?,
                        pg_type: column.pg_type.clone(),
                    },
                });
            }
        }
        Ok(defaults)
    }

    pub(crate) fn apply_bulk_insert_defaults(
        &mut self,
        schema: Option<&TableSchema>,
        defaults: &[BulkInsertDefault],
        row_idx: usize,
        fields: &mut BTreeMap<String, SqlValue>,
    ) -> Result<()> {
        if defaults.is_empty() {
            self.fill_missing_insert_defaults(schema, fields)?;
            return Ok(());
        }
        for default in defaults {
            if fields.keys().any(|name| name == &default.column) {
                continue;
            }
            let value = match &default.values {
                BulkInsertDefaultValues::Sequence(values) => {
                    values.get(row_idx).copied().map(SqlValue::Int)
                }
                BulkInsertDefaultValues::Static(value) => Some(value.clone()),
                BulkInsertDefaultValues::Expr { expr, pg_type } => {
                    Some(self.eval_parsed_default_expr(expr, pg_type)?)
                }
            };
            if let Some(value) = value {
                fields.insert(default.column.clone(), value);
            }
        }
        Ok(())
    }

    pub(crate) fn fill_missing_insert_defaults(
        &mut self,
        schema: Option<&TableSchema>,
        fields: &mut BTreeMap<String, SqlValue>,
    ) -> Result<()> {
        let Some(schema) = schema else {
            return Ok(());
        };
        for column in &schema.columns {
            if fields.keys().any(|name| name == &column.name) {
                continue;
            }
            if let Some(value) = self.default_value_for_column(Some(schema), &column.name)? {
                fields.insert(column.name.clone(), value);
            }
        }
        Ok(())
    }

    pub(crate) fn execute_insert_on_conflict(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
        on_insert: &OnInsert,
        returning: Option<&[SelectItem]>,
    ) -> Result<SqlResult> {
        let OnInsert::OnConflict(on_conflict) = on_insert else {
            return Err(SqlError::Unsupported(
                "INSERT ON DUPLICATE KEY UPDATE is not supported".to_string(),
            ));
        };
        if matches!(on_conflict.action, OnConflictAction::DoNothing) {
            if on_conflict.conflict_target.is_none() {
                return self.execute_insert_on_conflict_do_nothing_all_unique(
                    table, schema, records, returning,
                );
            }
            let conflict_columns =
                conflict_target_columns(table, schema, on_conflict.conflict_target.as_ref())?;
            return self.execute_insert_on_conflict_do_nothing(
                table,
                schema,
                records,
                &conflict_columns,
                returning,
            );
        }
        if let OnConflictAction::DoUpdate(update) = &on_conflict.action {
            self.require_update_privileges(table, &update.assignments)?;
        }
        let conflict_columns =
            conflict_target_columns(table, schema, on_conflict.conflict_target.as_ref())?;
        let mut affected_records = Vec::new();
        let mut inserted_records = Vec::new();

        for (idx, record) in records.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if let Some(existing) =
                self.find_conflict_record(table, schema, &record, &conflict_columns)?
            {
                match &on_conflict.action {
                    OnConflictAction::DoNothing => {}
                    OnConflictAction::DoUpdate(update) => {
                        let Some(schema) = schema else {
                            return Err(SqlError::Unsupported(
                                "ON CONFLICT DO UPDATE requires a table schema".to_string(),
                            ));
                        };
                        // Unlike a plain UPDATE (which silently skips rows
                        // failing USING), PostgreSQL errors when the
                        // conflicting existing row is not updatable.
                        if let PreparedRls::Filter(filter) = prepare_rls_with_schema(
                            self.db_ref(),
                            table,
                            PolicyAction::Update,
                            Some(schema),
                            &self.session_gucs,
                            self.security_context.as_ref(),
                            false,
                        )? {
                            if !filter.allows(&self.sql_engine(), schema, &existing)? {
                                return Err(rls_using_denied(table));
                            }
                        }
                        let mut update_row =
                            row_from_record(table, table, Some(schema), &existing)?;
                        update_row = merge_rows(
                            &update_row,
                            &row_from_record("excluded", "excluded", Some(schema), &record)?,
                        );
                        let row_engine = self.row_engine();
                        if let Some(selection) = &update.selection {
                            if !row_engine
                                .eval_row_truth(&update_row, selection)?
                                .unwrap_or(false)
                            {
                                continue;
                            }
                        }

                        let mut updated = existing.clone();
                        for assignment in &update.assignments {
                            let AssignmentTarget::ColumnName(column_name) = &assignment.target
                            else {
                                return Err(SqlError::Unsupported(
                                    "ON CONFLICT DO UPDATE supports only single-column assignments"
                                        .to_string(),
                                ));
                            };
                            let column = relation_name(column_name)?;
                            ensure_schema_column(table, schema, &column)?;
                            if schema
                                .column(&column)
                                .is_some_and(|column| column.generated_expr.is_some())
                            {
                                if expr_is_default(&assignment.value) {
                                    continue;
                                }
                                return Err(SqlError::generated_always_violation(format!(
                                    "column \"{column}\" can only be updated to DEFAULT"
                                )));
                            }
                            let mut value =
                                row_engine.eval_row_value(&update_row, &assignment.value)?;
                            if schema
                                .column(&column)
                                .is_some_and(|column| column.pg_type == "oid")
                            {
                                if let Some(cast) = explicit_oid_cast(
                                    value.clone(),
                                    &assignment.value,
                                    Some(schema),
                                ) {
                                    value = cast?;
                                }
                            }
                            let value = resolve_oid_alias_column_value(
                                self.db_ref(),
                                schema,
                                &column,
                                value,
                            )?;
                            set_record_column(&mut updated, Some(schema), &column, value)?;
                        }
                        self.materialize_generated_columns(
                            table,
                            schema,
                            std::slice::from_mut(&mut updated),
                        )?;
                        // ON CONFLICT DO UPDATE takes the update path, so
                        // BEFORE/AFTER UPDATE row triggers apply — the same
                        // guards a plain UPDATE of the row would run.
                        let Some(updated) = self.apply_before_update_row_triggers(
                            table,
                            Some(schema),
                            &existing,
                            updated,
                        )?
                        else {
                            continue;
                        };
                        validate_records_for_write(
                            self.db_ref(),
                            self.tx.as_ref(),
                            table,
                            schema,
                            &[updated.clone()],
                            true,
                        )?;
                        self.apply_foreign_key_parent_update(table, schema, &existing, &updated)?;
                        self.enforce_rls_check(table, PolicyAction::Update, &updated)?;
                        self.update_session_record(table, updated.clone())?;
                        self.fire_after_row_triggers(
                            table,
                            Some(schema),
                            "update",
                            std::iter::once((Some(&existing), Some(&updated))),
                        )?;
                        affected_records.push(updated);
                    }
                }
                continue;
            }

            if let Some(schema) = schema {
                validate_records_for_write(
                    self.db_ref(),
                    self.tx.as_ref(),
                    table,
                    schema,
                    std::slice::from_ref(&record),
                    false,
                )?;
            }
            self.enforce_rls_checks(
                table,
                schema,
                PolicyAction::Insert,
                std::slice::from_ref(&record),
            )?;
            self.insert_session_records(table, vec![record.clone()])?;
            inserted_records.push(record.clone());
            affected_records.push(record);
        }

        self.fire_after_insert_triggers(table, schema, &inserted_records)?;
        self.insert_result(
            table,
            schema,
            returning,
            &affected_records,
            affected_records.len(),
        )
    }

    pub(crate) fn execute_insert_on_conflict_do_nothing(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
        conflict_columns: &[String],
        returning: Option<&[SelectItem]>,
    ) -> Result<SqlResult> {
        let inserted_records =
            self.non_conflicting_insert_records(table, schema, records, conflict_columns)?;
        if let Some(schema) = schema {
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                table,
                schema,
                &inserted_records,
                false,
            )?;
        }
        self.enforce_rls_checks(table, schema, PolicyAction::Insert, &inserted_records)?;
        self.insert_session_records(table, inserted_records.clone())?;
        self.fire_after_insert_triggers(table, schema, &inserted_records)?;
        self.insert_result(
            table,
            schema,
            returning,
            &inserted_records,
            inserted_records.len(),
        )
    }

    pub(crate) fn execute_insert_on_conflict_do_nothing_all_unique(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
        returning: Option<&[SelectItem]>,
    ) -> Result<SqlResult> {
        let inserted_records =
            self.non_conflicting_insert_records_all_unique(table, schema, records)?;
        if let Some(schema) = schema {
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                table,
                schema,
                &inserted_records,
                false,
            )?;
        }
        self.enforce_rls_checks(table, schema, PolicyAction::Insert, &inserted_records)?;
        self.insert_session_records(table, inserted_records.clone())?;
        self.fire_after_insert_triggers(table, schema, &inserted_records)?;
        self.insert_result(
            table,
            schema,
            returning,
            &inserted_records,
            inserted_records.len(),
        )
    }

    pub(crate) fn non_conflicting_insert_records(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
        conflict_columns: &[String],
    ) -> Result<Vec<Record>> {
        let Some(schema) = schema else {
            return Ok(records);
        };
        let Some(arbiter) = self.memoized_unique_arbiter(table, schema, conflict_columns)? else {
            let existing_records =
                self.scan_session_records_for_action(table, PolicyAction::Update)?;
            sql_profile_full_scan();
            return non_conflicting_records_from_scan(
                schema,
                &UniqueConflictArbiter {
                    name: conflict_columns.join("_"),
                    key: UniqueKey::Columns(conflict_columns.to_vec()),
                    index_name: None,
                },
                records,
                &existing_records,
            );
        };
        non_conflicting_records(
            self.db_ref(),
            self.tx.as_ref(),
            table,
            schema,
            &arbiter,
            records,
        )
    }

    pub(crate) fn non_conflicting_insert_records_all_unique(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        mut records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        let Some(schema) = schema else {
            return Ok(records);
        };
        for arbiter in unique_arbiters_for_table(self.db_ref(), table, schema).iter() {
            records = non_conflicting_records(
                self.db_ref(),
                self.tx.as_ref(),
                table,
                schema,
                &arbiter,
                records,
            )?;
            if records.is_empty() {
                break;
            }
        }
        Ok(records)
    }

    pub(crate) fn find_conflict_record(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        record: &Record,
        conflict_columns: &[String],
    ) -> Result<Option<Record>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let key = record_column_values(record, schema, conflict_columns);
        if key.iter().any(|value| matches!(value, SqlValue::Null)) {
            return Ok(None);
        }
        if let Some(arbiter) = self.memoized_unique_arbiter(table, schema, conflict_columns)? {
            if let Some(columns) = primary_key_columns_for_unique_arbiter(schema, &arbiter) {
                if !primary_key_requires_typed_identity(schema) {
                    sql_profile_index_lookup();
                    let record_id = record_id_from_column_values(table, schema, &columns, &key)?;
                    return self.conflict_record_by_id(table, &record_id);
                }
            }

            let key_values = unique_key_values(record, schema, &arbiter.key);
            let contains_json = key_values
                .iter()
                .any(|value| matches!(value, SqlValue::Json(_)));
            if let Some(index_name) = arbiter.index_name.as_ref().filter(|_| !contains_json) {
                sql_profile_index_lookup();
                let index_values = unique_key_index_values(schema, &arbiter.key, &key_values)?;
                let mut candidate_ids = self
                    .db_ref()
                    .lookup_index_exact(index_name, &index_values)
                    .map_err(SqlError::from)?;

                // Executable indexes contain committed state, so try those
                // candidates FIRST and re-verify each through the transaction
                // view (which applies pending writes, so a row this
                // transaction deleted or re-keyed correctly fails to match).
                //
                // Only when no committed candidate matches do we consult the
                // transaction's pending delta, which covers the remaining
                // cases: this transaction inserted a matching key, or changed
                // some row's key INTO this one. Read-your-writes is preserved.
                //
                // Merging the delta unconditionally (the previous shape) cost
                // a linear scan of the transaction's whole write set plus a
                // full Record clone — payload JSON included — per candidate,
                // per input row. In a multi-row upsert every processed row
                // appends a write, so a batch degraded to O(batch^2) clones,
                // and a long-lived transaction made it O(batch x rows): the
                // conflict-heavy bulk upsert that stalled past its 30s write
                // timeout on a large collection.
                candidate_ids.sort();
                candidate_ids.dedup();
                for candidate_id in &candidate_ids {
                    self.cancellation.check()?;
                    if let Some(existing) = self.conflict_record_by_id(table, candidate_id)? {
                        if record_column_values(&existing, schema, conflict_columns) == key {
                            return Ok(Some(existing));
                        }
                    }
                }

                if let Some(tx) = self.tx.as_ref() {
                    let committed: std::collections::HashSet<&String> =
                        candidate_ids.iter().collect();
                    for candidate_id in tx.pending_record_ids_for_collection(table) {
                        if committed.contains(&candidate_id) {
                            continue;
                        }
                        self.cancellation.check()?;
                        if let Some(existing) = self.conflict_record_by_id(table, &candidate_id)? {
                            if record_column_values(&existing, schema, conflict_columns) == key {
                                return Ok(Some(existing));
                            }
                        }
                    }
                }
                return Ok(None);
            }
        }
        // Conflict detection is index-level and independent of RLS, like
        // PostgreSQL: a policy-hidden conflicting row must surface here so
        // DO UPDATE can raise the USING-expression violation instead of a
        // duplicate-key error.
        let records = match self.security_context.as_ref() {
            Some(ctx) => match self.tx.as_ref() {
                Some(tx) => tx
                    .scan_collection_with_context(ctx, table)
                    .map_err(SqlError::from),
                None => self
                    .db_ref()
                    .scan_collection_with_context(ctx, table)
                    .map_err(SqlError::from),
            },
            None => match self.tx.as_ref() {
                Some(tx) => tx.scan_collection(table).map_err(SqlError::from),
                None => self.db_ref().scan_collection(table).map_err(SqlError::from),
            },
        }?;
        for existing in records {
            if record_column_values(&existing, schema, conflict_columns) == key {
                return Ok(Some(existing));
            }
        }
        Ok(None)
    }

    /// `unique_arbiter_for_columns`, memoized for the current statement.
    pub(crate) fn memoized_unique_arbiter(
        &self,
        table: &str,
        schema: &TableSchema,
        conflict_columns: &[String],
    ) -> Result<Option<records::UniqueConflictArbiter>> {
        if let Some((memo_table, memo_columns, arbiter)) =
            self.conflict_arbiter_memo.borrow().as_ref()
        {
            if memo_table == table && memo_columns.as_slice() == conflict_columns {
                return Ok(arbiter.clone());
            }
        }
        let arbiter = unique_arbiter_for_columns(self.db_ref(), table, schema, conflict_columns)?;
        *self.conflict_arbiter_memo.borrow_mut() = Some((
            table.to_string(),
            conflict_columns.to_vec(),
            arbiter.clone(),
        ));
        Ok(arbiter)
    }

    pub(crate) fn conflict_record_by_id(
        &self,
        table: &str,
        record_id: &str,
    ) -> Result<Option<Record>> {
        let existing = match self.security_context.as_ref() {
            Some(ctx) => match self.tx.as_ref() {
                Some(tx) => tx
                    .get_with_context(ctx, table, record_id)
                    .map_err(SqlError::from),
                None => self
                    .db_ref()
                    .get_with_context(ctx, table, record_id)
                    .map_err(SqlError::from),
            },
            None => match self.tx.as_ref() {
                Some(tx) => tx.get(table, record_id).map_err(SqlError::from),
                None => self.db_ref().get(table, record_id).map_err(SqlError::from),
            },
        }?;
        Ok(existing.map(|record| record.as_ref().clone()))
    }

    pub(crate) fn insert_result(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        returning: Option<&[SelectItem]>,
        records: &[Record],
        count: usize,
    ) -> Result<SqlResult> {
        if let Some(returning) = returning {
            return self.project_returning_records(table, schema, returning, records);
        }
        Ok(SqlResult::command(format!("INSERT 0 {count}")))
    }

    pub(crate) fn project_returning_records(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        returning: &[SelectItem],
        records: &[Record],
    ) -> Result<SqlResult> {
        self.project_returning_records_with_alias(table, table, schema, returning, records)
    }

    pub(crate) fn project_returning_records_with_alias(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        returning: &[SelectItem],
        records: &[Record],
    ) -> Result<SqlResult> {
        if let Some(result) =
            self.project_direct_returning_records(table, alias, schema, returning, records)?
        {
            return Ok(result);
        }
        let rows = records
            .iter()
            .map(|record| row_from_record(table, alias, schema, record))
            .collect::<Result<Vec<_>>>()?;
        let wildcard_columns = FieldRef::wildcard(schema)
            .into_iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        let result =
            self.row_engine()
                .project_sql_row_select(returning, &rows, &wildcard_columns)?;
        Ok(result
            .with_column_types(returning_projection_column_types(returning, schema))
            .with_column_metadata(returning_projection_column_metadata(
                self.db_ref(),
                table,
                returning,
                schema,
            )))
    }

    pub(crate) fn project_update_from_returning_slot_rows(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        returning: &[SelectItem],
        rows: &[SlotRow],
        row_columns: &[String],
        target_columns: &[String],
    ) -> Result<SqlResult> {
        Ok(self
            .row_engine()
            .project_slot_row_select_with_wildcard(returning, rows, row_columns, target_columns)?
            .with_column_types(returning_projection_column_types(returning, schema))
            .with_column_metadata(returning_projection_column_metadata(
                self.db_ref(),
                table,
                returning,
                schema,
            )))
    }

    pub(crate) fn project_direct_returning_records(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        returning: &[SelectItem],
        records: &[Record],
    ) -> Result<Option<SqlResult>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let mut columns = Vec::with_capacity(returning.len());
        let mut expressions = Vec::with_capacity(returning.len());
        for item in returning {
            let (expr, alias_name) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => return Ok(None),
            };
            columns.push(alias_name.unwrap_or_else(|| row_expr_column_name(expr)));
            expressions.push(expr);
        }

        let mut rows = Vec::with_capacity(records.len());
        for (idx, record) in records.iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut row = Vec::with_capacity(expressions.len());
            for expr in &expressions {
                let value = match self.eval_target_record_expr(table, alias, schema, record, expr) {
                    Ok(Some(value)) => value,
                    Ok(None) | Err(SqlError::Unsupported(_)) => return Ok(None),
                    Err(error) => return Err(error),
                };
                row.push(value);
            }
            rows.push(row);
        }
        let result = SqlResult::new(columns, rows);
        Ok(Some(
            with_projection_column_types(result, returning, Some(schema)).with_column_metadata(
                returning_projection_column_metadata(self.db_ref(), table, returning, Some(schema)),
            ),
        ))
    }

    pub(crate) fn eval_target_record_expr(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        record: &Record,
        expr: &Expr,
    ) -> Result<Option<SqlValue>> {
        match expr {
            Expr::Identifier(ident) => {
                if table_has_field(Some(schema), &ident.value) {
                    return Ok(Some(record_column_value(record, schema, &ident.value)));
                }
                Ok(Some(
                    routine_var_from_ident(&self.routine_vars, ident).unwrap_or(SqlValue::Null),
                ))
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                if let Some((field, _)) =
                    target_record_field_from_parts(table, alias, Some(schema), &parts)
                {
                    return Ok(Some(record_column_value(record, schema, &field)));
                }
                Ok(Some(
                    routine_var_from_parts(&self.routine_vars, &parts)?.unwrap_or(SqlValue::Null),
                ))
            }
            Expr::Value(value) => Ok(Some(
                routine_var_from_value(&self.routine_vars, value)
                    .map(Ok)
                    .unwrap_or_else(|| literal_to_value(value))?,
            )),
            Expr::TypedString(value) => {
                Ok(Some(typed_string_to_value_with_db(self.db_ref(), value)?))
            }
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
            {
                let Some(base) =
                    self.eval_target_record_expr(table, alias, schema, record, left)?
                else {
                    return Ok(None);
                };
                let Some(path) =
                    self.eval_target_record_expr(table, alias, schema, record, right)?
                else {
                    return Ok(None);
                };
                Ok(Some(eval_json_operator_value(base, op, path)?))
            }
            Expr::BinaryOp {
                left: left_expr,
                op,
                right: right_expr,
            } => {
                let Some(left) =
                    self.eval_target_record_expr(table, alias, schema, record, left_expr)?
                else {
                    return Ok(None);
                };
                let Some(right) =
                    self.eval_target_record_expr(table, alias, schema, record, right_expr)?
                else {
                    return Ok(None);
                };
                let left_type = projected_expr_pg_type_with_db(self.db_ref(), left_expr)
                    .or_else(|| projected_expr_pg_type(left_expr, Some(schema)));
                let right_type = projected_expr_pg_type_with_db(self.db_ref(), right_expr)
                    .or_else(|| projected_expr_pg_type(right_expr, Some(schema)));
                if let Some(value) = eval_range_binary_value_with_db(
                    self.db_ref(),
                    left.clone(),
                    op,
                    right.clone(),
                    left_type.as_deref(),
                    right_type.as_deref(),
                )? {
                    Ok(Some(value))
                } else {
                    Ok(Some(eval_binary_expr_value(
                        left_expr,
                        op,
                        right_expr,
                        left,
                        right,
                        Some(schema),
                    )?))
                }
            }
            Expr::JsonAccess { value, path } => {
                let Some(base) =
                    self.eval_target_record_expr(table, alias, schema, record, value)?
                else {
                    return Ok(None);
                };
                Ok(Some(json_extract_path_value(
                    &base,
                    &json_access_path(path)?,
                    false,
                )))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let Some(value) =
                        self.eval_target_record_expr(table, alias, schema, record, source)?
                    else {
                        return Ok(None);
                    };
                    return regclass_text_value(self.db_ref(), value).map(Some);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let Some(value) =
                        self.eval_target_record_expr(table, alias, schema, record, source)?
                    else {
                        return Ok(None);
                    };
                    return regtype_text_value(self.db_ref(), value).map(Some);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let Some(value) =
                        self.eval_target_record_expr(table, alias, schema, record, source)?
                    else {
                        return Ok(None);
                    };
                    return regclass_text_value(self.db_ref(), value).map(Some);
                }
                let Some(value) =
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                else {
                    return Ok(None);
                };
                Ok(Some(cast_expr_value_with_db(
                    self.db_ref(),
                    value,
                    expr,
                    data_type,
                    Some(schema),
                )?))
            }
            Expr::Position { expr, r#in } => {
                let needle_expr = expr;
                let haystack_expr = r#in;
                let Some(expr) =
                    self.eval_target_record_expr(table, alias, schema, record, needle_expr)?
                else {
                    return Ok(None);
                };
                let Some(r#in) =
                    self.eval_target_record_expr(table, alias, schema, record, haystack_expr)?
                else {
                    return Ok(None);
                };
                Ok(Some(eval_position_typed_value(
                    expr,
                    r#in,
                    projected_expr_pg_type(needle_expr, Some(schema)).as_deref() == Some("bytea")
                        || projected_expr_pg_type(haystack_expr, Some(schema)).as_deref()
                            == Some("bytea"),
                )?))
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => eval_substring_expr(
                expr,
                substring_from.as_deref(),
                substring_for.as_deref(),
                |expr| {
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "unsupported target-record expression {expr}"
                            ))
                        })
                },
                projected_expr_pg_type(expr, Some(schema)).as_deref() == Some("bytea"),
            )
            .map(Some),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => eval_overlay_expr(
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                |expr| {
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "unsupported target-record expression {expr}"
                            ))
                        })
                },
                projected_expr_pg_type(expr, Some(schema)).as_deref() == Some("bytea"),
            )
            .map(Some),
            Expr::Extract { field, expr, .. } => {
                let Some(value) =
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                else {
                    return Ok(None);
                };
                eval_extract_value(field, value).map(Some)
            }
            Expr::Function(function) => {
                self.eval_target_record_function_value(table, alias, schema, record, function)
            }
            Expr::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters,
            } => eval_trim_expr(
                trim_where.as_ref(),
                trim_what.as_deref(),
                expr,
                trim_characters.as_deref(),
                |expr| {
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "unsupported target-record expression {expr}"
                            ))
                        })
                },
            )
            .map(Some),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.eval_target_record_case(
                table,
                alias,
                schema,
                record,
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
            ),
            Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. } => {
                let row = row_from_record(table, alias, Some(schema), record)?;
                self.row_engine()
                    .eval_row_truth(&row, expr)
                    .map(|value| Some(value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)))
            }
            Expr::Array(array) => {
                let values = array
                    .elem
                    .iter()
                    .map(|expr| {
                        self.eval_target_record_expr(table, alias, schema, record, expr)?
                            .ok_or_else(|| {
                                SqlError::Unsupported(format!(
                                    "unsupported target-record expression {expr}"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql_array_value(values).map(Some)
            }
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                projected_expr_pg_type(root, Some(schema)).as_deref() == Some("jsonb"),
                |expr| {
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "unsupported target-record expression {expr}"
                            ))
                        })
                },
            )
            .map(Some),
            Expr::Interval(interval) => interval_literal_value(interval, |expr| {
                self.eval_target_record_expr(table, alias, schema, record, expr)?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "unsupported target-record expression {expr}"
                        ))
                    })
            })
            .map(Some),
            Expr::Nested(expr) => self.eval_target_record_expr(table, alias, schema, record, expr),
            Expr::Collate { expr, .. } => {
                self.eval_target_record_expr(table, alias, schema, record, expr)
            }
            Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
                let Some(value) =
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                else {
                    return Ok(None);
                };
                eval_unary_minus_expr_value(expr, value, Some(schema)).map(Some)
            }
            Expr::UnaryOp { op, expr } if op.to_string() == "+" => {
                self.eval_target_record_expr(table, alias, schema, record, expr)
            }
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                let Some(value) =
                    self.eval_target_record_expr(table, alias, schema, record, expr)?
                else {
                    return Ok(None);
                };
                eval_unary_bit_not_expr_value(op, expr, value, Some(schema)).map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn materialize_generated_columns(
        &self,
        table: &str,
        schema: &TableSchema,
        records: &mut [Record],
    ) -> Result<()> {
        let generated = schema
            .columns
            .iter()
            .filter_map(|column| {
                column
                    .generated_expr
                    .as_deref()
                    .map(|expression| Ok((column, Self::parse_default_expr(expression)?)))
            })
            .collect::<Result<Vec<_>>>()?;
        for record in records {
            for (column, expression) in &generated {
                let value = self
                    .eval_target_record_expr(table, table, schema, record, expression)?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "unsupported generated column expression {expression}"
                        ))
                    })?;
                let value = cast_value_to_column_type(value, column)?;
                set_record_column(record, Some(schema), &column.name, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn eval_target_record_function_value(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        record: &Record,
        function: &Function,
    ) -> Result<Option<SqlValue>> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let arg_exprs = function_args(function);
        let args = arg_exprs
            .iter()
            .map(|arg| {
                self.eval_target_record_expr(table, alias, schema, record, arg)?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!("unsupported target-record expression {arg}"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let arg_types = arg_exprs
            .iter()
            .map(|arg| projected_expr_pg_type(arg, Some(schema)))
            .collect::<Vec<_>>();
        if matches!(name.as_str(), "coalesce" | "pg_catalog.coalesce") {
            return Ok(Some(
                args.into_iter()
                    .find(|value| !matches!(value, SqlValue::Null))
                    .unwrap_or(SqlValue::Null),
            ));
        }
        if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
            .or(eval_user_range_function_value(
                self.db_ref(),
                &name,
                &args,
                &arg_types,
            )?)
            .or(eval_range_function_value(&name, &args, &arg_types)?)
        {
            return Ok(Some(value));
        }
        if let Some(value) = eval_session_function_value(&name, &args, &self.session_gucs)? {
            return Ok(Some(value));
        }
        if let Some(value) =
            eval_privilege_function_value(self.db_ref(), &name, &args, &self.session_gucs)?
        {
            return Ok(Some(value));
        }
        if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
            return Ok(Some(value));
        }
        if let Some(value) = eval_db_catalog_function_value(
            self.db_ref(),
            &name,
            &args,
            self.tx.as_ref().map(Transaction::visibility_watermark),
            Some(&self.session_gucs),
        )? {
            return Ok(Some(value));
        }
        if let Some(value) = eval_broker_function_value(
            self.db_ref(),
            &name,
            &args,
            BrokerCaller::Sql {
                context: self.security_context.as_ref(),
                superuser: session_role_is_superuser(self.db_ref(), &self.session_gucs),
            },
            self.tx.as_ref(),
        )? {
            return Ok(Some(value));
        }
        if let Some(value) = eval_json_function_call_value(function, &args)? {
            return Ok(Some(value));
        }
        if let Some(value) = eval_catalog_function_value(&name, &args) {
            return Ok(Some(value));
        }
        if let Some(value) = eval_compatibility_function_value_with_db(
            self.db_ref(),
            &name,
            &args,
            Some(&arg_types),
        )? {
            return Ok(Some(value));
        }
        if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
            return Ok(Some(value));
        }
        eval_builtin_function_value(function).map(Some)
    }

    pub(crate) fn eval_target_record_case(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        record: &Record,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
    ) -> Result<Option<SqlValue>> {
        let operand_value = operand
            .map(|expr| self.eval_target_record_expr(table, alias, schema, record, expr))
            .transpose()?
            .flatten();
        for condition in conditions {
            let Some(condition_value) =
                self.eval_target_record_expr(table, alias, schema, record, &condition.condition)?
            else {
                return Ok(None);
            };
            let matched = if let Some(operand_value) = &operand_value {
                values_equal(operand_value, &condition_value)
            } else {
                sql_value_truth(condition_value)?.unwrap_or(false)
            };
            if matched {
                return self.eval_target_record_expr(
                    table,
                    alias,
                    schema,
                    record,
                    &condition.result,
                );
            }
        }
        else_result
            .map(|expr| self.eval_target_record_expr(table, alias, schema, record, expr))
            .unwrap_or(Ok(Some(SqlValue::Null)))
    }

    pub(crate) fn row_engine(&self) -> SqlEngine<'_> {
        self.sql_engine()
    }
}
