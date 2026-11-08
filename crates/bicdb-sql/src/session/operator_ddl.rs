//! `CREATE OPERATOR`, `ALTER OPERATOR` and `DROP OPERATOR`.
use crate::pg_operators::*;
use crate::*;
use sqlparser::ast::{
    AlterOperator, AlterOperatorOperation, CreateOperator, DropOperator, OperatorOption,
};

/// An operator name part: quoted parts keep their spelling.
fn name_part(part: &str) -> String {
    match part
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(quoted) => quoted.to_string(),
        None => part.to_ascii_lowercase(),
    }
}

/// `schema.name` of an operator or function name; the schema is `None` when
/// the name is unqualified.
fn split_name(name: &ObjectName) -> Result<(Option<String>, String)> {
    let parts = name
        .0
        .iter()
        .map(|part| match part.as_ident() {
            Some(ident) if ident.quote_style.is_some() => ident.value.clone(),
            Some(ident) => name_part(&ident.value),
            None => name_part(&part.to_string()),
        })
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [name] => Ok((None, name.clone())),
        [schema, name] => Ok((Some(schema.clone()), name.clone())),
        [_catalog, schema, name] => Ok((Some(schema.clone()), name.clone())),
        _ => Err(SqlError::InvalidSql(format!(
            "improper qualified name: {name}"
        ))),
    }
}

fn operand_type(data_type: &DataType) -> Result<(String, String)> {
    let declaration = data_type.to_string();
    let pg_type = match pg_type_from_data_type(data_type) {
        Ok((name, _)) => canonical_type(&name),
        Err(_) => canonical_type(&declaration),
    };
    Ok((pg_type, declaration))
}

fn operator_type_list(left: Option<&str>, right: &str) -> String {
    format!(
        "{},{}",
        left.map(|left| pg_type_spec(left)
            .map(|spec| spec.display_name)
            .unwrap_or(left))
            .unwrap_or("NONE"),
        pg_type_spec(right)
            .map(|spec| spec.display_name)
            .unwrap_or(right)
    )
}

fn missing_operator(id: &OperatorRef, qualified: bool) -> SqlError {
    let name = if qualified {
        format!("{}.{}", id.schema, id.name)
    } else {
        id.name.clone()
    };
    let display = |pg_type: &str| {
        pg_type_spec(pg_type)
            .map(|spec| spec.display_name.to_string())
            .unwrap_or_else(|| pg_type.to_string())
    };
    SqlError::undefined_function(match &id.left {
        Some(left) => format!(
            "operator does not exist: {} {name} {}",
            display(left),
            display(&id.right)
        ),
        None => format!("operator does not exist: {name} {}", display(&id.right)),
    })
}

/// A routine's input argument types (IN, INOUT and VARIADIC), canonical.
fn routine_input_types(routine: &RoutineSchema) -> Result<Vec<Option<String>>> {
    let params = parse_routine_params(&routine.args)?;
    let recorded = routine.arg_types.len() == params.len();
    Ok(params
        .iter()
        .enumerate()
        .filter(|(_, param)| param.mode != RoutineArgMode::Out)
        .map(|(index, param)| {
            if recorded {
                Some(canonical_type(&routine.arg_types[index].pg_type))
            } else {
                param
                    .type_schema
                    .as_ref()
                    .map(|schema| canonical_type(&schema.pg_type))
            }
        })
        .collect())
}

impl SqlSession<'_> {
    /// Operators implemented by `routine` (its schema, name and input types).
    pub(crate) fn routine_operators(&self, routine: &RoutineSchema) -> Result<Vec<OperatorSchema>> {
        if routine.kind != RoutineKind::Function {
            return Ok(Vec::new());
        }
        let operators = user_operators(self.db_ref())?;
        if operators.is_empty() {
            return Ok(Vec::new());
        }
        let schema = routine_schema_name(routine);
        let name = normalize_object_name(&routine.name);
        let inputs = routine_input_types(routine)?;
        Ok(operators
            .iter()
            .filter(|operator| {
                operator
                    .function
                    .as_ref()
                    .is_some_and(|function| function.schema == schema && function.name == name)
                    && operator
                        .id
                        .left
                        .iter()
                        .chain(std::iter::once(&operator.id.right))
                        .map(|pg_type| Some(pg_type.clone()))
                        .eq(inputs.iter().cloned())
            })
            .cloned()
            .collect())
    }

    fn operator_creation_schema(&self, schema: Option<String>) -> Result<String> {
        if let Some(schema) = schema {
            if !matches!(schema.as_str(), "public" | "pg_catalog")
                && load_namespace(self.db_ref(), &schema)?.is_none()
            {
                return Err(SqlError::data_exception_public(
                    "3F000",
                    format!("schema \"{schema}\" does not exist"),
                    None,
                ));
            }
            return Ok(schema);
        }
        for schema in effective_search_path(&self.session_gucs) {
            if schema == "pg_catalog" {
                continue;
            }
            if schema == "public" || load_namespace(self.db_ref(), &schema)?.is_some() {
                return Ok(schema);
            }
        }
        Err(SqlError::data_exception_public(
            "3F000",
            "no schema has been selected to create in",
            None,
        ))
    }

    /// The function implementing an operator: a function of exactly the
    /// operand types.
    fn operator_function(
        &self,
        name: &ObjectName,
        arg_types: &[String],
    ) -> Result<OperatorFunction> {
        let (schema, function) = split_name(name)?;
        let schemas = match &schema {
            Some(schema) => vec![schema.clone()],
            None => effective_search_path(&self.session_gucs),
        };
        let records = match self.db_ref().scan_collection(ROUTINE_COLLECTION) {
            Ok(records) => records,
            Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let routines = records
            .into_iter()
            .filter_map(|record| serde_json::from_value::<RoutineSchema>(record.metadata).ok())
            .map(RoutineSchema::with_declared_metadata)
            .filter(|routine| {
                routine.kind == RoutineKind::Function
                    && normalize_object_name(&routine.name) == function
            })
            .collect::<Vec<_>>();
        for schema in &schemas {
            for routine in &routines {
                if routine_schema_name(routine) != schema {
                    continue;
                }
                let inputs = routine_input_types(routine)?;
                if inputs.len() == arg_types.len()
                    && inputs
                        .iter()
                        .zip(arg_types)
                        .all(|(input, wanted)| input.as_deref() == Some(wanted.as_str()))
                {
                    return Ok(OperatorFunction {
                        schema: schema.clone(),
                        name: function.clone(),
                        result: canonical_type(&routine.return_type),
                    });
                }
            }
        }
        let display = arg_types
            .iter()
            .map(|pg_type| {
                pg_type_spec(pg_type)
                    .map(|spec| spec.display_name.to_string())
                    .unwrap_or_else(|| pg_type.clone())
            })
            .collect::<Vec<_>>()
            .join(", ");
        let qualified = match schema {
            Some(schema) => format!("{schema}.{function}"),
            None => function,
        };
        Err(SqlError::undefined_function(format!(
            "function {qualified}({display}) does not exist"
        )))
    }

    /// The operator `name(left, right)` names, by explicit schema or by the
    /// search path.
    fn find_operator(
        &self,
        name: &ObjectName,
        left: Option<&DataType>,
        right: &DataType,
    ) -> Result<std::result::Result<OperatorSchema, OperatorRef>> {
        let (schema, symbol) = split_name(name)?;
        let left = left
            .map(operand_type)
            .transpose()?
            .map(|(pg_type, _)| pg_type);
        let (right, _) = operand_type(right)?;
        let schemas = match &schema {
            Some(schema) => vec![schema.clone()],
            None => effective_search_path(&self.session_gucs),
        };
        for candidate in &schemas {
            let id = OperatorRef {
                schema: candidate.clone(),
                name: symbol.clone(),
                left: left.clone(),
                right: right.clone(),
            };
            if let Some(operator) = load_operator(self.db_ref(), &id)? {
                return Ok(Ok(operator));
            }
        }
        Ok(Err(OperatorRef {
            schema: schema.unwrap_or_else(|| "public".into()),
            name: symbol,
            left,
            right,
        }))
    }

    fn record_operator_undo(&mut self, id: &OperatorRef) -> Result<()> {
        if self.tx.is_some() {
            let previous = load_operator(self.db_ref(), id)?;
            self.ddl_undo.push(DdlUndo::RestoreOperator {
                id: id.clone(),
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    fn store_operator(&mut self, operator: &OperatorSchema) -> Result<()> {
        self.record_operator_undo(&operator.id)?;
        save_operator(self.db_mut()?, operator)
    }

    fn remove_operator(&mut self, id: &OperatorRef) -> Result<()> {
        self.record_operator_undo(id)?;
        delete_operator(self.db_mut()?, id)
    }

    /// The operator a COMMUTATOR or NEGATOR option names, given its operand
    /// types; unqualified names live in the new operator's schema.
    fn linked_operator_ref(
        &self,
        name: &ObjectName,
        schema: &str,
        left: Option<String>,
        right: String,
    ) -> Result<OperatorRef> {
        let (linked_schema, symbol) = split_name(name)?;
        Ok(OperatorRef {
            schema: linked_schema.unwrap_or_else(|| schema.to_string()),
            name: symbol,
            left,
            right,
        })
    }

    pub(crate) fn execute_create_operator(&mut self, create: &CreateOperator) -> Result<SqlResult> {
        let (schema, symbol) = split_name(&create.name)?;
        let schema = self.operator_creation_schema(schema)?;
        self.require_schema_create_privilege(&schema)?;
        let Some(right_arg) = &create.right_arg else {
            return Err(SqlError::data_exception_public(
                "42P13",
                "operator right argument type must be specified",
                None,
            ));
        };
        let left = create.left_arg.as_ref().map(operand_type).transpose()?;
        let (right, right_declaration) = operand_type(right_arg)?;
        let id = OperatorRef {
            schema: schema.clone(),
            name: symbol.clone(),
            left: left.as_ref().map(|(pg_type, _)| pg_type.clone()),
            right: right.clone(),
        };
        let arg_types = id
            .left
            .iter()
            .cloned()
            .chain(std::iter::once(right.clone()))
            .collect::<Vec<_>>();
        let function = self.operator_function(&create.function, &arg_types)?;
        if function.result == "void" {
            return Err(SqlError::data_exception_public(
                "42P13",
                "operator function must not return void",
                None,
            ));
        }
        let existing = load_operator(self.db_ref(), &id)?;
        if existing
            .as_ref()
            .is_some_and(|existing| !existing.is_shell())
        {
            return Err(SqlError::data_exception_public(
                "42723",
                format!("operator {schema}.{symbol} already exists"),
                None,
            ));
        }
        let current_user = current_user_from_gucs(&self.session_gucs);
        let mut operator = OperatorSchema {
            id: id.clone(),
            left_declaration: left.map(|(_, declaration)| declaration),
            right_declaration,
            function: Some(function),
            commutator: existing.as_ref().and_then(|shell| shell.commutator.clone()),
            negator: existing.as_ref().and_then(|shell| shell.negator.clone()),
            restrict: None,
            join: None,
            hashes: false,
            merges: false,
            owner: Some(
                existing
                    .as_ref()
                    .map(OperatorSchema::owner)
                    .unwrap_or_else(|| current_user.clone()),
            ),
        };
        let mut commutator_name = None;
        let mut negator_name = None;
        for option in &create.options {
            match option {
                OperatorOption::Commutator(name) => commutator_name = Some(name),
                OperatorOption::Negator(name) => negator_name = Some(name),
                OperatorOption::Restrict(function) => {
                    operator.restrict = function.as_ref().map(ToString::to_string)
                }
                OperatorOption::Join(function) => {
                    operator.join = function.as_ref().map(ToString::to_string)
                }
                OperatorOption::Hashes => operator.hashes = true,
                OperatorOption::Merges => operator.merges = true,
            }
        }
        if (operator.hashes || operator.merges) && operator.id.left.is_none() {
            return Err(SqlError::data_exception_public(
                "42P13",
                if operator.hashes {
                    "only binary operators can hash"
                } else {
                    "only binary operators can merge join"
                },
                None,
            ));
        }
        // Resolve the links first: nothing is written until every check passes.
        let mut writes: Vec<OperatorSchema> = Vec::new();
        let link = |session: &Self,
                    target: OperatorRef,
                    commutator: bool,
                    writes: &mut Vec<OperatorSchema>|
         -> Result<OperatorRef> {
            if target == id {
                if !commutator {
                    return Err(SqlError::data_exception_public(
                        "42P13",
                        "operator cannot be its own negator",
                        None,
                    ));
                }
                return Ok(target);
            }
            let mut other = match writes.iter().position(|write| write.id == target) {
                Some(index) => writes.remove(index),
                None => match load_operator(session.db_ref(), &target)? {
                    Some(other) => other,
                    None => {
                        if target.schema != schema {
                            session.require_schema_create_privilege(&target.schema)?;
                        }
                        OperatorSchema {
                            id: target.clone(),
                            left_declaration: target.left.clone(),
                            right_declaration: target.right.clone(),
                            function: None,
                            commutator: None,
                            negator: None,
                            restrict: None,
                            join: None,
                            hashes: false,
                            merges: false,
                            owner: Some(current_user.clone()),
                        }
                    }
                },
            };
            let back = if commutator {
                &mut other.commutator
            } else {
                &mut other.negator
            };
            // An operator already linked to another one keeps its link
            // (PostgreSQL 16 semantics): only an unset link is filled in.
            if back.is_none() {
                *back = Some(id.clone());
            }
            writes.push(other);
            Ok(target)
        };
        if let Some(name) = commutator_name {
            let right_type = id.left.clone().ok_or_else(|| {
                SqlError::data_exception_public(
                    "42P13",
                    "only binary operators can have commutators",
                    None,
                )
            })?;
            let target =
                self.linked_operator_ref(name, &schema, Some(id.right.clone()), right_type)?;
            let target = link(self, target, true, &mut writes)?;
            operator.commutator = Some(target);
        }
        if let Some(name) = negator_name {
            let target =
                self.linked_operator_ref(name, &schema, id.left.clone(), id.right.clone())?;
            let target = link(self, target, false, &mut writes)?;
            operator.negator = Some(target);
        }
        self.store_operator(&operator)?;
        for other in &writes {
            self.store_operator(other)?;
        }
        Ok(SqlResult::command("CREATE OPERATOR"))
    }

    fn require_operator_owner(&self, operator: &OperatorSchema) -> Result<()> {
        self.require_object_ownership(
            &operator.owner(),
            &format!("operator {}.{}", operator.id.schema, operator.id.name),
            "change it",
        )
    }

    pub(crate) fn execute_alter_operator(&mut self, alter: &AlterOperator) -> Result<SqlResult> {
        let mut operator =
            match self.find_operator(&alter.name, alter.left_type.as_ref(), &alter.right_type)? {
                Ok(operator) => operator,
                Err(id) => {
                    return Err(missing_operator(&id, split_name(&alter.name)?.0.is_some()));
                }
            };
        self.require_operator_owner(&operator)?;
        match &alter.operation {
            AlterOperatorOperation::OwnerTo(owner) => {
                let current_user = current_user_from_gucs(&self.session_gucs);
                let owner = match owner {
                    Owner::Ident(owner) => normalize_role_name(&ident_value(owner)),
                    Owner::CurrentRole | Owner::CurrentUser => current_user,
                    Owner::SessionUser => session_user_from_gucs(&self.session_gucs),
                };
                ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                self.require_settable_new_owner(
                    &owner,
                    &format!("operator {}.{}", operator.id.schema, operator.id.name),
                )?;
                operator.owner = Some(owner);
            }
            AlterOperatorOperation::Set { options } => {
                for option in options {
                    match option {
                        OperatorOption::Restrict(function) => {
                            operator.restrict = function.as_ref().map(ToString::to_string)
                        }
                        OperatorOption::Join(function) => {
                            operator.join = function.as_ref().map(ToString::to_string)
                        }
                        OperatorOption::Hashes => operator.hashes = true,
                        OperatorOption::Merges => operator.merges = true,
                        OperatorOption::Commutator(_) | OperatorOption::Negator(_) => {
                            return Err(SqlError::Unsupported(
                                "ALTER OPERATOR SET (COMMUTATOR | NEGATOR) is not supported".into(),
                            ));
                        }
                    }
                }
            }
            AlterOperatorOperation::SetSchema { .. } => {
                return Err(SqlError::Unsupported(
                    "ALTER OPERATOR SET SCHEMA is not supported".into(),
                ));
            }
        }
        self.store_operator(&operator)?;
        Ok(SqlResult::command("ALTER OPERATOR"))
    }

    pub(crate) fn execute_drop_operator(&mut self, drop: &DropOperator) -> Result<SqlResult> {
        let mut dropped = Vec::new();
        for signature in &drop.operators {
            let operator = match self.find_operator(
                &signature.name,
                signature.left_type.as_ref(),
                &signature.right_type,
            )? {
                Ok(operator) => operator,
                Err(id)
                    if (id.schema == "pg_catalog" || split_name(&signature.name)?.0.is_none())
                        && builtin_operators().iter().any(|builtin| {
                            builtin.name == id.name
                                && builtin.left == id.left.as_deref()
                                && builtin.right == id.right
                        }) =>
                {
                    return Err(SqlError::data_exception_public(
                        "2BP01",
                        format!(
                            "cannot drop operator {}({}) because it is required by the database system",
                            id.name,
                            operator_type_list(id.left.as_deref(), &id.right)
                        ),
                        None,
                    ));
                }
                Err(id) => {
                    if drop.if_exists {
                        continue;
                    }
                    return Err(missing_operator(
                        &id,
                        split_name(&signature.name)?.0.is_some(),
                    ));
                }
            };
            self.require_object_ownership(
                &operator.owner(),
                &format!("operator {}.{}", operator.id.schema, operator.id.name),
                "drop it",
            )?;
            dropped.push(operator);
        }
        let ids = dropped
            .into_iter()
            .map(|operator| operator.id)
            .collect::<Vec<_>>();
        self.drop_operators(&ids)?;
        Ok(SqlResult::command("DROP OPERATOR"))
    }

    /// The operators of `schema`, for DROP SCHEMA.
    pub(crate) fn schema_operators(&self, schema: &str) -> Result<Vec<OperatorSchema>> {
        Ok(user_operators(self.db_ref())?
            .iter()
            .filter(|operator| operator.id.schema == schema)
            .cloned()
            .collect())
    }

    /// Removes operators; links from other operators to them are cleared.
    pub(crate) fn drop_operators(&mut self, ids: &[OperatorRef]) -> Result<()> {
        for id in ids {
            self.remove_operator(id)?;
            for other in user_operators(self.db_ref())?.iter() {
                let commutes = other.commutator.as_ref() == Some(id);
                let negates = other.negator.as_ref() == Some(id);
                if commutes || negates {
                    let mut other = other.clone();
                    if commutes {
                        other.commutator = None;
                    }
                    if negates {
                        other.negator = None;
                    }
                    self.store_operator(&other)?;
                }
            }
        }
        Ok(())
    }
}
