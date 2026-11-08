//! PostgreSQL operators: the built-in `pg_catalog` operator catalog,
//! user-defined operators (`CREATE OPERATOR`), and operator resolution.
//!
//! The executor evaluates built-in operators directly. A user-defined
//! operator is a name for a function of its operand types, so a statement is
//! bound before it runs: every operator expression whose name has a
//! user-defined candidate is resolved by PostgreSQL's rules (search path,
//! exact match, then implicit-coercion candidates and the preferred-type and
//! unknown-literal heuristics). When resolution selects a user-defined
//! operator the expression becomes a call of its function with the operands
//! coerced to the declared operand types. When it selects a built-in operator
//! the expression is left to the executor unchanged.
use crate::*;
use sqlparser::ast::{CastKind, FunctionArgumentList, ObjectNamePart};
use std::sync::LazyLock;

pub(crate) const OPERATOR_COLLECTION: &str = "__bicdb_pg_operators";

/// A `pg_catalog` operator of the compatibility catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BuiltinOperator {
    pub(crate) oid: i64,
    pub(crate) name: &'static str,
    pub(crate) kind: char,
    pub(crate) left: Option<&'static str>,
    pub(crate) right: &'static str,
    pub(crate) result: &'static str,
    pub(crate) function_oid: i64,
    pub(crate) function: &'static str,
    pub(crate) commutator: i64,
    pub(crate) negator: i64,
    pub(crate) merges: bool,
    pub(crate) hashes: bool,
}

static BUILTIN_OPERATORS: LazyLock<Vec<BuiltinOperator>> = LazyLock::new(|| {
    include_str!("pg_operators.tsv")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                12,
                "invalid pg_operator compatibility row: {line}"
            );
            BuiltinOperator {
                oid: fields[0].parse().expect("pg_operator OID"),
                name: fields[1],
                kind: fields[2].chars().next().expect("pg_operator kind"),
                left: (!fields[3].is_empty()).then_some(fields[3]),
                right: fields[4],
                result: fields[5],
                function_oid: fields[6].parse().expect("pg_operator function OID"),
                function: fields[7],
                commutator: fields[8].parse().expect("pg_operator commutator"),
                negator: fields[9].parse().expect("pg_operator negator"),
                merges: fields[10] == "t",
                hashes: fields[11] == "t",
            }
        })
        .collect()
});

pub(crate) fn builtin_operators() -> &'static [BuiltinOperator] {
    BUILTIN_OPERATORS.as_slice()
}

/// Identity of an operator: namespace, name and operand types. `left` is
/// `None` for a prefix operator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperatorRef {
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) left: Option<String>,
    pub(crate) right: String,
}

impl OperatorRef {
    pub(crate) fn key(&self) -> String {
        format!(
            "{}.{}({},{})",
            self.schema,
            self.name,
            self.left.as_deref().unwrap_or("NONE"),
            self.right
        )
    }

    pub(crate) fn display(&self) -> String {
        format!(
            "{}.{}({}, {})",
            self.schema,
            self.name,
            self.left.as_deref().map(display_type).unwrap_or("NONE"),
            display_type(&self.right)
        )
    }
}

/// The function implementing an operator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperatorFunction {
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) result: String,
}

/// A user-defined operator. An operator without a function is a shell: it was
/// named as another operator's commutator or negator before being defined.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OperatorSchema {
    pub(crate) id: OperatorRef,
    /// Declared operand types as SQL text, for coercing operands.
    #[serde(default)]
    pub(crate) left_declaration: Option<String>,
    pub(crate) right_declaration: String,
    #[serde(default)]
    pub(crate) function: Option<OperatorFunction>,
    #[serde(default)]
    pub(crate) commutator: Option<OperatorRef>,
    #[serde(default)]
    pub(crate) negator: Option<OperatorRef>,
    #[serde(default)]
    pub(crate) restrict: Option<String>,
    #[serde(default)]
    pub(crate) join: Option<String>,
    #[serde(default)]
    pub(crate) hashes: bool,
    #[serde(default)]
    pub(crate) merges: bool,
    #[serde(default)]
    pub(crate) owner: Option<String>,
}

impl OperatorSchema {
    pub(crate) fn is_shell(&self) -> bool {
        self.function.is_none()
    }

    pub(crate) fn oid(&self) -> i64 {
        operator_oid(&self.id)
    }

    pub(crate) fn owner(&self) -> String {
        self.owner.clone().unwrap_or_else(current_role_name)
    }
}

pub(crate) fn operator_oid(id: &OperatorRef) -> i64 {
    stable_name_hash_wide(2_100_000_000, &id.key(), 100_000_000)
}

/// The canonical name of a PostgreSQL type, as operators record it.
pub(crate) fn canonical_type(pg_type: &str) -> String {
    let lower = pg_type.trim().to_ascii_lowercase();
    if let Some(element) = lower.strip_suffix("[]") {
        return format!("{}[]", canonical_type(element));
    }
    if let Some(spec) = pg_type_spec(&lower) {
        return spec.name.to_string();
    }
    if let Ok(data_type) = parse_sql_data_type(&lower) {
        if let Ok((name, _)) = pg_type_from_data_type(&data_type) {
            if let Some(spec) = pg_type_spec(&name) {
                return spec.name.to_string();
            }
            return name;
        }
    }
    lower
}

fn display_type(pg_type: &str) -> &str {
    if let Some(spec) = pg_type_spec(pg_type) {
        return spec.display_name;
    }
    pg_cast_display_type(pg_type)
}

pub(crate) fn parse_sql_data_type(text: &str) -> Result<DataType> {
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(text)
        .and_then(|mut parser| {
            let data_type = parser.parse_data_type()?;
            parser.expect_token(&Token::EOF)?;
            Ok(data_type)
        })
        .map_err(|error| SqlError::InvalidSql(error.to_string()))
}

thread_local! {
    static OPERATORS: RefCell<Option<(u64, u64, Arc<Vec<OperatorSchema>>)>> =
        const { RefCell::new(None) };
}

/// Every user-defined operator, cached per database and catalog generation.
pub(crate) fn user_operators(db: &BicDb) -> Result<Arc<Vec<OperatorSchema>>> {
    let database = db.instance_id();
    let generation = db.collection_generation(OPERATOR_COLLECTION);
    if let Some(list) = OPERATORS.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(OPERATOR_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut list = records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<OperatorSchema>>>()?;
    list.sort_by_key(|operator| operator.id.key());
    let list = Arc::new(list);
    OPERATORS.with(|cache| *cache.borrow_mut() = Some((database, generation, list.clone())));
    Ok(list)
}

pub(crate) fn load_operator(db: &BicDb, id: &OperatorRef) -> Result<Option<OperatorSchema>> {
    match db.get(OPERATOR_COLLECTION, &id.key()) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn save_operator(db: &mut BicDb, operator: &OperatorSchema) -> Result<()> {
    db.create_collection(OPERATOR_COLLECTION)?;
    db.insert(
        OPERATOR_COLLECTION,
        Record::new(operator.id.key()).with_metadata(serde_json::to_value(operator)?),
    )?;
    Ok(())
}

pub(crate) fn delete_operator(db: &mut BicDb, id: &OperatorRef) -> Result<()> {
    match db.delete(OPERATOR_COLLECTION, &id.key()) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

// ---------------------------------------------------------------------------
// pg_operator
// ---------------------------------------------------------------------------

fn type_oid(pg_type: &str) -> i64 {
    if let Some(element) = pg_type.strip_suffix("[]") {
        return pg_type_spec(element)
            .and_then(|spec| spec.array_oid)
            .map(i64::from)
            .unwrap_or(0);
    }
    pg_type_oid_by_name(pg_type).map(i64::from).unwrap_or(0)
}

pub(crate) fn pg_operator_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = builtin_operators()
        .iter()
        .map(|operator| {
            virtual_row([
                ("oid", SqlValue::Int(operator.oid)),
                ("oprname", SqlValue::String(operator.name.to_string())),
                ("oprnamespace", SqlValue::Int(11)),
                ("oprowner", SqlValue::Int(10)),
                ("oprkind", SqlValue::String(operator.kind.to_string())),
                ("oprcanmerge", SqlValue::Bool(operator.merges)),
                ("oprcanhash", SqlValue::Bool(operator.hashes)),
                (
                    "oprleft",
                    SqlValue::Int(operator.left.map(type_oid).unwrap_or(0)),
                ),
                ("oprright", SqlValue::Int(type_oid(operator.right))),
                ("oprresult", SqlValue::Int(type_oid(operator.result))),
                ("oprcom", SqlValue::Int(operator.commutator)),
                ("oprnegate", SqlValue::Int(operator.negator)),
                ("oprcode", SqlValue::Int(operator.function_oid)),
                ("oprrest", SqlValue::Int(0)),
                ("oprjoin", SqlValue::Int(0)),
            ])
        })
        .collect::<Vec<_>>();
    for operator in user_operators(db)?.iter() {
        let link = |id: &Option<OperatorRef>| id.as_ref().map(operator_oid).unwrap_or(0);
        rows.push(virtual_row([
            ("oid", SqlValue::Int(operator.oid())),
            ("oprname", SqlValue::String(operator.id.name.clone())),
            (
                "oprnamespace",
                SqlValue::Int(namespace_oid(&operator.id.schema)),
            ),
            ("oprowner", SqlValue::Int(role_oid(&operator.owner()))),
            (
                "oprkind",
                SqlValue::String(if operator.id.left.is_some() { "b" } else { "l" }.into()),
            ),
            ("oprcanmerge", SqlValue::Bool(operator.merges)),
            ("oprcanhash", SqlValue::Bool(operator.hashes)),
            (
                "oprleft",
                SqlValue::Int(operator.id.left.as_deref().map(type_oid).unwrap_or(0)),
            ),
            ("oprright", SqlValue::Int(type_oid(&operator.id.right))),
            (
                "oprresult",
                SqlValue::Int(
                    operator
                        .function
                        .as_ref()
                        .map(|function| type_oid(&function.result))
                        .unwrap_or(0),
                ),
            ),
            ("oprcom", SqlValue::Int(link(&operator.commutator))),
            ("oprnegate", SqlValue::Int(link(&operator.negator))),
            (
                "oprcode",
                SqlValue::Int(
                    operator
                        .function
                        .as_ref()
                        .map(|function| routine_oid(RoutineKind::Function, &function.name))
                        .unwrap_or(0),
                ),
            ),
            ("oprrest", SqlValue::Int(0)),
            ("oprjoin", SqlValue::Int(0)),
        ]));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// An operand's type for resolution. `None` is PostgreSQL's `unknown`: an
/// untyped string literal, NULL or untyped parameter.
pub(crate) type OperandType = Option<String>;

#[derive(Clone, Debug)]
pub(crate) struct Candidate<'a> {
    pub(crate) left: Option<String>,
    pub(crate) right: String,
    pub(crate) user: Option<&'a OperatorSchema>,
}

impl Candidate<'_> {
    fn args(&self) -> Vec<&str> {
        self.left
            .as_deref()
            .into_iter()
            .chain(std::iter::once(self.right.as_str()))
            .collect()
    }
}

#[derive(Debug)]
pub(crate) enum Resolution<'a> {
    /// No operator of the name accepts the operand types.
    NotFound,
    /// Several candidates remain; `user` when at least one is user-defined.
    Ambiguous {
        user: bool,
    },
    Builtin,
    User(&'a OperatorSchema),
}

/// The search path's schemas in lookup order: `pg_catalog` first unless the
/// path names it explicitly.
pub(crate) fn effective_search_path(gucs: &HashMap<String, String>) -> Vec<String> {
    let user = current_user_from_gucs(gucs);
    let mut path = gucs
        .get("search_path")
        .map(String::as_str)
        .unwrap_or("\"$user\", public")
        .split(',')
        .map(str::trim)
        .filter(|schema| !schema.is_empty())
        .map(|schema| {
            if schema == "\"$user\"" || schema == "$user" {
                user.clone()
            } else if let Some(quoted) = schema.strip_prefix('"').and_then(|s| s.strip_suffix('"'))
            {
                quoted.to_string()
            } else {
                schema.to_ascii_lowercase()
            }
        })
        .collect::<Vec<_>>();
    if !path.iter().any(|schema| schema == "pg_catalog") {
        path.insert(0, "pg_catalog".to_string());
    }
    path
}

/// Candidate operators named `name` of the given kind, in search-path order;
/// an operator hides a later one of the same operand types. `schema` is the
/// explicit `OPERATOR(schema.name)` qualification.
pub(crate) fn operator_candidates<'a>(
    operators: &'a [OperatorSchema],
    path: &[String],
    schema: Option<&str>,
    name: &str,
    prefix: bool,
) -> Vec<Candidate<'a>> {
    let schemas = match schema {
        Some(schema) => vec![schema.to_string()],
        None => path.to_vec(),
    };
    let mut candidates: Vec<Candidate<'a>> = Vec::new();
    let push = |candidate: Candidate<'a>, candidates: &mut Vec<Candidate<'a>>| {
        if !candidates
            .iter()
            .any(|seen| seen.left == candidate.left && seen.right == candidate.right)
        {
            candidates.push(candidate);
        }
    };
    for schema in &schemas {
        if schema == "pg_catalog" {
            for operator in builtin_operators() {
                if operator.name == name && (operator.kind == 'l') == prefix {
                    push(
                        Candidate {
                            left: operator.left.map(str::to_string),
                            right: operator.right.to_string(),
                            user: None,
                        },
                        &mut candidates,
                    );
                }
            }
        }
        for operator in operators {
            if operator.id.schema == *schema
                && operator.id.name == name
                && operator.id.left.is_none() == prefix
            {
                push(
                    Candidate {
                        left: operator.id.left.clone(),
                        right: operator.id.right.clone(),
                        user: Some(operator),
                    },
                    &mut candidates,
                );
            }
        }
    }
    candidates
}

/// Type facts resolution needs beyond the built-in catalog: domains reduce to
/// their base types and user types carry their categories.
pub(crate) trait TypeFacts {
    fn base_type(&self, pg_type: &str) -> String;
    fn category(&self, pg_type: &str) -> char;
}

pub(crate) struct CatalogTypeFacts<'db>(pub(crate) &'db BicDb);

impl TypeFacts for CatalogTypeFacts<'_> {
    fn base_type(&self, pg_type: &str) -> String {
        common_type_base_name(self.0, pg_type).unwrap_or_else(|_| pg_type.to_string())
    }

    fn category(&self, pg_type: &str) -> char {
        type_category(pg_type).unwrap_or_else(|| {
            common_type_category(self.0, &self.base_type(pg_type)).unwrap_or('U')
        })
    }
}

fn type_category(pg_type: &str) -> Option<char> {
    if pg_type.ends_with("[]") {
        return Some('A');
    }
    if is_polymorphic(pg_type) {
        return Some('P');
    }
    pg_type_spec(pg_type).map(|spec| spec.category)
}

fn is_preferred(pg_type: &str) -> bool {
    pg_type_spec(pg_type).is_some_and(|spec| spec.preferred())
}

fn is_polymorphic(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "anyelement"
            | "anyarray"
            | "anynonarray"
            | "anyenum"
            | "anyrange"
            | "anymultirange"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblenonarray"
            | "anycompatiblerange"
            | "anycompatiblemultirange"
    )
}

/// Whether an input of type `input` may be implicitly coerced to `target`.
fn can_coerce(facts: &dyn TypeFacts, input: &OperandType, target: &str) -> bool {
    let Some(input) = input else {
        // An unknown literal coerces to any type.
        return true;
    };
    if input == target || target == "any" {
        return true;
    }
    let base = facts.base_type(input);
    if base == target {
        return true;
    }
    match target {
        "anyelement" | "anycompatible" => true,
        "anyarray" | "anycompatiblearray" => base.ends_with("[]"),
        "anynonarray" | "anycompatiblenonarray" => !base.ends_with("[]"),
        "anyenum" => facts.category(&base) == 'E',
        "anyrange" | "anycompatiblerange" => {
            facts.category(&base) == 'R' && !base.ends_with("multirange")
        }
        "anymultirange" | "anycompatiblemultirange" => base.ends_with("multirange"),
        "record" => facts.category(&base) == 'C',
        _ => pg_cast_allows(&base, target, PgCastContext::Implicit),
    }
}

/// Polymorphic arguments of one candidate must agree on their actual type.
fn polymorphic_consistent(inputs: &[OperandType], targets: &[&str]) -> bool {
    let mut element: Option<&str> = None;
    for (input, target) in inputs.iter().zip(targets) {
        let Some(input) = input.as_deref() else {
            continue;
        };
        let actual = match *target {
            "anyelement" | "anynonarray" | "anyenum" => input,
            "anyarray" => match input.strip_suffix("[]") {
                Some(element) => element,
                None => continue,
            },
            _ => continue,
        };
        match element {
            Some(seen) if seen != actual => return false,
            _ => element = Some(actual),
        }
    }
    true
}

fn candidate_accepts(facts: &dyn TypeFacts, inputs: &[OperandType], candidate: &Candidate) -> bool {
    let targets = candidate.args();
    targets.len() == inputs.len()
        && inputs
            .iter()
            .zip(&targets)
            .all(|(input, target)| can_coerce(facts, input, target))
        && polymorphic_consistent(inputs, &targets)
}

/// PostgreSQL's `func_select_candidate`: pick one candidate among several that
/// accept the inputs.
fn select_candidate<'c, 'a>(
    facts: &dyn TypeFacts,
    inputs: &[OperandType],
    candidates: Vec<&'c Candidate<'a>>,
) -> std::result::Result<&'c Candidate<'a>, Vec<&'c Candidate<'a>>> {
    let bases = inputs
        .iter()
        .map(|input| input.as_deref().map(|input| facts.base_type(input)))
        .collect::<Vec<_>>();
    let unknowns = bases.iter().filter(|base| base.is_none()).count();

    // Most exact matches.
    let keep_best = |candidates: Vec<&'c Candidate<'a>>,
                     score: &dyn Fn(&Candidate) -> usize|
     -> Vec<&'c Candidate<'a>> {
        let best = candidates.iter().map(|c| score(c)).max().unwrap_or(0);
        candidates
            .into_iter()
            .filter(|c| score(c) == best)
            .collect()
    };
    let exact = |candidate: &Candidate| {
        candidate
            .args()
            .iter()
            .zip(&bases)
            .filter(|(target, base)| base.as_deref() == Some(**target))
            .count()
    };
    let mut candidates = keep_best(candidates, &exact);
    if candidates.len() == 1 {
        return Ok(candidates[0]);
    }

    // Exact matches or preferred types where coercion is required.
    let categories = bases
        .iter()
        .map(|base| base.as_deref().map(|base| facts.category(base)))
        .collect::<Vec<_>>();
    let preferred = |candidate: &Candidate| {
        candidate
            .args()
            .iter()
            .zip(bases.iter().zip(&categories))
            .filter(|(target, (base, category))| {
                base.is_some()
                    && (base.as_deref() == Some(**target)
                        || (**category == Some(facts.category(target)) && is_preferred(target)))
            })
            .count()
    };
    candidates = keep_best(candidates, &preferred);
    if candidates.len() == 1 {
        return Ok(candidates[0]);
    }
    if unknowns == 0 {
        return Err(candidates);
    }

    // Unknown inputs: prefer the string category, else a common category,
    // and within it a preferred type.
    let mut slots: Vec<Option<(char, bool)>> = vec![None; bases.len()];
    let mut resolved = true;
    for (index, base) in bases.iter().enumerate() {
        if base.is_some() {
            continue;
        }
        let mut slot: Option<(char, bool)> = None;
        let mut conflict = false;
        for candidate in &candidates {
            let target = candidate.args()[index];
            let category = facts.category(target);
            let preferred = is_preferred(target);
            slot = match slot {
                None => Some((category, preferred)),
                Some((slot_category, slot_preferred)) if slot_category == category => {
                    Some((slot_category, slot_preferred || preferred))
                }
                Some(_) if category == 'S' => Some((category, preferred)),
                Some(existing) => {
                    conflict = true;
                    Some(existing)
                }
            };
        }
        if conflict && slot.map(|(category, _)| category) != Some('S') {
            resolved = false;
            break;
        }
        slots[index] = slot;
    }
    if resolved {
        let kept = candidates
            .iter()
            .copied()
            .filter(|candidate| {
                let args = candidate.args();
                slots.iter().enumerate().all(|(index, slot)| match slot {
                    None => true,
                    Some((category, preferred)) => {
                        facts.category(args[index]) == *category
                            && (!preferred || is_preferred(args[index]))
                    }
                })
            })
            .collect::<Vec<_>>();
        if !kept.is_empty() {
            candidates = kept;
            if candidates.len() == 1 {
                return Ok(candidates[0]);
            }
        }
    }

    // Last gasp: unknown inputs take the one type of the known inputs.
    if unknowns < bases.len() {
        let mut known = bases.iter().flatten();
        let first = known.next().cloned();
        if let Some(first) = first.filter(|first| known.all(|other| other == first)) {
            let assumed = vec![Some(first); bases.len()];
            let accepting = candidates
                .iter()
                .copied()
                .filter(|candidate| candidate_accepts(facts, &assumed, candidate))
                .collect::<Vec<_>>();
            if accepting.len() == 1 {
                return Ok(accepting[0]);
            }
        }
    }
    Err(candidates)
}

/// Resolve an operator for operand types by PostgreSQL's rules.
pub(crate) fn resolve_operator<'a>(
    facts: &dyn TypeFacts,
    candidates: &[Candidate<'a>],
    left: Option<&OperandType>,
    right: &OperandType,
) -> Resolution<'a> {
    let outcome = |candidate: &Candidate<'a>| match candidate.user {
        Some(operator) => Resolution::User(operator),
        None => Resolution::Builtin,
    };
    // Exact match; a lone unknown operand is assumed to have the other's type.
    let (exact_left, exact_right) = match (left, right) {
        (Some(None), Some(right)) => (Some(Some(right.clone())), Some(right.clone())),
        (Some(Some(left)), None) => (Some(Some(left.clone())), Some(left.clone())),
        (left, right) => (left.cloned(), right.clone()),
    };
    if let Some(exact_right) = &exact_right {
        if exact_left.as_ref().is_none_or(Option::is_some) {
            let exact_left = exact_left
                .as_ref()
                .map(|left| left.clone().unwrap_or_default());
            let found = candidates
                .iter()
                .find(|candidate| candidate.left == exact_left && candidate.right == *exact_right);
            if let Some(candidate) = found {
                return outcome(candidate);
            }
            // A domain operand matches exactly through its base type.
            let base_left = exact_left.as_deref().map(|left| facts.base_type(left));
            let base_right = facts.base_type(exact_right);
            if base_left != exact_left || base_right != *exact_right {
                if let Some(candidate) = candidates
                    .iter()
                    .find(|candidate| candidate.left == base_left && candidate.right == base_right)
                {
                    return outcome(candidate);
                }
            }
        }
    }
    let inputs = left
        .cloned()
        .into_iter()
        .chain(std::iter::once(right.clone()))
        .collect::<Vec<_>>();
    let accepting = candidates
        .iter()
        .filter(|candidate| candidate_accepts(facts, &inputs, candidate))
        .collect::<Vec<_>>();
    match accepting.len() {
        0 => Resolution::NotFound,
        1 => outcome(accepting[0]),
        _ => match select_candidate(facts, &inputs, accepting) {
            Ok(candidate) => outcome(candidate),
            Err(remaining) => Resolution::Ambiguous {
                user: remaining.iter().any(|candidate| candidate.user.is_some()),
            },
        },
    }
}

// ---------------------------------------------------------------------------
// Statement binding
// ---------------------------------------------------------------------------

/// The operator name an expression operator spells, with its explicit
/// `OPERATOR(schema.name)` namespace.
pub(crate) fn binary_operator_name(op: &BinaryOperator) -> Option<(Option<String>, String)> {
    let name = match op {
        BinaryOperator::PGCustomBinaryOperator(parts) => {
            return match parts.as_slice() {
                [name] => Some((None, name.clone())),
                [schema, name] => Some((Some(schema.to_ascii_lowercase()), name.clone())),
                _ => None,
            };
        }
        BinaryOperator::Custom(name) => name.as_str(),
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::StringConcat => "||",
        BinaryOperator::Gt => ">",
        BinaryOperator::Lt => "<",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "<>",
        BinaryOperator::BitwiseOr => "|",
        BinaryOperator::BitwiseAnd => "&",
        BinaryOperator::PGBitwiseXor => "#",
        BinaryOperator::PGBitwiseShiftLeft => "<<",
        BinaryOperator::PGBitwiseShiftRight => ">>",
        BinaryOperator::PGExp => "^",
        BinaryOperator::PGOverlap => "&&",
        BinaryOperator::PGRegexMatch => "~",
        BinaryOperator::PGRegexIMatch => "~*",
        BinaryOperator::PGRegexNotMatch => "!~",
        BinaryOperator::PGRegexNotIMatch => "!~*",
        BinaryOperator::PGLikeMatch => "~~",
        BinaryOperator::PGILikeMatch => "~~*",
        BinaryOperator::PGNotLikeMatch => "!~~",
        BinaryOperator::PGNotILikeMatch => "!~~*",
        BinaryOperator::PGStartsWith => "^@",
        BinaryOperator::Arrow => "->",
        BinaryOperator::LongArrow => "->>",
        BinaryOperator::HashArrow => "#>",
        BinaryOperator::HashLongArrow => "#>>",
        BinaryOperator::AtArrow => "@>",
        BinaryOperator::ArrowAt => "<@",
        BinaryOperator::AtAt => "@@",
        BinaryOperator::AtQuestion => "@?",
        BinaryOperator::Question => "?",
        BinaryOperator::QuestionAnd => "?&",
        BinaryOperator::QuestionPipe => "?|",
        _ => return None,
    };
    Some((None, name.to_string()))
}

/// The expression operator for a built-in operator name, when it has one.
fn standard_binary_operator(name: &str) -> Option<BinaryOperator> {
    Some(match name {
        "+" => BinaryOperator::Plus,
        "-" => BinaryOperator::Minus,
        "*" => BinaryOperator::Multiply,
        "/" => BinaryOperator::Divide,
        "%" => BinaryOperator::Modulo,
        "||" => BinaryOperator::StringConcat,
        ">" => BinaryOperator::Gt,
        "<" => BinaryOperator::Lt,
        ">=" => BinaryOperator::GtEq,
        "<=" => BinaryOperator::LtEq,
        "=" => BinaryOperator::Eq,
        "<>" | "!=" => BinaryOperator::NotEq,
        "|" => BinaryOperator::BitwiseOr,
        "&" => BinaryOperator::BitwiseAnd,
        "#" => BinaryOperator::PGBitwiseXor,
        "<<" => BinaryOperator::PGBitwiseShiftLeft,
        ">>" => BinaryOperator::PGBitwiseShiftRight,
        "^" => BinaryOperator::PGExp,
        "&&" => BinaryOperator::PGOverlap,
        "~" => BinaryOperator::PGRegexMatch,
        "~*" => BinaryOperator::PGRegexIMatch,
        "!~" => BinaryOperator::PGRegexNotMatch,
        "!~*" => BinaryOperator::PGRegexNotIMatch,
        "~~" => BinaryOperator::PGLikeMatch,
        "~~*" => BinaryOperator::PGILikeMatch,
        "!~~" => BinaryOperator::PGNotLikeMatch,
        "!~~*" => BinaryOperator::PGNotILikeMatch,
        "^@" => BinaryOperator::PGStartsWith,
        "->" => BinaryOperator::Arrow,
        "->>" => BinaryOperator::LongArrow,
        "#>" => BinaryOperator::HashArrow,
        "#>>" => BinaryOperator::HashLongArrow,
        "@>" => BinaryOperator::AtArrow,
        "<@" => BinaryOperator::ArrowAt,
        "@@" => BinaryOperator::AtAt,
        _ => return None,
    })
}

pub(crate) fn unary_operator_name(op: &UnaryOperator) -> Option<&'static str> {
    Some(match op {
        UnaryOperator::Minus => "-",
        UnaryOperator::Plus => "+",
        UnaryOperator::BitwiseNot => "~",
        UnaryOperator::PGSquareRoot => "|/",
        UnaryOperator::PGCubeRoot => "||/",
        UnaryOperator::PGAbs => "@",
        _ => return None,
    })
}

fn qualified_operator_text(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}

fn operand_display(operand: &OperandType) -> &str {
    operand.as_deref().map(display_type).unwrap_or("unknown")
}

/// Statement binder state: the operator catalog, the search path, and the
/// FROM scopes enclosing the expression being visited.
struct Binder<'s, 'db> {
    engine: &'s SqlEngine<'db>,
    facts: CatalogTypeFacts<'s>,
    operators: &'s [OperatorSchema],
    path: Vec<String>,
    scopes: Vec<Vec<RelationColumns>>,
    /// Result types of operator expressions already bound to functions.
    bound_types: HashMap<String, String>,
    changed: bool,
}

impl Binder<'_, '_> {
    fn relation_env(&self, from: &[TableWithJoins]) -> Vec<RelationColumns> {
        let mut env = Vec::new();
        for table in from {
            for factor in std::iter::once(&table.relation)
                .chain(table.joins.iter().map(|join| &join.relation))
            {
                let mut columns = Vec::new();
                if self
                    .engine
                    .collect_relation_columns(factor, &mut columns)
                    .is_some()
                {
                    env.extend(columns);
                }
            }
        }
        env
    }

    fn operand_type(&self, expr: &Expr) -> std::result::Result<OperandType, ()> {
        match expr {
            Expr::Nested(inner) => return self.operand_type(inner),
            Expr::Value(value)
                if matches!(
                    value.value,
                    Value::SingleQuotedString(_) | Value::DollarQuotedString(_) | Value::Null
                ) =>
            {
                return Ok(None);
            }
            _ => {}
        }
        if matches!(expr, Expr::Function(_)) {
            if let Some(pg_type) = self.bound_types.get(&expr.to_string()) {
                return Ok(Some(pg_type.clone()));
            }
        }
        let inner = self.scopes.last().map(Vec::as_slice).unwrap_or(&[]);
        if let Some(pg_type) = self.engine.infer_env_expr_type(expr, inner) {
            return Ok(Some(canonical_type(&pg_type)));
        }
        if self.scopes.len() > 1 {
            let all = self
                .scopes
                .iter()
                .rev()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            if let Some(pg_type) = self.engine.infer_env_expr_type(expr, &all) {
                return Ok(Some(canonical_type(&pg_type)));
            }
        }
        Err(())
    }

    fn has_user_candidates(&self, schema: Option<&str>, name: &str, prefix: bool) -> bool {
        self.operators.iter().any(|operator| {
            operator.id.name == name
                && operator.id.left.is_none() == prefix
                && schema.is_none_or(|schema| operator.id.schema == schema)
        })
    }

    fn call(&mut self, operator: &OperatorSchema, operands: Vec<Expr>) -> Result<Expr> {
        let function = operator.function.as_ref().ok_or_else(|| {
            SqlError::undefined_function(format!(
                "operator is only a shell: {}",
                operator.id.display()
            ))
        })?;
        let declarations = operator
            .left_declaration
            .iter()
            .chain(std::iter::once(&operator.right_declaration))
            .collect::<Vec<_>>();
        let args = operands
            .into_iter()
            .zip(declarations)
            .map(|(operand, declaration)| {
                let data_type = parse_sql_data_type(declaration)?;
                Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(operand),
                    data_type,
                    array: false,
                    format: None,
                })))
            })
            .collect::<Result<Vec<_>>>()?;
        let call = Expr::Function(Function {
            name: ObjectName(vec![
                ObjectNamePart::Identifier(Ident::with_quote('"', function.schema.clone())),
                ObjectNamePart::Identifier(Ident::with_quote('"', function.name.clone())),
            ]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args,
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
        });
        self.bound_types
            .insert(call.to_string(), function.result.clone());
        self.changed = true;
        Ok(call)
    }

    fn bind_expr(&mut self, expr: &mut Expr) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let Some((schema, name)) = binary_operator_name(op) else {
                    return Ok(());
                };
                if schema.as_deref() == Some("pg_catalog") {
                    if let Some(standard) = standard_binary_operator(&name) {
                        *op = standard;
                        self.changed = true;
                    }
                    return Ok(());
                }
                if schema.is_none() && !self.has_user_candidates(None, &name, false) {
                    return Ok(());
                }
                let candidates = operator_candidates(
                    self.operators,
                    &self.path,
                    schema.as_deref(),
                    &name,
                    false,
                );
                let (Ok(left_type), Ok(right_type)) =
                    (self.operand_type(left), self.operand_type(right))
                else {
                    // Untyped operands: an explicitly qualified name with one
                    // defined operator still names that operator.
                    if schema.is_some() {
                        if let [Candidate {
                            user: Some(operator),
                            ..
                        }] = candidates.as_slice()
                        {
                            if !operator.is_shell() {
                                let operator = (*operator).clone();
                                let operands = vec![(**left).clone(), (**right).clone()];
                                *expr = self.call(&operator, operands)?;
                            }
                        }
                    }
                    return Ok(());
                };
                let describe = || {
                    format!(
                        "{} {} {}",
                        operand_display(&left_type),
                        qualified_operator_text(schema.as_deref(), &name),
                        operand_display(&right_type)
                    )
                };
                match resolve_operator(&self.facts, &candidates, Some(&left_type), &right_type) {
                    Resolution::User(operator) => {
                        if operator.is_shell() {
                            return Err(SqlError::undefined_function(format!(
                                "operator is only a shell: {}",
                                describe()
                            )));
                        }
                        let operator = operator.clone();
                        let operands = vec![(**left).clone(), (**right).clone()];
                        *expr = self.call(&operator, operands)?;
                    }
                    Resolution::Builtin => {
                        if schema.is_some() {
                            if let Some(standard) = standard_binary_operator(&name) {
                                *op = standard;
                                self.changed = true;
                            }
                        }
                    }
                    Resolution::NotFound if schema.is_some() => {
                        return Err(SqlError::undefined_function(format!(
                            "operator does not exist: {}",
                            describe()
                        )));
                    }
                    Resolution::Ambiguous { user: true } => {
                        return Err(SqlError::data_exception_public(
                            "42725",
                            format!("operator is not unique: {}", describe()),
                            None,
                        ));
                    }
                    Resolution::NotFound | Resolution::Ambiguous { user: false } => {}
                }
            }
            Expr::UnaryOp { op, expr: operand } => {
                let Some(name) = unary_operator_name(op) else {
                    return Ok(());
                };
                if !self.has_user_candidates(None, name, true) {
                    return Ok(());
                }
                let Ok(operand_type) = self.operand_type(operand) else {
                    return Ok(());
                };
                let candidates = operator_candidates(self.operators, &self.path, None, name, true);
                match resolve_operator(&self.facts, &candidates, None, &operand_type) {
                    Resolution::User(operator) => {
                        if operator.is_shell() {
                            return Err(SqlError::undefined_function(format!(
                                "operator is only a shell: {name} {}",
                                operand_display(&operand_type)
                            )));
                        }
                        let operator = operator.clone();
                        *expr = self.call(&operator, vec![(**operand).clone()])?;
                    }
                    Resolution::Ambiguous { user: true } => {
                        return Err(SqlError::data_exception_public(
                            "42725",
                            format!(
                                "operator is not unique: {name} {}",
                                operand_display(&operand_type)
                            ),
                            None,
                        ));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl sqlparser::ast::VisitorMut for Binder<'_, '_> {
    type Break = Box<SqlError>;

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        let env = match statement {
            Statement::Update(update) => {
                let mut from = vec![update.table.clone()];
                if let Some(
                    UpdateTableFromKind::BeforeSet(tables) | UpdateTableFromKind::AfterSet(tables),
                ) = &update.from
                {
                    from.extend(tables.iter().cloned());
                }
                self.relation_env(&from)
            }
            Statement::Delete(delete) => {
                let mut from = match &delete.from {
                    FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => {
                        from.clone()
                    }
                };
                from.extend(delete.using.iter().flatten().cloned());
                self.relation_env(&from)
            }
            _ => Vec::new(),
        };
        self.scopes.push(env);
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        let env = self.relation_env(&select.from);
        self.scopes.push(env);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &mut Select) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match self.bind_expr(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Box::new(error)),
        }
    }
}

/// Whether any expression of `statement` names an operator `binds` would
/// consider, so statements without one are never cloned.
fn mentions_bindable_operator(statement: &Statement, operators: &[OperatorSchema]) -> bool {
    let binary = operators
        .iter()
        .filter(|operator| operator.id.left.is_some())
        .map(|operator| operator.id.name.as_str())
        .collect::<BTreeSet<_>>();
    let prefix = operators
        .iter()
        .filter(|operator| operator.id.left.is_none())
        .map(|operator| operator.id.name.as_str())
        .collect::<BTreeSet<_>>();
    sqlparser::ast::visit_expressions(statement, |expr| {
        let found = match expr {
            Expr::BinaryOp {
                op: BinaryOperator::PGCustomBinaryOperator(_),
                ..
            } => true,
            Expr::BinaryOp { op, .. } => {
                binary_operator_name(op).is_some_and(|(_, name)| binary.contains(name.as_str()))
            }
            Expr::UnaryOp { op, .. } => {
                unary_operator_name(op).is_some_and(|name| prefix.contains(name))
            }
            _ => false,
        };
        if found {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

/// The user-defined operators `statement` may bind, or `None` when it names
/// none of them (statements are then executed exactly as written).
pub(crate) fn operators_to_bind(
    db: &BicDb,
    statement: &Statement,
) -> Result<Option<Arc<Vec<OperatorSchema>>>> {
    let operators = user_operators(db)?;
    if operators.is_empty()
        || matches!(
            statement,
            Statement::CreateFunction(_)
                | Statement::CreateProcedure { .. }
                | Statement::CreateOperator(_)
                | Statement::AlterOperator(_)
                | Statement::DropOperator(_)
        )
        || !mentions_bindable_operator(statement, &operators)
    {
        return Ok(None);
    }
    Ok(Some(operators))
}

/// Bind the user-defined operators of `statement`: `Some` with the rewritten
/// statement when any operator expression resolved to one.
pub(crate) fn bind_statement_operators(
    engine: &SqlEngine<'_>,
    gucs: &HashMap<String, String>,
    operators: &[OperatorSchema],
    statement: &Statement,
) -> Result<Option<Statement>> {
    let mut bound = statement.clone();
    let mut binder = Binder {
        engine,
        facts: CatalogTypeFacts(engine.db_ref()),
        operators,
        path: effective_search_path(gucs),
        scopes: Vec::new(),
        bound_types: HashMap::new(),
        changed: false,
    };
    if let ControlFlow::Break(error) = sqlparser::ast::VisitMut::visit(&mut bound, &mut binder) {
        return Err(*error);
    }
    Ok(binder.changed.then_some(bound))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BuiltinFacts;

    impl TypeFacts for BuiltinFacts {
        fn base_type(&self, pg_type: &str) -> String {
            pg_type.to_string()
        }
        fn category(&self, pg_type: &str) -> char {
            type_category(pg_type).unwrap_or('U')
        }
    }

    fn user(schema: &str, name: &str, left: Option<&str>, right: &str) -> OperatorSchema {
        OperatorSchema {
            id: OperatorRef {
                schema: schema.into(),
                name: name.into(),
                left: left.map(str::to_string),
                right: right.into(),
            },
            left_declaration: left.map(str::to_string),
            right_declaration: right.into(),
            function: Some(OperatorFunction {
                schema: schema.into(),
                name: "f".into(),
                result: "timestamptz".into(),
            }),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            hashes: false,
            merges: false,
            owner: None,
        }
    }

    fn day_operators() -> Vec<OperatorSchema> {
        vec![
            user("app", "+", Some("interval"), "numeric"),
            user("app", "+", Some("timestamptz"), "numeric"),
            user("app", "-", Some("timestamptz"), "numeric"),
        ]
    }

    fn resolve(
        operators: &[OperatorSchema],
        path: &[&str],
        name: &str,
        left: Option<&str>,
        right: Option<&str>,
    ) -> &'static str {
        let path = path.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let candidates = operator_candidates(operators, &path, None, name, false);
        let left = left.map(str::to_string);
        let right = right.map(str::to_string);
        match resolve_operator(&BuiltinFacts, &candidates, Some(&left), &right) {
            Resolution::User(_) => "user",
            Resolution::Builtin => "builtin",
            Resolution::NotFound => "none",
            Resolution::Ambiguous { .. } => "ambiguous",
        }
    }

    #[test]
    fn builtin_catalog_loads_postgres_operators() {
        let plus = builtin_operators()
            .iter()
            .filter(|operator| operator.name == "+" && operator.kind == 'b')
            .count();
        assert!(plus > 30, "{plus}");
        assert!(builtin_operators()
            .iter()
            .any(|operator| operator.name == "+"
                && operator.left == Some("date")
                && operator.right == "int4"
                && operator.result == "date"));
    }

    #[test]
    fn day_arithmetic_follows_postgres_resolution() {
        let operators = day_operators();
        let path = ["app", "pg_catalog"];
        // Matches PostgreSQL 18 with the same operators on the search path.
        assert_eq!(
            resolve(&operators, &path, "+", Some("timestamptz"), Some("int4")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("timestamptz"), Some("numeric")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("timestamp"), Some("int4")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "-", Some("timestamp"), Some("int4")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("date"), Some("numeric")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("interval"), Some("int4")),
            "user"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("date"), Some("int4")),
            "builtin"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("int4"), Some("int4")),
            "builtin"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("numeric"), Some("int4")),
            "builtin"
        );
        assert_eq!(
            resolve(
                &operators,
                &path,
                "+",
                Some("timestamptz"),
                Some("interval")
            ),
            "builtin"
        );
        assert_eq!(
            resolve(&operators, &path, "+", Some("timestamptz"), None),
            "ambiguous"
        );
        // Not on the search path: no user candidate.
        assert_eq!(
            resolve(
                &operators,
                &["pg_catalog", "public"],
                "+",
                Some("timestamptz"),
                Some("int4")
            ),
            "none"
        );
    }

    #[test]
    fn unknown_literals_prefer_the_string_category() {
        let operators = Vec::new();
        assert_eq!(
            resolve(
                &operators,
                &["pg_catalog", "public"],
                "||",
                Some("text"),
                None
            ),
            "builtin"
        );
        assert_eq!(
            resolve(&operators, &["pg_catalog", "public"], "=", None, None),
            "builtin"
        );
        assert_eq!(
            resolve(
                &operators,
                &["pg_catalog", "public"],
                "+",
                Some("int4"),
                None
            ),
            "builtin"
        );
    }

    #[test]
    fn search_path_defaults_put_pg_catalog_first() {
        let gucs = HashMap::from([("search_path".to_string(), "app, \"Mixed\"".to_string())]);
        assert_eq!(
            effective_search_path(&gucs),
            vec!["pg_catalog", "app", "Mixed"]
        );
        let gucs = HashMap::from([("search_path".to_string(), "app, pg_catalog".to_string())]);
        assert_eq!(effective_search_path(&gucs), vec!["app", "pg_catalog"]);
    }
}
