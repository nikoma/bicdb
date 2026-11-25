//! User collations (`CREATE COLLATION`): their catalog, the per-statement
//! registry that name resolution reads, and ICU comparison.
//!
//! Collation names reach comparison code as strings. A user collation's
//! name is resolved, while its statement runs, into a self-describing
//! *semantic* name that carries the provider, locale and determinism, so
//! the comparison functions (which have no database handle) never need a
//! catalog lookup and can never silently fall back to byte order.
use crate::*;
use icu_collator::options::{AlternateHandling, CaseLevel, CollatorOptions, Strength};
use icu_collator::{Collator, CollatorBorrowed, CollatorPreferences};

pub(crate) const COLLATION_COLLECTION: &str = "__bicdb_pg_collations";
const SEMANTIC_MARK: char = '\u{1}';

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CollationSchema {
    pub(crate) name: String,
    pub(crate) schema_name: String,
    pub(crate) oid: i64,
    pub(crate) owner: String,
    /// `i` (ICU) or `c` (libc), as `pg_collation.collprovider`.
    pub(crate) provider: char,
    /// Canonical locale: a BCP 47 tag for ICU, `C` or `POSIX` for libc.
    pub(crate) locale: String,
    pub(crate) deterministic: bool,
}

impl CollationSchema {
    /// The semantic name comparison code receives for this collation.
    pub(crate) fn semantic_name(&self) -> String {
        format!(
            "{SEMANTIC_MARK}{}{SEMANTIC_MARK}{}{SEMANTIC_MARK}{}{SEMANTIC_MARK}{}",
            self.provider,
            if self.deterministic { "d" } else { "n" },
            self.locale,
            self.name
        )
    }
}

struct SemanticCollation<'a> {
    provider: char,
    deterministic: bool,
    locale: &'a str,
    name: &'a str,
}

fn decode(semantic: &str) -> Option<SemanticCollation<'_>> {
    let rest = semantic.strip_prefix(SEMANTIC_MARK)?;
    let mut parts = rest.splitn(4, SEMANTIC_MARK);
    let provider = parts.next()?.chars().next()?;
    let deterministic = parts.next()? == "d";
    let locale = parts.next()?;
    let name = parts.next()?;
    Some(SemanticCollation {
        provider,
        deterministic,
        locale,
        name,
    })
}

/// The user-visible name of a (possibly semantic) collation name.
pub(crate) fn display_name(collation: &str) -> &str {
    decode(collation).map_or(collation, |semantic| semantic.name)
}

/// Whether a semantic collation compares by something other than bytes
/// for equality.
pub(crate) fn is_nondeterministic(collation: &str) -> bool {
    decode(collation).is_some_and(|semantic| !semantic.deterministic)
}

thread_local! {
    static CATALOG: RefCell<Option<(u64, u64, Arc<Vec<CollationSchema>>)>> =
        const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<Arc<Vec<CollationSchema>>>> = const { RefCell::new(None) };
    static COLLATORS: RefCell<HashMap<String, Option<Rc<CollatorBorrowed<'static>>>>> =
        RefCell::new(HashMap::new());
}

pub(crate) fn user_collations(db: &BicDb) -> Result<Arc<Vec<CollationSchema>>> {
    let database = db.instance_id();
    let generation = db.collection_generation(COLLATION_COLLECTION);
    if let Some(list) = CATALOG.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(COLLATION_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut list = records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<CollationSchema>>>()?;
    list.sort_by(|left, right| left.oid.cmp(&right.oid));
    let list = Arc::new(list);
    CATALOG.with(|cache| *cache.borrow_mut() = Some((database, generation, list.clone())));
    Ok(list)
}

fn collation_key(schema_name: &str, name: &str) -> String {
    format!("{schema_name}.{name}")
}

pub(crate) fn load_collation(
    db: &BicDb,
    schema_name: &str,
    name: &str,
) -> Result<Option<CollationSchema>> {
    Ok(user_collations(db)?
        .iter()
        .find(|collation| collation.schema_name == schema_name && collation.name == name)
        .cloned())
}

pub(crate) fn save_collation(db: &mut BicDb, collation: &CollationSchema) -> Result<()> {
    db.create_collection(COLLATION_COLLECTION)?;
    db.insert(
        COLLATION_COLLECTION,
        Record::new(collation_key(&collation.schema_name, &collation.name))
            .with_metadata(serde_json::to_value(collation)?),
    )?;
    Ok(())
}

pub(crate) fn delete_collation(db: &mut BicDb, schema_name: &str, name: &str) -> Result<()> {
    match db.delete(COLLATION_COLLECTION, &collation_key(schema_name, name)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Makes `db`'s user collations resolvable by name on this thread for the
/// guard's lifetime (one statement). Nested guards restore the outer one.
pub(crate) struct ActiveCollations(Option<Arc<Vec<CollationSchema>>>);

impl ActiveCollations {
    pub(crate) fn install(db: &BicDb) -> Result<Self> {
        let list = user_collations(db)?;
        Ok(Self(
            ACTIVE.with(|active| active.borrow_mut().replace(list)),
        ))
    }
}

impl Drop for ActiveCollations {
    fn drop(&mut self) {
        let previous = self.0.take();
        ACTIVE.with(|active| *active.borrow_mut() = previous);
    }
}

/// The active user collations (empty outside a statement).
pub(crate) fn active_collations() -> Arc<Vec<CollationSchema>> {
    ACTIVE.with(|active| active.borrow().clone().unwrap_or_default())
}

/// A user collation named `name` (optionally schema-qualified), resolved
/// through the active registry.
pub(crate) fn resolve_user_collation(schema: Option<&str>, name: &str) -> Option<CollationSchema> {
    active_collations()
        .iter()
        .find(|collation| {
            collation.name == name && schema.is_none_or(|schema| collation.schema_name == schema)
        })
        .cloned()
}

/// The semantic name for a stored or parsed collation name: built-in names
/// are returned unchanged, user collations become self-describing.
pub(crate) fn semantic_collation(name: String) -> String {
    if name.starts_with(SEMANTIC_MARK) {
        return name;
    }
    match resolve_user_collation(None, &name) {
        Some(collation) => collation.semantic_name(),
        None => name,
    }
}

/// Splits a BCP 47 tag's `-u-` extension into its keywords.
fn unicode_keywords(locale: &str) -> Vec<(String, String)> {
    let lower = locale.to_ascii_lowercase();
    let mut keywords = Vec::new();
    let mut parts = lower
        .split('-')
        .skip_while(|part| *part != "u")
        .skip(1)
        .peekable();
    while let Some(part) = parts.next() {
        if part.len() == 1 {
            break;
        }
        if part.len() == 2 {
            let mut value = Vec::new();
            while let Some(next) = parts.peek() {
                if next.len() <= 2 {
                    break;
                }
                value.push(parts.next().unwrap().to_string());
            }
            keywords.push((part.to_string(), value.join("-")));
        }
    }
    keywords
}

fn collator_options(locale: &str) -> CollatorOptions {
    let mut options = CollatorOptions::default();
    for (key, value) in unicode_keywords(locale) {
        match (key.as_str(), value.as_str()) {
            ("ks", "level1") => options.strength = Some(Strength::Primary),
            ("ks", "level2") => options.strength = Some(Strength::Secondary),
            ("ks", "level3") => options.strength = Some(Strength::Tertiary),
            ("ks", "level4") => options.strength = Some(Strength::Quaternary),
            ("ks", "identic") => options.strength = Some(Strength::Identical),
            ("ka", "shifted") => options.alternate_handling = Some(AlternateHandling::Shifted),
            ("ka", "noignore") => {
                options.alternate_handling = Some(AlternateHandling::NonIgnorable)
            }
            ("kc", "" | "true" | "yes") => options.case_level = Some(CaseLevel::On),
            ("kc", "false" | "no") => options.case_level = Some(CaseLevel::Off),
            _ => {}
        }
    }
    options
}

/// Validates and canonicalizes an ICU locale (BCP 47 or `ll_CC` form).
pub(crate) fn canonical_icu_locale(locale: &str) -> Result<String> {
    let bcp = locale.trim().replace('_', "-");
    let parsed = icu_locale_core::Locale::try_from_str(&bcp).map_err(|_| {
        SqlError::data_exception(
            "22023",
            format!("could not convert locale name \"{locale}\" to language tag"),
            None,
        )
    })?;
    Ok(parsed.to_string())
}

fn icu_collator(locale: &str) -> Option<Rc<CollatorBorrowed<'static>>> {
    COLLATORS.with(|cache| {
        if let Some(collator) = cache.borrow().get(locale) {
            return collator.clone();
        }
        let built = icu_locale_core::Locale::try_from_str(locale)
            .ok()
            .and_then(|parsed| {
                Collator::try_new(CollatorPreferences::from(&parsed), collator_options(locale)).ok()
            })
            .map(Rc::new);
        cache.borrow_mut().insert(locale.to_string(), built.clone());
        built
    })
}

/// The ordering a semantic user collation gives `left` and `right`, or
/// `None` when `collation` is not one.
pub(crate) fn user_collation_ordering(
    collation: &str,
    left: &str,
    right: &str,
) -> Option<Ordering> {
    let semantic = decode(collation)?;
    if semantic.provider == 'c' {
        return Some(left.as_bytes().cmp(right.as_bytes()));
    }
    let ordering = match icu_collator(semantic.locale) {
        Some(collator) => collator.compare(left, right),
        // A locale ICU cannot build was rejected at CREATE COLLATION; keep a
        // total order rather than panic if data is missing.
        None => left.cmp(right),
    };
    Some(if ordering == Ordering::Equal && semantic.deterministic {
        left.as_bytes().cmp(right.as_bytes())
    } else {
        ordering
    })
}

/// Whether ICU can build a collator for `locale`.
pub(crate) fn icu_locale_is_supported(locale: &str) -> bool {
    icu_collator(locale).is_some()
}

/// Nondeterministic collations change equality for every operation that
/// compares strings. BicDB applies them to comparison operators and ORDER
/// BY; anywhere else (grouping, DISTINCT, joins, IN lists, column and index
/// definitions, ...) they would silently compare bytes, so such statements
/// are refused.
#[inline(never)]
pub(crate) fn reject_unsupported_nondeterministic(statement: &Statement) -> Result<()> {
    let active = active_collations();
    if !active.iter().any(|collation| !collation.deterministic) {
        return Ok(());
    }
    let nondeterministic = |name: &ObjectName| {
        object_name(name).ok().is_some_and(|name| {
            let bare = name.rsplit('.').next().unwrap_or(&name).to_string();
            active
                .iter()
                .any(|collation| !collation.deterministic && collation.name == bare)
        })
    };
    let mut allowed: BTreeSet<usize> = BTreeSet::new();
    let direct_collate = |expr: &Expr| -> Option<usize> {
        let mut expr = expr;
        while let Expr::Nested(inner) = expr {
            expr = inner;
        }
        matches!(expr, Expr::Collate { .. }).then_some(expr as *const Expr as usize)
    };
    let _ = sqlparser::ast::visit_expressions(statement, |expr| {
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
            ) {
                allowed.extend(direct_collate(left));
                allowed.extend(direct_collate(right));
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    collect_order_by_collates(statement, &mut allowed, &direct_collate);
    let mut refused = None;
    let _ = sqlparser::ast::visit_expressions(statement, |expr| {
        if let Expr::Collate { collation, .. } = expr {
            if nondeterministic(collation) && !allowed.contains(&(expr as *const Expr as usize)) {
                refused = Some(display_object_name(collation));
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    if let Some(name) = refused {
        return Err(SqlError::Unsupported(format!(
            "nondeterministic collation \"{name}\" is supported only in comparison operators and ORDER BY"
        )));
    }
    Ok(())
}

fn display_object_name(name: &ObjectName) -> String {
    object_name(name).unwrap_or_else(|_| name.to_string())
}

fn collect_order_by_collates(
    statement: &Statement,
    allowed: &mut BTreeSet<usize>,
    direct_collate: &dyn Fn(&Expr) -> Option<usize>,
) {
    struct Orders<'a> {
        allowed: &'a mut BTreeSet<usize>,
        direct_collate: &'a dyn Fn(&Expr) -> Option<usize>,
    }
    impl sqlparser::ast::Visitor for Orders<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
            if let Some(order) = &query.order_by {
                if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order.kind {
                    for order in exprs {
                        self.allowed.extend((self.direct_collate)(&order.expr));
                    }
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = sqlparser::ast::Visit::visit(
        statement,
        &mut Orders {
            allowed,
            direct_collate,
        },
    );
}

/// A column, domain or index definition naming a nondeterministic
/// collation (see `reject_unsupported_nondeterministic`).
pub(crate) fn reject_nondeterministic_definition(collation: &str) -> Result<()> {
    let semantic = semantic_collation(collation.to_string());
    if is_nondeterministic(&semantic) {
        return Err(SqlError::Unsupported(format!(
            "nondeterministic collation \"{}\" is not supported for columns, domains or indexes",
            display_name(&semantic)
        )));
    }
    Ok(())
}
