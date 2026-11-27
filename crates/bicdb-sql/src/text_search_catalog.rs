//! Text search parsers, templates, dictionaries and configurations as catalog
//! objects (`pg_ts_parser`, `pg_ts_template`, `pg_ts_dict`, `pg_ts_config`
//! and `pg_ts_config_map`).
//!
//! The built-in objects are those initdb creates (with PostgreSQL's oids);
//! user objects are stored one record per object, keyed by oid, so renames
//! and schema moves rewrite a single record. Text search functions have no
//! database handle, so each statement installs the user objects and the
//! search path on its thread (`ActiveTextSearch`), like user collations.
use crate::*;

pub(crate) const TS_COLLECTION: &str = "__bicdb_pg_ts_objects";

pub(crate) const DEFAULT_PARSER_OID: i64 = 3722;
pub(crate) const SIMPLE_TEMPLATE_OID: i64 = 3727;
pub(crate) const SYNONYM_TEMPLATE_OID: i64 = 3730;
pub(crate) const ISPELL_TEMPLATE_OID: i64 = 3733;
pub(crate) const THESAURUS_TEMPLATE_OID: i64 = 3742;
pub(crate) const SNOWBALL_TEMPLATE_OID: i64 = 13234;
pub(crate) const SIMPLE_DICTIONARY_OID: i64 = 3765;
pub(crate) const SIMPLE_CONFIGURATION_OID: i64 = 3748;

/// The Snowball languages initdb creates a `<language>_stem` dictionary and a
/// `<language>` configuration for, in oid order, with whether a stop-word
/// file of the language's name is used.
pub(crate) const SNOWBALL_LANGUAGES: [(&str, bool); 29] = [
    ("arabic", false),
    ("armenian", false),
    ("basque", false),
    ("catalan", false),
    ("danish", true),
    ("dutch", true),
    ("english", true),
    ("estonian", false),
    ("finnish", true),
    ("french", true),
    ("german", true),
    ("greek", false),
    ("hindi", false),
    ("hungarian", true),
    ("indonesian", false),
    ("irish", false),
    ("italian", true),
    ("lithuanian", false),
    ("nepali", true),
    ("norwegian", true),
    ("portuguese", true),
    ("romanian", false),
    ("russian", true),
    ("serbian", false),
    ("spanish", true),
    ("swedish", true),
    ("tamil", false),
    ("turkish", true),
    ("yiddish", false),
];

/// Token types a built-in configuration maps to its language's stemmer; the
/// other mapped types go to `simple`.
const WORD_TOKEN_TYPES: [i32; 6] = [1, 2, 10, 11, 16, 17];
/// The ASCII word types, which non-Latin configurations stem as English.
const ASCII_WORD_TOKEN_TYPES: [i32; 3] = [1, 11, 16];
/// Every token type a built-in configuration maps (all but blank, tag,
/// protocol and entity).
const MAPPED_TOKEN_TYPES: [i32; 19] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22,
];

/// The built-in text search support functions: oid, name, result type oid.
pub(crate) const TEXT_SEARCH_SUPPORT_FUNCTIONS: [(i64, &str, i64, i64); 15] = [
    (3717, "prsd_start", 2281, 2),
    (3718, "prsd_nexttoken", 2281, 3),
    (3719, "prsd_end", 2278, 1),
    (3720, "prsd_headline", 2281, 3),
    (3721, "prsd_lextype", 2281, 1),
    (3725, "dsimple_init", 2281, 1),
    (3726, "dsimple_lexize", 2281, 4),
    (3728, "dsynonym_init", 2281, 1),
    (3729, "dsynonym_lexize", 2281, 4),
    (3731, "dispell_init", 2281, 1),
    (3732, "dispell_lexize", 2281, 4),
    (3740, "thesaurus_init", 2281, 1),
    (3741, "thesaurus_lexize", 2281, 4),
    (13232, "dsnowball_init", 2281, 1),
    (13233, "dsnowball_lexize", 2281, 4),
];

pub(crate) fn support_function_oid(name: &str) -> i64 {
    TEXT_SEARCH_SUPPORT_FUNCTIONS
        .iter()
        .find(|(_, candidate, _, _)| *candidate == name)
        .map_or(0, |(oid, _, _, _)| *oid)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TsKind {
    Parser,
    Template,
    Dictionary,
    Configuration,
}

impl TsKind {
    /// The object's description in messages ("text search dictionary").
    pub(crate) fn noun(self) -> &'static str {
        match self {
            Self::Parser => "text search parser",
            Self::Template => "text search template",
            Self::Dictionary => "text search dictionary",
            Self::Configuration => "text search configuration",
        }
    }

    /// The object's keyword after TEXT SEARCH ("DICTIONARY").
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Self::Parser => "PARSER",
            Self::Template => "TEMPLATE",
            Self::Dictionary => "DICTIONARY",
            Self::Configuration => "CONFIGURATION",
        }
    }

    fn key_prefix(self) -> &'static str {
        match self {
            Self::Parser => "parser",
            Self::Template => "template",
            Self::Dictionary => "dictionary",
            Self::Configuration => "configuration",
        }
    }

    /// Whether objects of this kind have an owner (parsers and templates
    /// belong to their schema only, as in PostgreSQL).
    pub(crate) fn has_owner(self) -> bool {
        matches!(self, Self::Dictionary | Self::Configuration)
    }
}

/// One dictionary option as written (`name = value`); `numeric` values are
/// shown unquoted in `dictinitoption`, as PostgreSQL does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TsOption {
    pub(crate) name: String,
    pub(crate) value: String,
    pub(crate) numeric: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TsObject {
    pub(crate) kind: TsKind,
    pub(crate) oid: i64,
    pub(crate) schema: String,
    pub(crate) name: String,
    /// Dictionaries and configurations.
    #[serde(default)]
    pub(crate) owner: Option<String>,
    /// Parsers: start, gettoken, end, headline ("" when absent), lextypes.
    /// Templates: init ("" when absent), lexize.
    #[serde(default)]
    pub(crate) methods: Vec<String>,
    /// Dictionaries: the template's oid.
    #[serde(default)]
    pub(crate) template: i64,
    /// Dictionaries: their options in order.
    #[serde(default)]
    pub(crate) options: Vec<TsOption>,
    /// Configurations: the parser's oid.
    #[serde(default)]
    pub(crate) parser: i64,
    /// Configurations: token type to dictionary oids, in token type order.
    #[serde(default)]
    pub(crate) mappings: Vec<(i32, Vec<i64>)>,
}

impl TsObject {
    pub(crate) fn key(&self) -> String {
        format!("{}:{}", self.kind.key_prefix(), self.oid)
    }

    /// `dictinitoption` text: `name = 'value', ...`, or `None` without options.
    pub(crate) fn init_option_text(&self) -> Option<String> {
        if self.options.is_empty() {
            return None;
        }
        Some(
            self.options
                .iter()
                .map(|option| {
                    let value = if option.numeric {
                        option.value.clone()
                    } else {
                        let quoted = option.value.replace('\'', "''");
                        if option.value.contains('\\') {
                            format!("E'{}'", quoted.replace('\\', "\\\\"))
                        } else {
                            format!("'{quoted}'")
                        }
                    };
                    format!("{} = {value}", quote_ts_identifier(&option.name))
                })
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

/// `quote_identifier`: names that are not plain lower-case identifiers are
/// double-quoted.
pub(crate) fn quote_ts_identifier(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_');
    if plain && !is_reserved_ts_word(name) {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn is_reserved_ts_word(name: &str) -> bool {
    matches!(
        name,
        "all"
            | "and"
            | "any"
            | "as"
            | "default"
            | "end"
            | "from"
            | "select"
            | "table"
            | "user"
            | "where"
    )
}

fn builtin_object(kind: TsKind, oid: i64, name: &str) -> TsObject {
    TsObject {
        kind,
        oid,
        schema: "pg_catalog".to_string(),
        name: name.to_string(),
        owner: kind.has_owner().then(|| BOOTSTRAP_ROLE_NAME.to_string()),
        methods: Vec::new(),
        template: 0,
        options: Vec::new(),
        parser: 0,
        mappings: Vec::new(),
    }
}

fn builtin_template(oid: i64, name: &str, init: &str, lexize: &str) -> TsObject {
    TsObject {
        methods: vec![init.to_string(), lexize.to_string()],
        ..builtin_object(TsKind::Template, oid, name)
    }
}

fn option(name: &str, value: &str) -> TsOption {
    TsOption {
        name: name.to_string(),
        value: value.to_string(),
        numeric: false,
    }
}

/// The objects initdb creates, with PostgreSQL's oids.
pub(crate) fn builtin_objects() -> &'static [TsObject] {
    static OBJECTS: OnceLock<Vec<TsObject>> = OnceLock::new();
    OBJECTS.get_or_init(|| {
        let mut objects = vec![
            TsObject {
                methods: [
                    "prsd_start",
                    "prsd_nexttoken",
                    "prsd_end",
                    "prsd_headline",
                    "prsd_lextype",
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
                ..builtin_object(TsKind::Parser, DEFAULT_PARSER_OID, "default")
            },
            builtin_template(
                SIMPLE_TEMPLATE_OID,
                "simple",
                "dsimple_init",
                "dsimple_lexize",
            ),
            builtin_template(
                SYNONYM_TEMPLATE_OID,
                "synonym",
                "dsynonym_init",
                "dsynonym_lexize",
            ),
            builtin_template(
                ISPELL_TEMPLATE_OID,
                "ispell",
                "dispell_init",
                "dispell_lexize",
            ),
            builtin_template(
                THESAURUS_TEMPLATE_OID,
                "thesaurus",
                "thesaurus_init",
                "thesaurus_lexize",
            ),
            builtin_template(
                SNOWBALL_TEMPLATE_OID,
                "snowball",
                "dsnowball_init",
                "dsnowball_lexize",
            ),
            TsObject {
                template: SIMPLE_TEMPLATE_OID,
                ..builtin_object(TsKind::Dictionary, SIMPLE_DICTIONARY_OID, "simple")
            },
            TsObject {
                parser: DEFAULT_PARSER_OID,
                mappings: MAPPED_TOKEN_TYPES
                    .iter()
                    .map(|token| (*token, vec![SIMPLE_DICTIONARY_OID]))
                    .collect(),
                ..builtin_object(TsKind::Configuration, SIMPLE_CONFIGURATION_OID, "simple")
            },
        ];
        let english_stem = snowball_dictionary_oid("english").expect("english is a language");
        for (index, (language, stopwords)) in SNOWBALL_LANGUAGES.iter().enumerate() {
            let dictionary = 13_235 + 2 * index as i64;
            let mut options = vec![option("language", language)];
            if *stopwords {
                options.push(option("stopwords", language));
            }
            objects.push(TsObject {
                template: SNOWBALL_TEMPLATE_OID,
                options,
                ..builtin_object(TsKind::Dictionary, dictionary, &format!("{language}_stem"))
            });
            let ascii_english = matches!(*language, "hindi" | "russian");
            objects.push(TsObject {
                parser: DEFAULT_PARSER_OID,
                mappings: MAPPED_TOKEN_TYPES
                    .iter()
                    .map(|token| {
                        let target = if ascii_english && ASCII_WORD_TOKEN_TYPES.contains(token) {
                            english_stem
                        } else if WORD_TOKEN_TYPES.contains(token) {
                            dictionary
                        } else {
                            SIMPLE_DICTIONARY_OID
                        };
                        (*token, vec![target])
                    })
                    .collect(),
                ..builtin_object(TsKind::Configuration, dictionary + 1, language)
            });
        }
        objects
    })
}

pub(crate) fn snowball_dictionary_oid(language: &str) -> Option<i64> {
    SNOWBALL_LANGUAGES
        .iter()
        .position(|(candidate, _)| *candidate == language)
        .map(|index| 13_235 + 2 * index as i64)
}

/// What a statement installed: the user objects and the session settings
/// the search path is read from (computed on first use).
struct ActiveEntry {
    user: Arc<Vec<TsObject>>,
    gucs: Arc<HashMap<String, String>>,
    scope: std::cell::OnceCell<Arc<TsScope>>,
}

thread_local! {
    static CATALOG: RefCell<Option<(u64, u64, Arc<Vec<TsObject>>)>> = const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<Rc<ActiveEntry>>> = const { RefCell::new(None) };
}

/// The user text search objects of `db`, in oid order.
pub(crate) fn user_ts_objects(db: &BicDb) -> Result<Arc<Vec<TsObject>>> {
    let database = db.instance_id();
    let generation = db.collection_generation(TS_COLLECTION);
    if let Some(list) = CATALOG.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(TS_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut list = records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<TsObject>>>()?;
    list.sort_by_key(|object| object.oid);
    let list = Arc::new(list);
    CATALOG.with(|cache| *cache.borrow_mut() = Some((database, generation, list.clone())));
    Ok(list)
}

pub(crate) fn save_ts_object(db: &mut BicDb, object: &TsObject) -> Result<()> {
    db.create_collection(TS_COLLECTION)?;
    db.insert(
        TS_COLLECTION,
        Record::new(object.key()).with_metadata(serde_json::to_value(object)?),
    )?;
    Ok(())
}

pub(crate) fn delete_ts_object(db: &mut BicDb, key: &str) -> Result<()> {
    match db.delete(TS_COLLECTION, key) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// The text search objects one statement sees: the built-ins, the user
/// objects and the search path that resolves unqualified names.
#[derive(Debug, Default)]
pub(crate) struct TsScope {
    pub(crate) user: Arc<Vec<TsObject>>,
    pub(crate) search_path: Vec<String>,
}

impl TsScope {
    pub(crate) fn new(user: Arc<Vec<TsObject>>, search_path: Vec<String>) -> Self {
        Self { user, search_path }
    }

    /// Every object: the built-ins (unless a user record replaced one, as
    /// ALTER of a built-in object stores) and the user objects.
    pub(crate) fn objects(&self) -> impl Iterator<Item = &TsObject> {
        builtin_objects()
            .iter()
            .filter(|builtin| {
                !self
                    .user
                    .iter()
                    .any(|object| object.kind == builtin.kind && object.oid == builtin.oid)
            })
            .chain(self.user.iter())
    }

    pub(crate) fn by_oid(&self, kind: TsKind, oid: i64) -> Option<&TsObject> {
        self.objects()
            .find(|object| object.kind == kind && object.oid == oid)
    }

    pub(crate) fn in_schema(&self, kind: TsKind, schema: &str, name: &str) -> Option<&TsObject> {
        self.objects()
            .find(|object| object.kind == kind && object.schema == schema && object.name == name)
    }

    /// Resolves a (possibly qualified) name: a qualified name in its schema,
    /// an unqualified one along the search path.
    pub(crate) fn resolve(&self, kind: TsKind, parts: &[String]) -> Option<&TsObject> {
        match parts {
            [name] => self
                .search_path
                .iter()
                .find_map(|schema| self.in_schema(kind, schema, name)),
            [schema, name] | [_, schema, name] => self.in_schema(kind, schema, name),
            _ => None,
        }
    }

    /// Resolves the text of a regconfig/regdictionary value: an oid or a
    /// name.
    pub(crate) fn resolve_text(&self, kind: TsKind, text: &str) -> Option<&TsObject> {
        if let Ok(oid) = text.trim().parse::<i64>() {
            return self.by_oid(kind, oid);
        }
        self.resolve(kind, &split_ts_name(text)?)
    }

    /// The name a regconfig/regdictionary value prints as: qualified unless
    /// its schema is found first on the search path.
    pub(crate) fn display_name(&self, object: &TsObject) -> String {
        let visible = self.resolve(object.kind, std::slice::from_ref(&object.name));
        let name = quote_ts_identifier(&object.name);
        if visible.is_some_and(|found| found.oid == object.oid) {
            name
        } else {
            format!("{}.{name}", quote_ts_identifier(&object.schema))
        }
    }
}

/// Splits `schema.name` text into identifier parts: unquoted parts are
/// lower-cased, quoted parts kept. `None` when the text is not a name.
pub(crate) fn split_ts_name(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.trim().chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    '"' => break,
                    ch => part.push(ch),
                }
            }
        } else {
            while let Some(&ch) = chars.peek() {
                if ch == '.' {
                    break;
                }
                part.extend(ch.to_lowercase());
                chars.next();
            }
            part = part.trim().to_string();
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') => continue,
            Some(_) => return None,
        }
    }
}

/// The text search objects of the current statement on this thread for the
/// guard's lifetime. Nested guards restore the outer one.
pub(crate) struct ActiveTextSearch(Option<Rc<ActiveEntry>>);

impl ActiveTextSearch {
    pub(crate) fn install(db: &BicDb, gucs: &Arc<HashMap<String, String>>) -> Result<Self> {
        let entry = Rc::new(ActiveEntry {
            user: user_ts_objects(db)?,
            gucs: gucs.clone(),
            scope: std::cell::OnceCell::new(),
        });
        Ok(Self(
            ACTIVE.with(|active| active.borrow_mut().replace(entry)),
        ))
    }
}

impl Drop for ActiveTextSearch {
    fn drop(&mut self) {
        let previous = self.0.take();
        ACTIVE.with(|active| *active.borrow_mut() = previous);
    }
}

/// The active scope; outside a statement, the built-ins on the default path.
pub(crate) fn active_ts_scope() -> Arc<TsScope> {
    let entry = ACTIVE.with(|active| active.borrow().clone());
    match entry {
        Some(entry) => entry
            .scope
            .get_or_init(|| {
                Arc::new(TsScope::new(
                    entry.user.clone(),
                    crate::pg_operators::effective_search_path(&entry.gucs),
                ))
            })
            .clone(),
        None => Arc::new(TsScope::new(
            Arc::new(Vec::new()),
            vec!["pg_catalog".to_string(), "public".to_string()],
        )),
    }
}

/// The active scope, or outside a statement `db`'s objects on the default
/// search path.
pub(crate) fn ts_scope_for(db: Option<&BicDb>) -> Arc<TsScope> {
    if ACTIVE.with(|active| active.borrow().is_some()) {
        return active_ts_scope();
    }
    match db.map(user_ts_objects) {
        Some(Ok(user)) => Arc::new(TsScope::new(
            user,
            vec!["pg_catalog".to_string(), "public".to_string()],
        )),
        _ => active_ts_scope(),
    }
}

/// `regconfig` / `regdictionary` input: the oid of the named object.
pub(crate) fn regconfig_oid(db: Option<&BicDb>, kind: TsKind, text: &str) -> Option<i64> {
    ts_scope_for(db)
        .resolve_text(kind, text)
        .map(|object| object.oid)
}

/// `regconfig` / `regdictionary` output for an oid.
pub(crate) fn regconfig_name(db: Option<&BicDb>, kind: TsKind, oid: i64) -> Option<String> {
    let scope = ts_scope_for(db);
    let object = scope.by_oid(kind, oid)?;
    Some(scope.display_name(object))
}

/// The `pg_ts_*` catalog rows.
pub(crate) fn ts_catalog_rows(db: &BicDb, table: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let scope = TsScope::new(user_ts_objects(db)?, Vec::new());
    let objects = scope.objects();
    let regproc = |name: &str| SqlValue::Int(support_function_oid(name));
    let namespace = |schema: &str| SqlValue::Int(namespace_oid(schema));
    let owner = |object: &TsObject| {
        SqlValue::Int(match object.owner.as_deref() {
            None | Some(BOOTSTRAP_ROLE_NAME) => 10,
            Some(owner) => role_oid(owner),
        })
    };
    let mut rows = Vec::new();
    match table {
        "pg_ts_parser" => {
            for object in objects.filter(|object| object.kind == TsKind::Parser) {
                let method =
                    |index: usize| regproc(object.methods.get(index).map_or("", String::as_str));
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(object.oid)),
                    ("prsname", SqlValue::String(object.name.clone())),
                    ("prsnamespace", namespace(&object.schema)),
                    ("prsstart", method(0)),
                    ("prstoken", method(1)),
                    ("prsend", method(2)),
                    ("prsheadline", method(3)),
                    ("prslextype", method(4)),
                ]));
            }
        }
        "pg_ts_template" => {
            for object in objects.filter(|object| object.kind == TsKind::Template) {
                let method =
                    |index: usize| regproc(object.methods.get(index).map_or("", String::as_str));
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(object.oid)),
                    ("tmplname", SqlValue::String(object.name.clone())),
                    ("tmplnamespace", namespace(&object.schema)),
                    ("tmplinit", method(0)),
                    ("tmpllexize", method(1)),
                ]));
            }
        }
        "pg_ts_dict" => {
            for object in objects.filter(|object| object.kind == TsKind::Dictionary) {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(object.oid)),
                    ("dictname", SqlValue::String(object.name.clone())),
                    ("dictnamespace", namespace(&object.schema)),
                    ("dictowner", owner(object)),
                    ("dicttemplate", SqlValue::Int(object.template)),
                    (
                        "dictinitoption",
                        object
                            .init_option_text()
                            .map_or(SqlValue::Null, SqlValue::String),
                    ),
                ]));
            }
        }
        "pg_ts_config" => {
            for object in objects.filter(|object| object.kind == TsKind::Configuration) {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(object.oid)),
                    ("cfgname", SqlValue::String(object.name.clone())),
                    ("cfgnamespace", namespace(&object.schema)),
                    ("cfgowner", owner(object)),
                    ("cfgparser", SqlValue::Int(object.parser)),
                ]));
            }
        }
        "pg_ts_config_map" => {
            for object in objects.filter(|object| object.kind == TsKind::Configuration) {
                for (token, dictionaries) in &object.mappings {
                    for (index, dictionary) in dictionaries.iter().enumerate() {
                        rows.push(virtual_row([
                            ("mapcfg", SqlValue::Int(object.oid)),
                            ("maptokentype", SqlValue::Int(i64::from(*token))),
                            ("mapseqno", SqlValue::Int(index as i64 + 1)),
                            ("mapdict", SqlValue::Int(*dictionary)),
                        ]));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(rows)
}

pub(crate) const TS_CATALOG_COLUMN_TYPES: [(&str, &[(&str, &str)]); 5] = [
    (
        "pg_ts_parser",
        &[
            ("oid", "oid"),
            ("prsname", "name"),
            ("prsnamespace", "oid"),
            ("prsstart", "regproc"),
            ("prstoken", "regproc"),
            ("prsend", "regproc"),
            ("prsheadline", "regproc"),
            ("prslextype", "regproc"),
        ],
    ),
    (
        "pg_ts_template",
        &[
            ("oid", "oid"),
            ("tmplname", "name"),
            ("tmplnamespace", "oid"),
            ("tmplinit", "regproc"),
            ("tmpllexize", "regproc"),
        ],
    ),
    (
        "pg_ts_dict",
        &[
            ("oid", "oid"),
            ("dictname", "name"),
            ("dictnamespace", "oid"),
            ("dictowner", "oid"),
            ("dicttemplate", "oid"),
            ("dictinitoption", "text"),
        ],
    ),
    (
        "pg_ts_config",
        &[
            ("oid", "oid"),
            ("cfgname", "name"),
            ("cfgnamespace", "oid"),
            ("cfgowner", "oid"),
            ("cfgparser", "oid"),
        ],
    ),
    (
        "pg_ts_config_map",
        &[
            ("mapcfg", "oid"),
            ("maptokentype", "int4"),
            ("mapseqno", "int4"),
            ("mapdict", "oid"),
        ],
    ),
];
