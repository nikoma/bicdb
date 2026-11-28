//! Durable, authored text search configurations. A copied configuration
//! retains the built-in normalization semantics it was created from.
use crate::eval::{
    builtin_regconfig_name, builtin_regdictionary_name, REGCONFIG_OBJECTS, REGDICTIONARY_OBJECTS,
};
use crate::{pg_quote_ident, BicDb, BicDbError, Record, Result, SqlError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::thread::LocalKey;

pub(crate) const COLLECTION: &str = "__bicdb_pg_ts_configurations";
pub(crate) const DICTIONARIES: &str = "__bicdb_pg_ts_dictionaries";

pub(crate) type DictionaryMappings = BTreeMap<u8, Vec<u32>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthoredConfig {
    pub(crate) oid: u32,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) owner: String,
    pub(crate) source_oid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mappings: Option<Arc<DictionaryMappings>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthoredDictionary {
    pub(crate) oid: u32,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) owner: String,
    pub(crate) template: u32,
    pub(crate) options: Vec<(String, String)>,
}

impl AuthoredDictionary {
    pub(crate) fn option(&self, name: &str) -> Option<&str> {
        self.options
            .iter()
            .find_map(|(option, value)| (name == option).then_some(value.as_str()))
    }

    pub(crate) fn qualified_name(&self) -> String {
        format!(
            "{}.{}",
            pg_quote_ident(&self.schema),
            pg_quote_ident(&self.name)
        )
    }
}

type CatalogCache<T> = RefCell<Option<(u64, u64, Arc<Vec<T>>)>>;

thread_local! {
    static CONFIG_CACHE: CatalogCache<AuthoredConfig> = const { RefCell::new(None) };
    static DICTIONARY_CACHE: CatalogCache<AuthoredDictionary> = const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<Catalog>> = const { RefCell::new(None) };
    static WIRE_SETTINGS: RefCell<Option<Arc<HashMap<String, String>>>> = const { RefCell::new(None) };
}

pub(crate) fn list(db: &BicDb) -> Result<Arc<Vec<AuthoredConfig>>> {
    load_objects(db, COLLECTION, &CONFIG_CACHE)
}

pub(crate) fn list_dictionaries(db: &BicDb) -> Result<Arc<Vec<AuthoredDictionary>>> {
    load_objects(db, DICTIONARIES, &DICTIONARY_CACHE)
}

fn load_objects<T: DeserializeOwned + 'static>(
    db: &BicDb,
    collection: &str,
    cache: &'static LocalKey<CatalogCache<T>>,
) -> Result<Arc<Vec<T>>> {
    let id = db.instance_id();
    let stamp = db.collection_generation(collection);
    if let Some(records) = cache.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .and_then(|(cached_id, cached_stamp, records)| {
                (*cached_id == id && *cached_stamp == stamp).then(|| records.clone())
            })
    }) {
        return Ok(records);
    }
    let rows = match db.scan_collection(collection) {
        Ok(rows) => rows,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let values = rows
        .into_iter()
        .map(|row| serde_json::from_value(row.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<T>>>()?;
    let values = Arc::new(values);
    cache.with(|cache| *cache.borrow_mut() = Some((id, stamp, values.clone())));
    Ok(values)
}

pub(crate) fn store(db: &mut BicDb, config: &AuthoredConfig) -> Result<()> {
    store_object(db, COLLECTION, config.oid, config)
}

pub(crate) fn store_dictionary(db: &mut BicDb, dictionary: &AuthoredDictionary) -> Result<()> {
    store_object(db, DICTIONARIES, dictionary.oid, dictionary)
}

fn store_object(db: &mut BicDb, collection: &str, oid: u32, object: &impl Serialize) -> Result<()> {
    db.create_collection(collection)?;
    db.insert(
        collection,
        Record::new(oid.to_string()).with_metadata(serde_json::to_value(object)?),
    )?;
    Ok(())
}

pub(crate) fn delete(db: &mut BicDb, oid: u32) -> Result<()> {
    delete_object(db, COLLECTION, oid)
}

pub(crate) fn delete_dictionary(db: &mut BicDb, oid: u32) -> Result<()> {
    delete_object(db, DICTIONARIES, oid)
}

fn delete_object(db: &mut BicDb, collection: &str, oid: u32) -> Result<()> {
    match db.delete(collection, &oid.to_string()) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) enum Configuration<'a> {
    Builtin(u32),
    Authored(&'a AuthoredConfig),
}

impl Configuration<'_> {
    pub(crate) fn oid(&self) -> u32 {
        match self {
            Self::Builtin(oid) => *oid,
            Self::Authored(config) => config.oid,
        }
    }

    pub(crate) fn mappings(&self) -> Option<Arc<DictionaryMappings>> {
        match self {
            Self::Authored(config) => config.mappings.clone(),
            Self::Builtin(_) => None,
        }
    }

    pub(crate) fn source_oid(&self) -> u32 {
        match self {
            Self::Builtin(oid) => *oid,
            Self::Authored(config) => config.source_oid,
        }
    }
}

pub(crate) struct Catalog {
    authored: Arc<Vec<AuthoredConfig>>,
    dictionaries: Arc<Vec<AuthoredDictionary>>,
    search_path: Vec<String>,
    inaccessible: BTreeSet<String>,
}

impl Catalog {
    pub(crate) fn load(db: &BicDb, gucs: &HashMap<String, String>) -> Result<Self> {
        let authored = list(db)?;
        let dictionaries = list_dictionaries(db)?;
        let mut inaccessible = BTreeSet::new();
        let mut search_path = Vec::new();
        if !authored.is_empty() || !dictionaries.is_empty() {
            let role = crate::schema_meta::current_user_from_gucs(gucs);
            let schemas: BTreeSet<_> = authored
                .iter()
                .map(|config| config.schema.as_str())
                .chain(
                    dictionaries
                        .iter()
                        .map(|dictionary| dictionary.schema.as_str()),
                )
                .collect();
            for schema in schemas {
                if !crate::schema_meta::role_can_use_schema(db, &role, schema)? {
                    inaccessible.insert(schema.to_owned());
                }
            }
            search_path = configuration_search_path(gucs);
        }
        Ok(Self {
            authored,
            dictionaries,
            search_path,
            inaccessible,
        })
    }

    pub(crate) fn resolve(&self, input: &str) -> Result<Option<Configuration<'_>>> {
        if let Ok(oid) = input.trim().parse::<u32>() {
            return Ok(self
                .authored
                .iter()
                .find(|object| object.oid == oid)
                .map(Configuration::Authored)
                .or_else(|| {
                    builtin_regconfig_name(i64::from(oid)).map(|_| Configuration::Builtin(oid))
                }));
        }
        let Some(parts) = qualified_name(input) else {
            return Ok(None);
        };
        match parts.as_slice() {
            [name] if self.authored.is_empty() => Ok(self.in_schema("pg_catalog", name)),
            [name] => Ok(self
                .search_path
                .iter()
                .filter(|schema| !self.inaccessible.contains(*schema))
                .find_map(|schema| self.in_schema(schema, name))),
            [schema, name] => {
                if self.inaccessible.contains(schema) {
                    return Err(SqlError::data_exception_public(
                        "42501",
                        format!("permission denied for schema {schema}"),
                        None,
                    ));
                }
                Ok(self.in_schema(schema, name))
            }
            _ => Ok(None),
        }
    }

    fn resolve_authored(&self, input: &str) -> Result<Option<&AuthoredConfig>> {
        if self.authored.is_empty() {
            return Ok(None);
        }
        Ok(match self.resolve(input)? {
            Some(Configuration::Authored(config)) => Some(config),
            _ => None,
        })
    }

    pub(crate) fn dictionary_oid(&self, input: &str) -> Result<Option<u32>> {
        if let Ok(oid) = input.trim().parse::<u32>() {
            return Ok((self.find_dictionary(oid).is_some()
                || builtin_regdictionary_name(i64::from(oid)).is_some())
            .then_some(oid));
        }
        let Some(parts) = qualified_name(input) else {
            return Ok(None);
        };
        match parts.as_slice() {
            [name] if self.dictionaries.is_empty() => {
                Ok(self.dictionary_in_schema("pg_catalog", name))
            }
            [name] => Ok(self
                .search_path
                .iter()
                .filter(|schema| !self.inaccessible.contains(*schema))
                .find_map(|schema| self.dictionary_in_schema(schema, name))),
            [schema, name] => {
                if self.inaccessible.contains(schema) {
                    return Err(SqlError::data_exception_public(
                        "42501",
                        format!("permission denied for schema {schema}"),
                        None,
                    ));
                }
                Ok(self.dictionary_in_schema(schema, name))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn find_dictionary(&self, oid: u32) -> Option<&AuthoredDictionary> {
        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.oid == oid)
    }

    pub(crate) fn dictionary_in_schema(&self, schema: &str, name: &str) -> Option<u32> {
        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.schema == schema && dictionary.name == name)
            .map(|dictionary| dictionary.oid)
            .or_else(|| {
                (schema == "pg_catalog").then_some(())?;
                REGDICTIONARY_OBJECTS
                    .iter()
                    .find_map(|&(oid, candidate)| (name == candidate).then_some(oid))
            })
    }

    pub(crate) fn in_schema(&self, schema: &str, name: &str) -> Option<Configuration<'_>> {
        self.authored
            .iter()
            .find(|object| object.schema == schema && object.name == name)
            .map(Configuration::Authored)
            .or_else(|| {
                (schema == "pg_catalog").then_some(())?;
                REGCONFIG_OBJECTS.iter().find_map(|&(oid, candidate)| {
                    (name == candidate).then_some(Configuration::Builtin(oid))
                })
            })
    }
}

pub(crate) fn qualified_name(input: &str) -> Option<Vec<String>> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, input)
        .tokenize()
        .ok()?;
    let mut tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)));
    let mut names = Vec::new();
    loop {
        let Some(Token::Word(word)) = tokens.next() else {
            return None;
        };
        names.push(if word.quote_style.is_some() {
            word.value
        } else {
            word.value.to_ascii_lowercase()
        });
        match tokens.next() {
            None => return Some(names),
            Some(Token::Period) if names.len() == 1 => {}
            _ => return None,
        }
    }
}

fn configuration_search_path(gucs: &HashMap<String, String>) -> Vec<String> {
    let role = crate::schema_meta::current_user_from_gucs(gucs);
    let path = gucs
        .get("search_path")
        .map_or("\"$user\", public", String::as_str);
    let mut path = crate::search_path::parse_search_path(path, &role);
    if !path.iter().any(|schema| schema == "pg_catalog") {
        path.insert(0, "pg_catalog".to_owned());
    }
    path
}

/// Publishes the connection search path while encoding catalog references.
pub struct TextSearchReferenceScope(Option<Arc<HashMap<String, String>>>);

impl TextSearchReferenceScope {
    pub fn enter(gucs: Arc<HashMap<String, String>>) -> Self {
        Self(WIRE_SETTINGS.with(|active| active.replace(Some(gucs))))
    }
}

impl Drop for TextSearchReferenceScope {
    fn drop(&mut self) {
        WIRE_SETTINGS.with(|active| active.replace(self.0.take()));
    }
}

fn wire_catalog(db: &BicDb) -> Result<Catalog> {
    WIRE_SETTINGS.with(|active| match active.borrow().as_ref() {
        Some(gucs) => Catalog::load(db, gucs),
        None => Catalog::load(db, &HashMap::new()),
    })
}

pub(crate) struct Scope(Option<Catalog>);

impl Scope {
    pub(crate) fn enter(db: &BicDb, gucs: &HashMap<String, String>) -> Result<Self> {
        let catalog = Catalog::load(db, gucs)?;
        Ok(Self(ACTIVE.with(|active| active.replace(Some(catalog)))))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.replace(self.0.take()));
    }
}

pub(crate) fn oid(input: &str) -> Option<u32> {
    ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()?
            .resolve_authored(input)
            .ok()
            .flatten()
            .map(|config| config.oid)
    })
}

pub(super) fn normalization(input: &str) -> Result<Option<(u32, Option<Arc<DictionaryMappings>>)>> {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(catalog) = active.as_ref() else {
            return Ok(None);
        };
        Ok(catalog
            .resolve_authored(input)?
            .map(|config| (config.source_oid, config.mappings.clone())))
    })
}

impl Catalog {
    pub(crate) fn display_name(&self, oid: i64) -> Option<String> {
        let config = self
            .authored
            .iter()
            .find(|object| i64::from(object.oid) == oid)?;
        let name = pg_quote_ident(&config.name);
        if self
            .resolve(&name)
            .is_ok_and(|found| found.is_some_and(|visible| visible.oid() == config.oid))
        {
            Some(name)
        } else {
            Some(config.qualified_name())
        }
    }
}

pub(crate) fn resolve_oid(db: &BicDb, input: &str) -> Result<Option<u32>> {
    let active = ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(|catalog| Ok(catalog.resolve_authored(input)?.map(|config| config.oid)))
    });
    match active {
        Some(oid) => oid,
        None => Ok(wire_catalog(db)?
            .resolve_authored(input)?
            .map(|config| config.oid)),
    }
}

pub(crate) fn display_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    if builtin_regconfig_name(oid).is_some() {
        return Ok(None);
    }
    let active = ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(|catalog| catalog.display_name(oid))
    });
    match active {
        Some(name) => Ok(name),
        None => Ok(wire_catalog(db)?.display_name(oid)),
    }
}

pub(crate) fn dictionary_oid(db: &BicDb, input: &str) -> Result<Option<u32>> {
    let active = ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(|catalog| catalog.dictionary_oid(input))
    });
    match active {
        Some(result) => result,
        None => wire_catalog(db)?.dictionary_oid(input),
    }
}

pub(crate) fn active_dictionary_oid(input: &str) -> Result<Option<u32>> {
    ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(|catalog| catalog.dictionary_oid(input))
            .unwrap_or(Ok(None))
    })
}

pub(super) fn with_dictionary<T>(
    oid: u32,
    read: impl FnOnce(&AuthoredDictionary) -> T,
) -> Option<T> {
    ACTIVE.with(|active| active.borrow().as_ref()?.find_dictionary(oid).map(read))
}

pub(crate) fn dictionary_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    fn output(catalog: &Catalog, oid: u32) -> Option<String> {
        let dictionary = catalog.find_dictionary(oid)?;
        let short = pg_quote_ident(&dictionary.name);
        if catalog.dictionary_oid(&short).ok().flatten() == Some(oid) {
            Some(short)
        } else {
            Some(dictionary.qualified_name())
        }
    }

    let Ok(oid) = u32::try_from(oid) else {
        return Ok(None);
    };
    if builtin_regdictionary_name(i64::from(oid)).is_some() {
        return Ok(None);
    }
    let active = ACTIVE.with(|active| active.borrow().as_ref().map(|catalog| output(catalog, oid)));
    match active {
        Some(name) => Ok(name),
        None => Ok(output(&wire_catalog(db)?, oid)),
    }
}

impl AuthoredConfig {
    pub(crate) fn effective_mappings(&self) -> Arc<DictionaryMappings> {
        self.mappings
            .clone()
            .unwrap_or_else(|| Arc::new(super::catalog::builtin_config_mappings(self.source_oid)))
    }

    pub(crate) fn qualified_name(&self) -> String {
        format!(
            "{}.{}",
            pg_quote_ident(&self.schema),
            pg_quote_ident(&self.name)
        )
    }
}
