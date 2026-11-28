use super::TextSearchConfig;
use crate::eval::{builtin_regconfig_name, REGCONFIG_OBJECTS, REGDICTIONARY_OBJECTS};
use crate::{namespace_oid, pg_quote_literal, role_oid, virtual_row, BicDb, Result, SqlValue};
use std::collections::BTreeMap;

const DEFAULT_PARSER: i64 = 3722;
const SIMPLE_TEMPLATE: i64 = super::dictionary::SIMPLE_TEMPLATE as i64;
const SNOWBALL_TEMPLATE: i64 = super::dictionary::SNOWBALL_TEMPLATE as i64;
const PG_CATALOG_NAMESPACE: i64 = 11;
const BOOTSTRAP_OWNER: i64 = 10;

pub(crate) fn rows(db: &BicDb, table: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let namespace = || SqlValue::Int(PG_CATALOG_NAMESPACE);
    let owner = || SqlValue::Int(BOOTSTRAP_OWNER);
    let text = |value: &str| SqlValue::String(value.to_owned());
    Ok(match table {
        "pg_ts_config" => {
            let mut rows: Vec<_> = REGCONFIG_OBJECTS
                .iter()
                .map(|&(oid, name)| {
                    virtual_row([
                        ("oid", SqlValue::Int(i64::from(oid))),
                        ("cfgname", text(name)),
                        ("cfgnamespace", namespace()),
                        ("cfgowner", owner()),
                        ("cfgparser", SqlValue::Int(DEFAULT_PARSER)),
                    ])
                })
                .collect();
            let authored = super::authored::list(db)?;
            rows.extend(authored.iter().map(|config| {
                virtual_row([
                    ("oid", SqlValue::Int(i64::from(config.oid))),
                    ("cfgname", text(&config.name)),
                    ("cfgnamespace", SqlValue::Int(namespace_oid(&config.schema))),
                    ("cfgowner", SqlValue::Int(role_oid(&config.owner))),
                    ("cfgparser", SqlValue::Int(DEFAULT_PARSER)),
                ])
            }));
            rows
        }
        "pg_ts_dict" => {
            let mut rows: Vec<_> = REGDICTIONARY_OBJECTS.iter().map(|&(oid, name)| {
            let (template, options) = match name.strip_suffix("_stem") {
                Some(language) => {
                    let mut options = format!("language = '{language}'");
                    if matches!(
                        TextSearchConfig::parse_builtin(language),
                        Ok(TextSearchConfig::Snowball { stopwords, .. }) if !stopwords.is_empty()
                    ) {
                        options.push_str(&format!(", stopwords = '{language}'"));
                    }
                    (SNOWBALL_TEMPLATE, SqlValue::String(options))
                }
                None => (SIMPLE_TEMPLATE, SqlValue::Null),
            };
            virtual_row([
                ("oid", SqlValue::Int(i64::from(oid))),
                ("dictname", text(name)),
                ("dictnamespace", namespace()),
                ("dictowner", owner()),
                ("dicttemplate", SqlValue::Int(template)),
                ("dictinitoption", options),
            ])
            }).collect();
            let authored = super::authored::list_dictionaries(db)?;
            rows.extend(authored.iter().map(|dictionary| {
                let options = if dictionary.options.is_empty() {
                    SqlValue::Null
                } else {
                    SqlValue::String(
                        dictionary
                            .options
                            .iter()
                            .map(|(name, value)| format!("{name} = {}", pg_quote_literal(value)))
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                };
                virtual_row([
                    ("oid", SqlValue::Int(i64::from(dictionary.oid))),
                    ("dictname", text(&dictionary.name)),
                    (
                        "dictnamespace",
                        SqlValue::Int(namespace_oid(&dictionary.schema)),
                    ),
                    ("dictowner", SqlValue::Int(role_oid(&dictionary.owner))),
                    (
                        "dicttemplate",
                        SqlValue::Int(i64::from(dictionary.template)),
                    ),
                    ("dictinitoption", options),
                ])
            }));
            rows
        }
        "pg_ts_template" => [
            (SIMPLE_TEMPLATE, "simple", 3725, 3726),
            (SNOWBALL_TEMPLATE, "snowball", 13232, 13233),
        ]
        .into_iter()
        .map(|(oid, name, init, lexize)| {
            virtual_row([
                ("oid", SqlValue::Int(oid)),
                ("tmplname", text(name)),
                ("tmplnamespace", namespace()),
                ("tmplinit", SqlValue::Int(init)),
                ("tmpllexize", SqlValue::Int(lexize)),
            ])
        })
        .collect(),
        "pg_ts_parser" => vec![virtual_row([
            ("oid", SqlValue::Int(DEFAULT_PARSER)),
            ("prsname", text("default")),
            ("prsnamespace", namespace()),
            ("prsstart", SqlValue::Int(3717)),
            ("prstoken", SqlValue::Int(3718)),
            ("prsend", SqlValue::Int(3719)),
            ("prsheadline", SqlValue::Int(3720)),
            ("prslextype", SqlValue::Int(3721)),
        ])],
        _ => Vec::new(),
    })
}

// PostgreSQL's default parser maps these token types for each stock config.
const MAPPED_TOKENS: [u8; 19] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22,
];
const STEMMED_TOKENS: [u8; 6] = [1, 2, 10, 11, 16, 17];
const ASCII_STEMMED_TOKENS: [u8; 3] = [1, 11, 16];

pub(super) fn builtin_config_mappings(oid: u32) -> super::authored::DictionaryMappings {
    let Some(language) = builtin_regconfig_name(i64::from(oid)) else {
        return Default::default();
    };
    let stemmer_name = format!("{language}_stem");
    MAPPED_TOKENS
        .into_iter()
        .filter_map(|token| {
            let dictionary = if language == "simple" || !STEMMED_TOKENS.contains(&token) {
                "simple"
            } else if matches!(language, "hindi" | "russian")
                && ASCII_STEMMED_TOKENS.contains(&token)
            {
                "english_stem"
            } else {
                &stemmer_name
            };
            let &(oid, _) = REGDICTIONARY_OBJECTS
                .iter()
                .find(|&&(_, name)| name == dictionary)?;
            Some((token, vec![oid]))
        })
        .collect()
}

pub(crate) fn config_mapping_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let authored = super::authored::list(db)?;
    let mut rows = Vec::new();
    for &(oid, _) in REGCONFIG_OBJECTS {
        rows.extend(mapping_rows(oid, &builtin_config_mappings(oid)));
    }
    for config in authored.iter() {
        rows.extend(mapping_rows(config.oid, &config.effective_mappings()));
    }
    Ok(rows)
}

fn mapping_rows(
    config_oid: u32,
    mappings: &super::authored::DictionaryMappings,
) -> Vec<BTreeMap<String, SqlValue>> {
    mappings
        .iter()
        .flat_map(|(&token, dictionaries)| {
            dictionaries
                .iter()
                .enumerate()
                .map(move |(index, &dictionary_oid)| {
                    virtual_row([
                        ("mapcfg", SqlValue::Int(i64::from(config_oid))),
                        ("maptokentype", SqlValue::Int(i64::from(token))),
                        ("mapseqno", SqlValue::Int((index + 1) as i64)),
                        ("mapdict", SqlValue::Int(i64::from(dictionary_oid))),
                    ])
                })
        })
        .collect()
}
