use super::TextSearchConfig;
use crate::eval::{REGCONFIG_OBJECTS, REGDICTIONARY_OBJECTS};
use crate::{virtual_row, SqlValue};
use std::collections::BTreeMap;

const DEFAULT_PARSER: i64 = 3722;
const SIMPLE_TEMPLATE: i64 = 3727;
const SNOWBALL_TEMPLATE: i64 = 13234;
const PG_CATALOG_NAMESPACE: i64 = 11;
const BOOTSTRAP_OWNER: i64 = 10;

pub(crate) fn rows(table: &str) -> Vec<BTreeMap<String, SqlValue>> {
    let namespace = || SqlValue::Int(PG_CATALOG_NAMESPACE);
    let owner = || SqlValue::Int(BOOTSTRAP_OWNER);
    let text = |value: &str| SqlValue::String(value.to_owned());
    match table {
        "pg_ts_config" => REGCONFIG_OBJECTS.iter().map(|&(oid, name)| {
            virtual_row([
                ("oid", SqlValue::Int(i64::from(oid))),
                ("cfgname", text(name)),
                ("cfgnamespace", namespace()),
                ("cfgowner", owner()),
                ("cfgparser", SqlValue::Int(DEFAULT_PARSER)),
            ])
        }).collect(),
        "pg_ts_dict" => REGDICTIONARY_OBJECTS.iter().map(|&(oid, name)| {
            let (template, options) = match name.strip_suffix("_stem") {
                Some(language) => {
                    let mut options = format!("language = '{language}'");
                    if matches!(
                        TextSearchConfig::parse(&text(language)),
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
        }).collect(),
        "pg_ts_template" => [
            (SIMPLE_TEMPLATE, "simple", 3725, 3726),
            (SNOWBALL_TEMPLATE, "snowball", 13232, 13233),
        ].into_iter().map(|(oid, name, init, lexize)| {
            virtual_row([
                ("oid", SqlValue::Int(oid)),
                ("tmplname", text(name)),
                ("tmplnamespace", namespace()),
                ("tmplinit", SqlValue::Int(init)),
                ("tmpllexize", SqlValue::Int(lexize)),
            ])
        }).collect(),
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
    }
}
