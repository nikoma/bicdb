//! Runtime options for authored dictionaries using the installed templates.
use super::{is_stopword, TextSearchConfig};
use crate::fts::authored::AuthoredDictionary;
use crate::{Result, SqlError};

pub(crate) const SIMPLE_TEMPLATE: u32 = 3727;
pub(crate) const SNOWBALL_TEMPLATE: u32 = 13234;

pub(crate) fn template_oid(name: &str) -> Option<u32> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "simple" => Some(SIMPLE_TEMPLATE),
        "snowball" => Some(SNOWBALL_TEMPLATE),
        _ => None,
    }
}

fn stopwords(dictionary: &AuthoredDictionary) -> Result<&'static str> {
    let Some(name) = dictionary.option("stopwords") else {
        return Ok("");
    };
    match TextSearchConfig::parse_builtin(name) {
        Ok(TextSearchConfig::Snowball { stopwords, .. }) if !stopwords.is_empty() => Ok(stopwords),
        _ => Err(SqlError::data_exception_public(
            "58P01",
            format!("could not open stop-word file \"{name}.stop\""),
            None,
        )),
    }
}

fn accepts_unknown(dictionary: &AuthoredDictionary) -> Result<bool> {
    let Some(value) = dictionary.option("accept") else {
        return Ok(true);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "on" | "1" => Ok(true),
        "false" | "off" | "0" => Ok(false),
        _ => Err(SqlError::data_exception_public(
            "42601",
            "accept requires a Boolean value",
            None,
        )),
    }
}

pub(super) enum Dictionary {
    Simple {
        stopwords: &'static str,
        accept: bool,
    },
    Snowball(TextSearchConfig),
}

impl Dictionary {
    pub(super) fn compile(object: &AuthoredDictionary) -> Result<Self> {
        let template = match object.template {
            SIMPLE_TEMPLATE => "simple",
            SNOWBALL_TEMPLATE => "snowball",
            _ => {
                return Err(SqlError::data_exception_public(
                    "0A000",
                    "unsupported text search dictionary template",
                    None,
                ))
            }
        };
        for (option, _) in &object.options {
            if !(option == "stopwords"
                || (template == "simple" && option == "accept")
                || (template == "snowball" && option == "language"))
            {
                return Err(SqlError::invalid_parameter_value(format!(
                    "unrecognized {template} dictionary parameter: \"{option}\""
                )));
            }
        }
        let stopwords = stopwords(object)?;
        if object.template == SIMPLE_TEMPLATE {
            return Ok(Self::Simple {
                stopwords,
                accept: accepts_unknown(object)?,
            });
        }
        let language = object
            .option("language")
            .ok_or_else(|| SqlError::invalid_parameter_value("missing language parameter"))?;
        let algorithm = match TextSearchConfig::parse_builtin(&language.to_ascii_lowercase()) {
            Ok(TextSearchConfig::Snowball { algorithm, .. }) => algorithm,
            _ => {
                return Err(SqlError::invalid_parameter_value(format!(
                "no Snowball stemmer available for language \"{language}\" and encoding \"UTF8\""
            )))
            }
        };
        Ok(Self::Snowball(TextSearchConfig::Snowball {
            algorithm,
            stopwords,
        }))
    }

    // None is unrecognized; a recognized stop word has no lexeme.
    pub(super) fn lexize(&self, token: &str) -> Option<Option<String>> {
        match self {
            Self::Simple { stopwords, accept } => {
                if !bicdb_core::full_text_term_is_indexable(token) {
                    return Some(None);
                }
                let normalized = token.to_lowercase();
                if is_stopword(stopwords, &normalized) {
                    Some(None)
                } else if *accept {
                    Some(TextSearchConfig::Simple.normalize_builtin(token))
                } else {
                    None
                }
            }
            Self::Snowball(config) => Some(config.normalize_builtin(token)),
        }
    }
}

pub(crate) fn validate(object: &AuthoredDictionary) -> Result<()> {
    Dictionary::compile(object).map(|_| ())
}
