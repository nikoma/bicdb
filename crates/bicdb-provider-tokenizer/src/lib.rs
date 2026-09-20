//! Operator-bound local tokenizer provider for BicDB application applications in BicDB.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bicdb_app_runtime::{AppRuntimeError, Result, TokenizerProvider};
use tokenizers::Tokenizer;

#[derive(Clone, Debug)]
pub struct TokenizerProviderConfig {
    pub application: String,
    pub provider: String,
    pub tokenizer_file: PathBuf,
    pub max_input_bytes: u64,
    pub max_tokens: u64,
}

impl TokenizerProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("tokenizer application", &self.application)?;
        validate_identifier("tokenizer provider", &self.provider)?;
        if !self.tokenizer_file.is_file() {
            return provider_error("tokenizer file does not exist");
        }
        if self.max_input_bytes == 0 || self.max_tokens == 0 {
            return provider_error("tokenizer bounds must be positive");
        }
        Ok(())
    }
}

struct Binding {
    tokenizer: Tokenizer,
    max_input_bytes: u64,
    max_tokens: u64,
}

pub struct ProductionTokenizerProvider {
    bindings: Arc<BTreeMap<(String, String), Binding>>,
}

impl std::fmt::Debug for ProductionTokenizerProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionTokenizerProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionTokenizerProvider {
    pub fn new(configs: Vec<TokenizerProviderConfig>) -> Result<Self> {
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.contains_key(&key) {
                return provider_error("duplicate application tokenizer provider binding");
            }
            let tokenizer = Tokenizer::from_file(&config.tokenizer_file)
                .map_err(|_| AppRuntimeError::Provider("failed to load tokenizer".to_string()))?;
            bindings.insert(
                key,
                Binding {
                    tokenizer,
                    max_input_bytes: config.max_input_bytes,
                    max_tokens: config.max_tokens,
                },
            );
        }
        Ok(Self {
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        Ok(())
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&Binding> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "tokenizer provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl TokenizerProvider for ProductionTokenizerProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        self.binding(application, provider).map(|_| ())
    }

    fn count(&self, application: &str, provider: &str, text: &str) -> Result<u64> {
        let binding = self.binding(application, provider)?;
        if text.len() as u64 > binding.max_input_bytes {
            return provider_error("tokenizer input exceeds operator bounds");
        }
        let tokens = binding
            .tokenizer
            .encode(text, false)
            .map_err(|_| AppRuntimeError::Provider("tokenizer inference failed".to_string()))?
            .len() as u64;
        if tokens > binding.max_tokens {
            return provider_error("tokenizer result exceeds operator bounds");
        }
        Ok(tokens)
    }
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return provider_error(format!("{label} is invalid"));
    }
    Ok(())
}

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

#[cfg(test)]
mod tests {
    use tokenizers::models::wordlevel::WordLevel;
    use tokenizers::pre_tokenizers::whitespace::Whitespace;

    use super::*;

    #[test]
    fn loads_and_counts_an_operator_tokenizer_without_exposing_text() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tokenizer.json");
        let vocabulary = [
            ("[UNK]".to_string(), 0),
            ("hello".to_string(), 1),
            ("world".to_string(), 2),
        ]
        .into_iter()
        .collect();
        let model = WordLevel::builder()
            .vocab(vocabulary)
            .unk_token("[UNK]".to_string())
            .build()
            .unwrap();
        let mut tokenizer = Tokenizer::new(model);
        tokenizer.with_pre_tokenizer(Some(Whitespace {}));
        tokenizer.save(&path, false).unwrap();
        let provider = ProductionTokenizerProvider::new(vec![TokenizerProviderConfig {
            application: "app".to_string(),
            provider: "default".to_string(),
            tokenizer_file: path,
            max_input_bytes: 64,
            max_tokens: 4,
        }])
        .unwrap();
        assert_eq!(provider.count("app", "default", "hello world").unwrap(), 2);
        assert!(provider.count("app", "default", &"x".repeat(65)).is_err());
        assert!(provider.count("other", "default", "hello").is_err());
    }
}
