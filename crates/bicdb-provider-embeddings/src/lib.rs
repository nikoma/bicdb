//! Operator-bound local embedding provider for BicDB application applications in BicDB.

use std::collections::BTreeMap;
use std::sync::Arc;

use bicdb_app_runtime::{AppRuntimeError, EmbeddingsProvider, Result};
use bicdb_core::ModelRegistryEntry;

#[derive(Clone, Debug)]
pub struct EmbeddingsProviderConfig {
    pub application: String,
    pub provider: String,
    pub model: ModelRegistryEntry,
    pub max_input_bytes: u64,
}

impl EmbeddingsProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("embedding application", &self.application)?;
        validate_identifier("embedding provider", &self.provider)?;
        if self.model.dimension == 0 || self.max_input_bytes == 0 {
            return provider_error("embedding dimension and input bound must be positive");
        }
        Ok(())
    }
}

pub struct ProductionEmbeddingsProvider {
    bindings: Arc<BTreeMap<(String, String), EmbeddingsProviderConfig>>,
}

impl std::fmt::Debug for ProductionEmbeddingsProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionEmbeddingsProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionEmbeddingsProvider {
    pub fn new(configs: Vec<EmbeddingsProviderConfig>) -> Result<Self> {
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.insert(key, config).is_some() {
                return provider_error("duplicate application embedding provider binding");
            }
        }
        Ok(Self {
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        for binding in self.bindings.values() {
            let embedding = binding.model.embed("bicdb readiness")?;
            if embedding.len() != binding.model.dimension
                || embedding.iter().any(|value| !value.is_finite())
            {
                return provider_error("embedding model failed its readiness probe");
            }
        }
        Ok(())
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&EmbeddingsProviderConfig> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "embedding provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl EmbeddingsProvider for ProductionEmbeddingsProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        self.binding(application, provider).map(|_| ())
    }

    fn embed(
        &self,
        application: &str,
        provider: &str,
        text: &str,
        dimensions: u32,
    ) -> Result<Vec<f32>> {
        let binding = self.binding(application, provider)?;
        if text.len() as u64 > binding.max_input_bytes
            || dimensions as usize != binding.model.dimension
        {
            return provider_error("embedding request exceeds operator bounds");
        }
        binding.model.embed(text).map_err(AppRuntimeError::from)
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
    use super::*;

    #[test]
    fn enforces_application_dimension_and_input_bounds() {
        let provider = ProductionEmbeddingsProvider::new(vec![EmbeddingsProviderConfig {
            application: "app".to_string(),
            provider: "default".to_string(),
            model: ModelRegistryEntry::local_test("test", 3),
            max_input_bytes: 16,
        }])
        .unwrap();
        provider.healthcheck().unwrap();
        let vector = provider.embed("app", "default", "hello", 3).unwrap();
        assert_eq!(vector.len(), 3);
        assert!(vector.iter().all(|value| value.is_finite()));
        assert!(provider.embed("app", "default", "hello", 4).is_err());
        assert!(provider
            .embed("app", "default", &"x".repeat(17), 3)
            .is_err());
    }
}
