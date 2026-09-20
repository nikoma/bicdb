//! Trusted Redis-compatible provider for the BicDB application host.
//!
//! This native provider owns endpoints, credentials, connection pools, and
//! physical key/channel scoping. Application WASM receives none of them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bicdb_app_runtime::{AppRuntimeError, RedisProvider, Result};
use r2d2::{ManageConnection, Pool, PooledConnection};
use redis::{Client, Connection, ConnectionLike, RedisError};
use url::Url;

const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_POOL_SIZE: u32 = 256;
const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;
const MAX_IDENTITY_BYTES: usize = 8 * 1024;

#[derive(Clone)]
pub struct RedisProviderConfig {
    pub application: String,
    pub provider: String,
    /// A credential-free `redis://` or `rediss://` origin.
    pub endpoint: Url,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: u32,
    pub key_prefix: String,
    pub channel_prefix: String,
    pub allow_insecure_redis: bool,
    pub pool_size: u32,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for RedisProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisProviderConfig")
            .field("application", &self.application)
            .field("provider", &self.provider)
            .field("endpoint_scheme", &self.endpoint.scheme())
            .field("endpoint_host", &self.endpoint.host_str())
            .field("endpoint_port", &self.endpoint.port())
            .field("username_configured", &self.username.is_some())
            .field("password_configured", &self.password.is_some())
            .field("database", &self.database)
            .field("key_prefix", &self.key_prefix)
            .field("channel_prefix", &self.channel_prefix)
            .field("pool_size", &self.pool_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl RedisProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("Redis application", &self.application)?;
        validate_identifier("Redis provider", &self.provider)?;
        if !matches!(self.endpoint.scheme(), "redis" | "rediss")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
            || !matches!(self.endpoint.path(), "" | "/")
        {
            return provider_error(
                "Redis endpoint must be a credential-free redis(s) origin without path, query, or fragment",
            );
        }
        if self.endpoint.scheme() == "redis" && !self.allow_insecure_redis {
            return provider_error(
                "Redis cleartext endpoint requires explicit allow_insecure_redis operator policy",
            );
        }
        if self.database > 1_000_000
            || self.pool_size == 0
            || self.pool_size > MAX_POOL_SIZE
            || self.connect_timeout.is_zero()
            || self.connect_timeout > MAX_TIMEOUT
            || self.request_timeout.is_zero()
            || self.request_timeout > MAX_TIMEOUT
            || !valid_prefix(&self.key_prefix)
            || !valid_prefix(&self.channel_prefix)
        {
            return provider_error("Redis database, prefixes, pool size, or timeouts are invalid");
        }
        validate_credential("Redis username", self.username.as_deref())?;
        validate_credential("Redis password", self.password.as_deref())?;
        Ok(())
    }

    fn authenticated_url(&self) -> Result<Url> {
        let mut endpoint = self.endpoint.clone();
        if let Some(username) = &self.username {
            endpoint
                .set_username(username)
                .map_err(|_| AppRuntimeError::Provider("Redis username is invalid".to_string()))?;
        }
        if let Some(password) = &self.password {
            endpoint
                .set_password(Some(password))
                .map_err(|_| AppRuntimeError::Provider("Redis password is invalid".to_string()))?;
        }
        endpoint.set_path(&format!("/{}", self.database));
        Ok(endpoint)
    }
}

#[derive(Clone)]
struct Binding {
    config: Arc<RedisProviderConfig>,
    pool: Pool<RedisConnectionManager>,
}

#[derive(Clone)]
struct RedisConnectionManager {
    client: Client,
    connect_timeout: Duration,
}

impl ManageConnection for RedisConnectionManager {
    type Connection = Connection;
    type Error = RedisError;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        self.client
            .get_connection_with_timeout(self.connect_timeout)
    }

    fn is_valid(&self, connection: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        redis::cmd("PING").query::<String>(connection).map(|_| ())
    }

    fn has_broken(&self, connection: &mut Self::Connection) -> bool {
        !connection.is_open()
    }
}

#[derive(Clone)]
pub struct ProductionRedisProvider {
    bindings: Arc<BTreeMap<(String, String), Binding>>,
}

impl std::fmt::Debug for ProductionRedisProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionRedisProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionRedisProvider {
    pub fn new(configs: Vec<RedisProviderConfig>) -> Result<Self> {
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.contains_key(&key) {
                return provider_error("duplicate application Redis provider binding");
            }
            let client = Client::open(config.authenticated_url()?.as_str())
                .map_err(|_| AppRuntimeError::Provider("Redis client setup failed".to_string()))?;
            let pool = Pool::builder()
                .max_size(config.pool_size)
                .connection_timeout(config.connect_timeout)
                .build_unchecked(RedisConnectionManager {
                    client,
                    connect_timeout: config.connect_timeout,
                });
            bindings.insert(
                key,
                Binding {
                    config: Arc::new(config),
                    pool,
                },
            );
        }
        Ok(Self {
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        for binding in self.bindings.values() {
            let mut connection = connection(binding)?;
            redis::cmd("PING")
                .query::<String>(&mut *connection)
                .map_err(|_| AppRuntimeError::Provider("Redis healthcheck failed".to_string()))?;
        }
        Ok(())
    }

    pub fn physical_channel(
        &self,
        application: &str,
        provider: &str,
        channel: &str,
    ) -> Result<String> {
        let binding = self.binding(application, provider)?;
        scoped_channel(&binding.config, application, channel)
    }

    pub fn physical_key(
        &self,
        application: &str,
        provider: &str,
        tenant: Option<&str>,
        key: &str,
    ) -> Result<String> {
        let binding = self.binding(application, provider)?;
        scoped_key(&binding.config, application, tenant, key)
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&Binding> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "Redis-compatible provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl RedisProvider for ProductionRedisProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        let binding = self.binding(application, provider)?;
        let mut connection = connection(binding)?;
        redis::cmd("PING")
            .query::<String>(&mut *connection)
            .map(|_| ())
            .map_err(|_| AppRuntimeError::Provider("Redis provider is unavailable".to_string()))
    }

    fn publish(
        &self,
        application: &str,
        provider: &str,
        channel: &str,
        message: &str,
    ) -> Result<i64> {
        let binding = self.binding(application, provider)?;
        let channel = scoped_channel(&binding.config, application, channel)?;
        let mut connection = connection(binding)?;
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(message)
            .query::<i64>(&mut *connection)
            .map_err(|_| AppRuntimeError::Provider("Redis publish failed".to_string()))
    }

    fn incr(
        &self,
        application: &str,
        provider: &str,
        tenant: Option<&str>,
        key: &str,
    ) -> Result<i64> {
        let binding = self.binding(application, provider)?;
        let key = scoped_key(&binding.config, application, tenant, key)?;
        let mut connection = connection(binding)?;
        redis::cmd("INCR")
            .arg(key)
            .query::<i64>(&mut *connection)
            .map_err(|_| AppRuntimeError::Provider("Redis increment failed".to_string()))
    }
}

fn connection(binding: &Binding) -> Result<PooledConnection<RedisConnectionManager>> {
    let mut connection = binding
        .pool
        .get_timeout(binding.config.connect_timeout)
        .map_err(|_| AppRuntimeError::Provider("Redis connection unavailable".to_string()))?;
    connection
        .set_read_timeout(Some(binding.config.request_timeout))
        .map_err(|_| AppRuntimeError::Provider("Redis connection setup failed".to_string()))?;
    connection
        .set_write_timeout(Some(binding.config.request_timeout))
        .map_err(|_| AppRuntimeError::Provider("Redis connection setup failed".to_string()))?;
    Ok(connection)
}

fn scoped_channel(
    config: &RedisProviderConfig,
    application: &str,
    channel: &str,
) -> Result<String> {
    validate_logical("Redis channel", channel)?;
    Ok(format!(
        "{}:app:{}:channel:{}",
        config.channel_prefix,
        encode(application),
        encode(channel)
    ))
}

fn scoped_key(
    config: &RedisProviderConfig,
    application: &str,
    tenant: Option<&str>,
    key: &str,
) -> Result<String> {
    validate_logical("Redis key", key)?;
    let tenant = tenant.unwrap_or("public");
    if tenant.is_empty()
        || tenant.len() > MAX_IDENTITY_BYTES
        || tenant.chars().any(char::is_control)
    {
        return provider_error("Redis tenant identity is invalid");
    }
    Ok(format!(
        "{}:app:{}:tenant:{}:key:{}",
        config.key_prefix,
        encode(application),
        encode(tenant),
        encode(key)
    ))
}

fn encode(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return provider_error(format!("{label} is invalid"));
    }
    Ok(())
}

fn validate_credential(label: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_CREDENTIAL_BYTES
            || value.chars().any(char::is_control)
    }) {
        return provider_error(format!("{label} is invalid"));
    }
    Ok(())
}

fn valid_prefix(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_logical(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
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

    fn config() -> RedisProviderConfig {
        RedisProviderConfig {
            application: "clinic".to_string(),
            provider: "default".to_string(),
            endpoint: Url::parse("redis://127.0.0.1:6379").unwrap(),
            username: Some("operator".to_string()),
            password: Some("secret-value".to_string()),
            database: 4,
            key_prefix: "carrier-cache".to_string(),
            channel_prefix: "carrier-events".to_string(),
            allow_insecure_redis: true,
            pool_size: 4,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
        }
    }

    #[test]
    fn config_rejects_ambient_credentials_and_implicit_cleartext() {
        let mut candidate = config();
        candidate.endpoint = Url::parse("redis://leak:secret@localhost:6379").unwrap();
        assert!(candidate.validate().is_err());
        let mut candidate = config();
        candidate.allow_insecure_redis = false;
        assert!(candidate.validate().is_err());
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let rendered = format!("{:?}", config());
        assert!(!rendered.contains("secret-value"));
        assert!(!rendered.contains("operator"));
        assert!(rendered.contains("password_configured"));

        let mut malformed = config();
        malformed.endpoint = Url::parse("redis://leak:embedded-secret@localhost:6379").unwrap();
        let rendered = format!("{malformed:?}");
        assert!(!rendered.contains("embedded-secret"));
        assert!(!rendered.contains("leak"));
    }

    #[test]
    fn physical_names_are_application_and_tenant_scoped() {
        let config = config();
        let a = scoped_key(&config, "clinic", Some("tenant-a"), "counter").unwrap();
        let b = scoped_key(&config, "clinic", Some("tenant-b"), "counter").unwrap();
        let public = scoped_key(&config, "clinic", None, "counter").unwrap();
        assert_ne!(a, b);
        assert_ne!(a, public);
        assert_eq!(
            scoped_channel(&config, "clinic", "appointments").unwrap(),
            scoped_channel(&config, "clinic", "appointments").unwrap()
        );
        assert_ne!(
            scoped_channel(&config, "clinic", "appointments").unwrap(),
            scoped_channel(&config, "billing", "appointments").unwrap()
        );
    }
}
