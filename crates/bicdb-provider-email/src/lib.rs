//! Trusted SMTP provider for BicDB application applications hosted by BicDB.
//!
//! SMTP endpoints, credentials, pooled transports, and sender policy remain
//! native host state. Application WASM receives only a bounded delivery
//! receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bicdb_app_runtime::{AppRuntimeError, EmailDelivery, EmailMessage, EmailProvider, Result};
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::PoolConfig;
use lettre::{Message, SmtpTransport, Transport};
use url::Url;
use uuid::Uuid;

const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_POOL_SIZE: u32 = 128;
const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct EmailProviderConfig {
    pub application: String,
    pub provider: String,
    /// Credential-free `smtp://` or `smtps://` origin.
    pub endpoint: Url,
    pub username: Option<String>,
    pub password: Option<String>,
    pub allowed_from: BTreeSet<String>,
    pub allowed_recipient_domains: BTreeSet<String>,
    pub allow_insecure_smtp: bool,
    pub max_recipients: u32,
    pub max_message_bytes: u64,
    pub pool_size: u32,
    pub timeout: Duration,
}

impl std::fmt::Debug for EmailProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmailProviderConfig")
            .field("application", &self.application)
            .field("provider", &self.provider)
            .field("endpoint_scheme", &self.endpoint.scheme())
            .field("endpoint_host", &self.endpoint.host_str())
            .field("endpoint_port", &self.endpoint.port())
            .field("username_configured", &self.username.is_some())
            .field("password_configured", &self.password.is_some())
            .field("allowed_from", &self.allowed_from)
            .field("allowed_recipient_domains", &self.allowed_recipient_domains)
            .field("allow_insecure_smtp", &self.allow_insecure_smtp)
            .field("max_recipients", &self.max_recipients)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("pool_size", &self.pool_size)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl EmailProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("email application", &self.application)?;
        validate_identifier("email provider", &self.provider)?;
        if !matches!(self.endpoint.scheme(), "smtp" | "smtps")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
            || !matches!(self.endpoint.path(), "" | "/")
        {
            return provider_error(
                "SMTP endpoint must be a credential-free smtp(s) origin without path, query, or fragment",
            );
        }
        if self.endpoint.scheme() == "smtp" && !self.allow_insecure_smtp {
            return provider_error(
                "cleartext SMTP requires explicit allow_insecure_smtp operator policy",
            );
        }
        if self.username.is_some() != self.password.is_some() {
            return provider_error("SMTP username and password must be configured together");
        }
        validate_credential("SMTP username", self.username.as_deref())?;
        validate_credential("SMTP password", self.password.as_deref())?;
        if self.allowed_from.is_empty()
            || self.max_recipients == 0
            || self.max_recipients > 1_000
            || self.max_message_bytes == 0
            || self.max_message_bytes > 16 * 1024 * 1024
            || self.pool_size == 0
            || self.pool_size > MAX_POOL_SIZE
            || self.timeout.is_zero()
            || self.timeout > MAX_TIMEOUT
        {
            return provider_error(
                "SMTP sender, size, recipient, pool, or timeout policy is invalid",
            );
        }
        for sender in &self.allowed_from {
            parse_mailbox(sender, "allowed sender")?;
        }
        for domain in &self.allowed_recipient_domains {
            if domain.is_empty()
                || domain.len() > 253
                || domain.starts_with('.')
                || domain.ends_with('.')
                || !domain
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
            {
                return provider_error("SMTP recipient-domain policy is invalid");
            }
        }
        Ok(())
    }
}

struct Binding {
    config: Arc<EmailProviderConfig>,
    transport: SmtpTransport,
}

pub struct ProductionEmailProvider {
    bindings: Arc<BTreeMap<(String, String), Binding>>,
}

impl std::fmt::Debug for ProductionEmailProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionEmailProvider")
            .field("bindings", &self.bindings.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ProductionEmailProvider {
    pub fn new(configs: Vec<EmailProviderConfig>) -> Result<Self> {
        let mut bindings = BTreeMap::new();
        for config in configs {
            config.validate()?;
            let key = (config.application.clone(), config.provider.clone());
            if bindings.contains_key(&key) {
                return provider_error("duplicate application email provider binding");
            }
            let mut builder = SmtpTransport::from_url(config.endpoint.as_str())
                .map_err(|_| AppRuntimeError::Provider("SMTP client setup failed".to_string()))?
                .timeout(Some(config.timeout))
                .pool_config(PoolConfig::new().max_size(config.pool_size));
            if let (Some(username), Some(password)) = (&config.username, &config.password) {
                builder = builder.credentials(Credentials::new(username.clone(), password.clone()));
            }
            bindings.insert(
                key,
                Binding {
                    config: Arc::new(config),
                    transport: builder.build(),
                },
            );
        }
        Ok(Self {
            bindings: Arc::new(bindings),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        for binding in self.bindings.values() {
            if !matches!(binding.transport.test_connection(), Ok(true)) {
                return provider_error("SMTP healthcheck failed");
            }
        }
        Ok(())
    }

    fn binding(&self, application: &str, provider: &str) -> Result<&Binding> {
        self.bindings
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "email provider `{provider}` is unavailable for application `{application}`"
                ))
            })
    }
}

impl EmailProvider for ProductionEmailProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        let binding = self.binding(application, provider)?;
        if matches!(binding.transport.test_connection(), Ok(true)) {
            Ok(())
        } else {
            provider_error("email provider is unavailable")
        }
    }

    fn send(
        &self,
        application: &str,
        provider: &str,
        message: EmailMessage,
    ) -> Result<EmailDelivery> {
        let binding = self.binding(application, provider)?;
        let recipient_count = message
            .to
            .len()
            .saturating_add(message.cc.len())
            .saturating_add(message.bcc.len());
        let message_bytes = message
            .text
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(message.html.as_ref().map_or(0, String::len));
        if recipient_count == 0
            || recipient_count > binding.config.max_recipients as usize
            || message_bytes == 0
            || message_bytes > binding.config.max_message_bytes as usize
        {
            return provider_error("email delivery exceeds operator bounds");
        }
        let from = parse_mailbox(&message.from, "from")?;
        let sender = from.email.to_string().to_ascii_lowercase();
        if !binding
            .config
            .allowed_from
            .iter()
            .filter_map(|value| value.parse::<Mailbox>().ok())
            .any(|allowed| allowed.email.to_string().eq_ignore_ascii_case(&sender))
        {
            return provider_error("email sender is outside operator policy");
        }
        let to = parse_recipients(&message.to, "to", &binding.config)?;
        let cc = parse_recipients(&message.cc, "cc", &binding.config)?;
        let bcc = parse_recipients(&message.bcc, "bcc", &binding.config)?;
        let reply_to = message
            .reply_to
            .as_deref()
            .map(|value| parse_mailbox(value, "reply_to"))
            .transpose()?;

        let mut builder = Message::builder().from(from).subject(message.subject);
        for recipient in to {
            builder = builder.to(recipient);
        }
        for recipient in cc {
            builder = builder.cc(recipient);
        }
        for recipient in bcc {
            builder = builder.bcc(recipient);
        }
        if let Some(reply_to) = reply_to {
            builder = builder.reply_to(reply_to);
        }
        let message = match (message.text, message.html) {
            (Some(text), Some(html)) => builder
                .multipart(
                    MultiPart::alternative()
                        .singlepart(SinglePart::plain(text))
                        .singlepart(SinglePart::html(html)),
                )
                .map_err(|_| AppRuntimeError::Provider("email message is invalid".to_string()))?,
            (Some(text), None) => builder
                .singlepart(SinglePart::plain(text))
                .map_err(|_| AppRuntimeError::Provider("email message is invalid".to_string()))?,
            (None, Some(html)) => builder
                .singlepart(SinglePart::html(html))
                .map_err(|_| AppRuntimeError::Provider("email message is invalid".to_string()))?,
            (None, None) => return provider_error("email delivery requires text or HTML"),
        };
        if message.formatted().len() > binding.config.max_message_bytes as usize {
            return provider_error("encoded email exceeds operator bounds");
        }
        binding
            .transport
            .send(&message)
            .map_err(|_| AppRuntimeError::Provider("SMTP delivery failed".to_string()))?;
        Ok(EmailDelivery {
            accepted: true,
            delivery_id: Uuid::new_v4().to_string(),
            status: "sent".to_string(),
            transport: "smtp".to_string(),
        })
    }
}

fn parse_recipients(
    values: &[String],
    label: &str,
    config: &EmailProviderConfig,
) -> Result<Vec<Mailbox>> {
    values
        .iter()
        .map(|value| {
            let mailbox = parse_mailbox(value, label)?;
            if !config.allowed_recipient_domains.is_empty()
                && !config
                    .allowed_recipient_domains
                    .iter()
                    .any(|domain| mailbox.email.domain().eq_ignore_ascii_case(domain))
            {
                return provider_error("email recipient is outside operator policy");
            }
            Ok(mailbox)
        })
        .collect()
}

fn parse_mailbox(value: &str, _label: &str) -> Result<Mailbox> {
    value
        .parse::<Mailbox>()
        .map_err(|_| AppRuntimeError::Provider("email mailbox is invalid".to_string()))
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

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> EmailProviderConfig {
        EmailProviderConfig {
            application: "notifications".to_string(),
            provider: "default".to_string(),
            endpoint: Url::parse("smtp://127.0.0.1:2525").unwrap(),
            username: Some("smtp-user".to_string()),
            password: Some("smtp-secret".to_string()),
            allowed_from: BTreeSet::from(["Care <care@example.com>".to_string()]),
            allowed_recipient_domains: BTreeSet::from(["example.com".to_string()]),
            allow_insecure_smtp: true,
            max_recipients: 10,
            max_message_bytes: 1024 * 1024,
            pool_size: 4,
            timeout: Duration::from_secs(2),
        }
    }

    #[test]
    fn configuration_is_exact_and_debug_redacts_credentials() {
        let config = config();
        config.validate().unwrap();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("smtp-secret"));
        assert!(!rendered.contains("smtp-user"));

        let mut embedded = config.clone();
        embedded.endpoint = Url::parse("smtp://leak:embedded-secret@localhost:2525").unwrap();
        assert!(embedded.validate().is_err());
        let rendered = format!("{embedded:?}");
        assert!(!rendered.contains("embedded-secret"));
        assert!(!rendered.contains("leak"));

        let mut insecure = config;
        insecure.allow_insecure_smtp = false;
        assert!(insecure.validate().is_err());
    }

    #[test]
    fn sender_and_recipient_policy_fail_closed_before_transport() {
        let config = config();
        let from: Mailbox = "care@example.com".parse().unwrap();
        assert!(config
            .allowed_from
            .iter()
            .filter_map(|value| value.parse::<Mailbox>().ok())
            .any(|allowed| allowed.email == from.email));
        assert!(parse_recipients(&["ada@example.com".to_string()], "to", &config).is_ok());
        assert!(parse_recipients(&["ada@outside.test".to_string()], "to", &config).is_err());
    }
}
