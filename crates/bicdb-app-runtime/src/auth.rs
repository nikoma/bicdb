use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use bicdb_extension::abi_v2::{ActorContext, ApplicationAuthKindV1, ApplicationAuthSchemeV1};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use hmac::{Hmac, Mac};
use rsa::pkcs1v15::{Signature as RsaSignature, VerifyingKey as RsaVerifyingKey};
use rsa::traits::PublicKeyParts;
use rsa::{BigUint, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::{AppRuntimeError, Result};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtClaims {
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub service_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub acting_client_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub organization_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub assurance_level: Option<String>,
    pub iss: String,
    pub aud: JwtAudience,
    pub exp: i64,
    /// BicDB application-issued refresh tokens are never valid as bearer access tokens.
    /// External OIDC tokens may omit this BicDB application-specific discriminator.
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub nbf: Option<i64>,
    #[serde(default)]
    pub policy: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

impl JwtAudience {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(value) => value == expected,
            Self::Many(values) => values.iter().any(|value| value == expected),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JwtConfiguration {
    pub issuer: String,
    pub audience: String,
    pub authentication_method: String,
    pub maximum_lifetime_seconds: i64,
    pub clock_skew_seconds: i64,
}

#[derive(Clone)]
pub struct JwtAuthenticator {
    verifiers: Vec<JwtSchemeVerifier>,
}

#[derive(Clone)]
struct JwtSchemeVerifier {
    configuration: JwtConfiguration,
    verifier: JwtVerifier,
}

#[derive(Clone)]
enum JwtVerifier {
    Hs256 {
        keys: BTreeMap<String, Vec<u8>>,
        require_key_id: bool,
    },
    OidcEd25519(BTreeMap<String, VerifyingKey>),
    OidcRs256(BTreeMap<String, RsaVerifyingKey<Sha256>>),
}

impl JwtAuthenticator {
    pub fn hs256(configuration: JwtConfiguration, key: Vec<u8>) -> Result<Self> {
        Self::hs256_verifier(configuration, BTreeMap::from([(String::new(), key)]), false)
    }

    /// Creates an HS256 verifier with an explicit overlap set for safe key
    /// rotation. BicDB application-issued tokens carry the secret provider's `kid`, so
    /// an operator can admit the current and previous versions during the
    /// maximum token lifetime without making either key available to the app.
    pub fn hs256_rotating(
        configuration: JwtConfiguration,
        keys: impl IntoIterator<Item = (String, Vec<u8>)>,
    ) -> Result<Self> {
        let mut by_id = BTreeMap::new();
        for (key_id, key) in keys {
            if key_id.trim().is_empty() {
                return Err(AppRuntimeError::Authentication(
                    "rotating HS256 keys require non-empty key ids".to_string(),
                ));
            }
            if by_id.insert(key_id, key).is_some() {
                return Err(AppRuntimeError::Authentication(
                    "rotating HS256 keys contain a duplicate key id".to_string(),
                ));
            }
        }
        Self::hs256_verifier(configuration, by_id, true)
    }

    fn hs256_verifier(
        configuration: JwtConfiguration,
        keys: BTreeMap<String, Vec<u8>>,
        require_key_id: bool,
    ) -> Result<Self> {
        if keys.is_empty() || keys.values().any(|key| key.len() < 32) {
            return Err(AppRuntimeError::Authentication(
                "HS256 verifier requires one or more keys containing at least 32 bytes".to_string(),
            ));
        }
        if configuration.maximum_lifetime_seconds <= 0
            || configuration.clock_skew_seconds < 0
            || configuration.issuer.is_empty()
            || configuration.audience.is_empty()
        {
            return Err(AppRuntimeError::Authentication(
                "invalid JWT verifier configuration".to_string(),
            ));
        }
        Ok(Self {
            verifiers: vec![JwtSchemeVerifier {
                configuration,
                verifier: JwtVerifier::Hs256 {
                    keys,
                    require_key_id,
                },
            }],
        })
    }

    /// Creates an OIDC JWT verifier from an operator-fetched, pinned JWKS.
    ///
    /// BicDB intentionally does not let applications fetch discovery
    /// documents. The trusted host/operator refreshes JWKS and constructs a
    /// new authenticator atomically. This implementation accepts only OKP
    /// Ed25519/EdDSA keys, avoiding algorithm confusion and ambient egress.
    pub fn oidc_ed25519(configuration: JwtConfiguration, jwks: &[u8]) -> Result<Self> {
        if configuration.maximum_lifetime_seconds <= 0
            || configuration.clock_skew_seconds < 0
            || configuration.issuer.is_empty()
            || configuration.audience.is_empty()
        {
            return Err(AppRuntimeError::Authentication(
                "invalid OIDC verifier configuration".to_string(),
            ));
        }
        #[derive(Deserialize)]
        struct Jwks {
            keys: Vec<Jwk>,
        }
        #[derive(Deserialize)]
        struct Jwk {
            kid: String,
            kty: String,
            crv: String,
            x: String,
            #[serde(default)]
            alg: Option<String>,
            #[serde(default)]
            r#use: Option<String>,
        }
        let document: Jwks = serde_json::from_slice(jwks)?;
        let mut keys = BTreeMap::new();
        for key in document.keys {
            if key.kid.trim().is_empty()
                || key.kty != "OKP"
                || key.crv != "Ed25519"
                || key
                    .alg
                    .as_deref()
                    .is_some_and(|algorithm| algorithm != "EdDSA")
                || key.r#use.as_deref().is_some_and(|usage| usage != "sig")
            {
                return Err(AppRuntimeError::Authentication(
                    "OIDC JWKS contains an unsupported or ambiguous signing key".to_string(),
                ));
            }
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(key.x)
                .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
                AppRuntimeError::Authentication(
                    "OIDC Ed25519 public key must contain 32 bytes".to_string(),
                )
            })?;
            let verifying_key = VerifyingKey::from_bytes(&bytes)
                .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
            if keys.insert(key.kid, verifying_key).is_some() {
                return Err(AppRuntimeError::Authentication(
                    "OIDC JWKS contains duplicate key ids".to_string(),
                ));
            }
        }
        if keys.is_empty() {
            return Err(AppRuntimeError::Authentication(
                "OIDC JWKS contains no signing keys".to_string(),
            ));
        }
        Ok(Self {
            verifiers: vec![JwtSchemeVerifier {
                configuration,
                verifier: JwtVerifier::OidcEd25519(keys),
            }],
        })
    }

    /// Creates an OIDC RS256 verifier from an operator-fetched, pinned JWKS.
    /// Applications never fetch discovery metadata or key material themselves.
    pub fn oidc_rs256(configuration: JwtConfiguration, jwks: &[u8]) -> Result<Self> {
        if configuration.maximum_lifetime_seconds <= 0
            || configuration.clock_skew_seconds < 0
            || configuration.issuer.is_empty()
            || configuration.audience.is_empty()
        {
            return Err(AppRuntimeError::Authentication(
                "invalid OIDC verifier configuration".to_string(),
            ));
        }
        #[derive(Deserialize)]
        struct Jwks {
            keys: Vec<Jwk>,
        }
        #[derive(Deserialize)]
        struct Jwk {
            kid: String,
            kty: String,
            n: String,
            e: String,
            #[serde(default)]
            alg: Option<String>,
            #[serde(default)]
            r#use: Option<String>,
        }
        let document: Jwks = serde_json::from_slice(jwks)?;
        let mut keys = BTreeMap::new();
        for key in document.keys {
            if key.kid.trim().is_empty()
                || key.kty != "RSA"
                || key
                    .alg
                    .as_deref()
                    .is_some_and(|algorithm| algorithm != "RS256")
                || key.r#use.as_deref().is_some_and(|usage| usage != "sig")
            {
                return Err(AppRuntimeError::Authentication(
                    "OIDC JWKS contains an unsupported or ambiguous signing key".to_string(),
                ));
            }
            let public_key = RsaPublicKey::new(
                BigUint::from_bytes_be(&decode_url(&key.n)?),
                BigUint::from_bytes_be(&decode_url(&key.e)?),
            )
            .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
            if public_key.n().bits() < 2_048 {
                return Err(AppRuntimeError::Authentication(
                    "OIDC RSA public key must contain at least 2048 modulus bits".to_string(),
                ));
            }
            if keys
                .insert(key.kid, RsaVerifyingKey::<Sha256>::new(public_key))
                .is_some()
            {
                return Err(AppRuntimeError::Authentication(
                    "OIDC JWKS contains duplicate key ids".to_string(),
                ));
            }
        }
        if keys.is_empty() {
            return Err(AppRuntimeError::Authentication(
                "OIDC JWKS contains no signing keys".to_string(),
            ));
        }
        Ok(Self {
            verifiers: vec![JwtSchemeVerifier {
                configuration,
                verifier: JwtVerifier::OidcRs256(keys),
            }],
        })
    }

    /// Builds an operator-owned verifier registry. Duplicate signed contracts
    /// are rejected so route selection can never be ambiguous.
    pub fn multiple(authenticators: Vec<Self>) -> Result<Self> {
        let verifiers = authenticators
            .into_iter()
            .flat_map(|authenticator| authenticator.verifiers)
            .collect::<Vec<_>>();
        if verifiers.is_empty() {
            return Err(AppRuntimeError::Authentication(
                "authentication verifier registry is empty".to_string(),
            ));
        }
        let mut contracts = BTreeSet::new();
        for verifier in &verifiers {
            let contract = (
                verifier.kind(),
                verifier.configuration.issuer.clone(),
                verifier.configuration.audience.clone(),
            );
            if !contracts.insert(contract) {
                return Err(AppRuntimeError::Authentication(
                    "authentication verifier registry contains a duplicate signed contract"
                        .to_string(),
                ));
            }
        }
        Ok(Self { verifiers })
    }

    pub fn authenticate(
        &self,
        bearer_token: &str,
        trace_id: String,
        correlation_id: Option<String>,
        request_origin: Option<String>,
        request_deadline_unix_ms: i64,
    ) -> Result<ActorContext> {
        self.authenticate_candidates(
            self.verifiers.iter(),
            bearer_token,
            trace_id,
            correlation_id,
            request_origin,
            request_deadline_unix_ms,
        )
    }

    /// Authenticates only with the operator verifier matching the route's
    /// signed kind, issuer, and audience contract.
    pub fn authenticate_for(
        &self,
        contract: &ApplicationAuthSchemeV1,
        bearer_token: &str,
        trace_id: String,
        correlation_id: Option<String>,
        request_origin: Option<String>,
        request_deadline_unix_ms: i64,
    ) -> Result<ActorContext> {
        let candidates = self
            .verifiers
            .iter()
            .filter(|verifier| verifier.matches(contract));
        self.authenticate_candidates(
            candidates,
            bearer_token,
            trace_id,
            correlation_id,
            request_origin,
            request_deadline_unix_ms,
        )
    }

    /// Preserves public-route behavior for supplied bearer tokens while
    /// limiting verification to contracts declared by that application.
    pub fn authenticate_for_any<'a>(
        &self,
        contracts: impl IntoIterator<Item = &'a ApplicationAuthSchemeV1>,
        bearer_token: &str,
        trace_id: String,
        correlation_id: Option<String>,
        request_origin: Option<String>,
        request_deadline_unix_ms: i64,
    ) -> Result<ActorContext> {
        let contracts = contracts.into_iter().collect::<Vec<_>>();
        let candidates = self
            .verifiers
            .iter()
            .filter(|verifier| contracts.iter().any(|contract| verifier.matches(contract)));
        self.authenticate_candidates(
            candidates,
            bearer_token,
            trace_id,
            correlation_id,
            request_origin,
            request_deadline_unix_ms,
        )
    }

    fn authenticate_candidates<'a>(
        &self,
        candidates: impl IntoIterator<Item = &'a JwtSchemeVerifier>,
        bearer_token: &str,
        trace_id: String,
        correlation_id: Option<String>,
        request_origin: Option<String>,
        request_deadline_unix_ms: i64,
    ) -> Result<ActorContext> {
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(AppRuntimeError::Authentication(
                "no operator verifier matches the route authentication contract".to_string(),
            ));
        }
        if candidates.len() == 1 {
            return candidates[0].authenticate(
                bearer_token,
                trace_id,
                correlation_id,
                request_origin,
                request_deadline_unix_ms,
            );
        }
        let mut actor = None;
        for candidate in candidates {
            if let Ok(candidate_actor) = candidate.authenticate(
                bearer_token,
                trace_id.clone(),
                correlation_id.clone(),
                request_origin.clone(),
                request_deadline_unix_ms,
            ) {
                if actor.is_some() {
                    return Err(AppRuntimeError::Authentication(
                        "bearer token matches multiple authentication contracts".to_string(),
                    ));
                }
                actor = Some(candidate_actor);
            }
        }
        actor.ok_or_else(|| {
            AppRuntimeError::Authentication(
                "bearer token does not match an allowed authentication contract".to_string(),
            )
        })
    }
}

impl JwtSchemeVerifier {
    fn kind(&self) -> ApplicationAuthKindV1 {
        match &self.verifier {
            JwtVerifier::Hs256 { .. } => ApplicationAuthKindV1::JwtHs256,
            JwtVerifier::OidcEd25519(_) => ApplicationAuthKindV1::OidcEd25519,
            JwtVerifier::OidcRs256(_) => ApplicationAuthKindV1::OidcRs256,
        }
    }

    fn matches(&self, contract: &ApplicationAuthSchemeV1) -> bool {
        self.kind() == contract.kind
            && self.configuration.issuer == contract.issuer
            && self.configuration.audience == contract.audience
    }

    fn authenticate(
        &self,
        bearer_token: &str,
        trace_id: String,
        correlation_id: Option<String>,
        request_origin: Option<String>,
        request_deadline_unix_ms: i64,
    ) -> Result<ActorContext> {
        let token = bearer_token.strip_prefix("Bearer ").unwrap_or(bearer_token);
        let mut parts = token.split('.');
        let (Some(header), Some(payload), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(AppRuntimeError::Authentication(
                "JWT must contain three segments".to_string(),
            ));
        };
        let header_bytes = decode_url(header)?;
        let header_json: serde_json::Value = serde_json::from_slice(&header_bytes)?;
        if header_json
            .get("typ")
            .and_then(|value| value.as_str())
            .is_some_and(|kind| kind != "JWT")
        {
            return Err(AppRuntimeError::Authentication(
                "JWT type is not allowed".to_string(),
            ));
        }
        let signature = decode_url(signature)?;
        let signing_input = format!("{header}.{payload}");
        match &self.verifier {
            JwtVerifier::Hs256 {
                keys,
                require_key_id,
            } => {
                if header_json.get("alg").and_then(|value| value.as_str()) != Some("HS256") {
                    return Err(AppRuntimeError::Authentication(
                        "JWT algorithm is not allowed".to_string(),
                    ));
                }
                let key_id = header_json.get("kid").and_then(|value| value.as_str());
                let key = if *require_key_id {
                    key_id.and_then(|key_id| keys.get(key_id)).ok_or_else(|| {
                        AppRuntimeError::Authentication(
                            "JWT references an unknown or absent HS256 key id".to_string(),
                        )
                    })?
                } else {
                    keys.values().next().expect("HS256 verifier has a key")
                };
                let mut mac = Hmac::<Sha256>::new_from_slice(key)
                    .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
                mac.update(signing_input.as_bytes());
                mac.verify_slice(&signature).map_err(|_| {
                    AppRuntimeError::Authentication("JWT signature is invalid".to_string())
                })?;
            }
            JwtVerifier::OidcEd25519(keys) => {
                if header_json.get("alg").and_then(|value| value.as_str()) != Some("EdDSA") {
                    return Err(AppRuntimeError::Authentication(
                        "OIDC JWT algorithm is not EdDSA".to_string(),
                    ));
                }
                let key_id = header_json
                    .get("kid")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        AppRuntimeError::Authentication("OIDC JWT is missing a key id".to_string())
                    })?;
                let key = keys.get(key_id).ok_or_else(|| {
                    AppRuntimeError::Authentication(
                        "OIDC JWT references an unknown key id".to_string(),
                    )
                })?;
                let signature = Signature::from_slice(&signature)
                    .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
                // Strict: reject small-order keys and non-canonical
                // signature encodings. A malleable JWT signature is a second
                // valid representation of an accepted token.
                key.verify_strict(signing_input.as_bytes(), &signature)
                    .map_err(|_| {
                        AppRuntimeError::Authentication("JWT signature is invalid".to_string())
                    })?;
            }
            JwtVerifier::OidcRs256(keys) => {
                if header_json.get("alg").and_then(|value| value.as_str()) != Some("RS256") {
                    return Err(AppRuntimeError::Authentication(
                        "OIDC JWT algorithm is not RS256".to_string(),
                    ));
                }
                let key_id = header_json
                    .get("kid")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        AppRuntimeError::Authentication("OIDC JWT is missing a key id".to_string())
                    })?;
                let key = keys.get(key_id).ok_or_else(|| {
                    AppRuntimeError::Authentication(
                        "OIDC JWT references an unknown key id".to_string(),
                    )
                })?;
                let signature = RsaSignature::try_from(signature.as_slice())
                    .map_err(|error| AppRuntimeError::Authentication(error.to_string()))?;
                key.verify(signing_input.as_bytes(), &signature)
                    .map_err(|_| {
                        AppRuntimeError::Authentication("JWT signature is invalid".to_string())
                    })?;
            }
        }
        let claims: JwtClaims = serde_json::from_slice(&decode_url(payload)?)?;
        let now_seconds = now_ms() / 1000;
        if claims.iss != self.configuration.issuer
            || !claims.aud.contains(&self.configuration.audience)
            || claims.exp < now_seconds - self.configuration.clock_skew_seconds
            || claims
                .nbf
                .is_some_and(|nbf| nbf > now_seconds + self.configuration.clock_skew_seconds)
            || claims.exp.saturating_sub(now_seconds) > self.configuration.maximum_lifetime_seconds
            || claims.sub.trim().is_empty()
            || claims
                .token_type
                .as_deref()
                .is_some_and(|token_type| token_type != "access")
        {
            return Err(AppRuntimeError::Authentication(
                "JWT claims violate issuer, audience, time, or subject policy".to_string(),
            ));
        }
        let token_deadline = claims.exp.saturating_mul(1000);
        let deadline_unix_ms = request_deadline_unix_ms.min(token_deadline);
        let mut policy_attributes = claims.policy;
        for (key, value) in [("email", claims.email), ("name", claims.name)] {
            let Some(value) = value.filter(|value| !value.is_empty()) else {
                continue;
            };
            if policy_attributes
                .get(key)
                .is_some_and(|existing| existing != &value)
            {
                return Err(AppRuntimeError::Authentication(format!(
                    "JWT `{key}` claim conflicts with its policy attribute"
                )));
            }
            policy_attributes.insert(key.to_string(), value);
        }
        let actor = ActorContext {
            user_id: Some(claims.sub),
            service_id: claims.service_id,
            client_id: claims.client_id,
            acting_client_id: claims.acting_client_id,
            authentication_method: Some(self.configuration.authentication_method.clone()),
            roles: claims.roles,
            scopes: claims.scopes,
            tenant_id: claims.tenant_id,
            workspace_id: claims.workspace_id,
            organization_id: claims.organization_id,
            session_id: claims.session_id,
            delegation_chain: vec![],
            assurance_level: claims.assurance_level,
            request_origin,
            trace_id,
            correlation_id,
            causation_id: None,
            deadline_unix_ms,
            policy_attributes,
        };
        actor.validate()?;
        Ok(actor)
    }
}

fn decode_url(value: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|error| AppRuntimeError::Authentication(error.to_string()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signer, SigningKey};
    use rand_core::OsRng;
    use rsa::pkcs1v15::SigningKey as RsaSigningKey;
    use rsa::signature::SignatureEncoding;
    use rsa::RsaPrivateKey;
    use serde_json::{json, Value};

    use super::*;

    fn configuration() -> JwtConfiguration {
        JwtConfiguration {
            issuer: "https://issuer.example".to_string(),
            audience: "carrier".to_string(),
            authentication_method: "oidc".to_string(),
            maximum_lifetime_seconds: 3_600,
            clock_skew_seconds: 30,
        }
    }

    fn payload() -> Value {
        json!({
            "sub": "user-1",
            "email": "user@example.com",
            "name": "BicDB application User",
            "tenant_id": "tenant-a",
            "workspace_id": "workspace-a",
            "roles": ["editor"],
            "scopes": ["documents:write"],
            "iss": "https://issuer.example",
            "aud": ["carrier", "other"],
            "exp": now_ms() / 1000 + 300
        })
    }

    fn encoded(value: &Value) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap())
    }

    #[test]
    fn oidc_ed25519_jwks_verifies_and_constructs_trusted_actor() {
        let signing = SigningKey::from_bytes(&[11; 32]);
        let jwks = serde_json::to_vec(&json!({
            "keys": [{
                "kid": "current",
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "use": "sig",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(signing.verifying_key().as_bytes())
            }]
        }))
        .unwrap();
        let authenticator = JwtAuthenticator::oidc_ed25519(configuration(), &jwks).unwrap();
        let header = encoded(&json!({"alg":"EdDSA","typ":"JWT","kid":"current"}));
        let payload = encoded(&payload());
        let signature = signing.sign(format!("{header}.{payload}").as_bytes());
        let token = format!(
            "{header}.{payload}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes())
        );
        let actor = authenticator
            .authenticate(
                &format!("Bearer {token}"),
                "trace-1".to_string(),
                Some("correlation-1".to_string()),
                Some("https://app.example".to_string()),
                now_ms() + 60_000,
            )
            .unwrap();
        assert_eq!(actor.user_id.as_deref(), Some("user-1"));
        assert_eq!(actor.tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(actor.workspace_id.as_deref(), Some("workspace-a"));
        assert!(actor.roles.contains("editor"));
        assert_eq!(
            actor.policy_attributes.get("email").map(String::as_str),
            Some("user@example.com")
        );
        assert_eq!(
            actor.policy_attributes.get("name").map(String::as_str),
            Some("BicDB application User")
        );
    }

    #[test]
    fn oidc_rs256_jwks_verifies_and_rejects_algorithm_confusion() {
        let private_key = RsaPrivateKey::new(&mut OsRng, 2_048).unwrap();
        let public_key = private_key.to_public_key();
        let jwks = serde_json::to_vec(&json!({
            "keys": [{
                "kid": "current-rsa",
                "kty": "RSA",
                "alg": "RS256",
                "use": "sig",
                "n": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(public_key.n().to_bytes_be()),
                "e": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(public_key.e().to_bytes_be())
            }]
        }))
        .unwrap();
        let authenticator = JwtAuthenticator::oidc_rs256(configuration(), &jwks).unwrap();
        let header = encoded(&json!({"alg":"RS256","typ":"JWT","kid":"current-rsa"}));
        let payload = encoded(&payload());
        let signature = RsaSigningKey::<Sha256>::new(private_key)
            .sign(format!("{header}.{payload}").as_bytes());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes());
        let token = format!("{header}.{payload}.{signature}");
        let actor = authenticator
            .authenticate(
                &token,
                "trace-rs256".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .unwrap();
        assert_eq!(actor.user_id.as_deref(), Some("user-1"));

        let confused_header = encoded(&json!({"alg":"HS256","typ":"JWT","kid":"current-rsa"}));
        let confused = format!("{confused_header}.{payload}.{signature}");
        assert!(authenticator
            .authenticate(
                &confused,
                "trace-confused".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .is_err());
    }

    #[test]
    fn oidc_rejects_unknown_keys_and_algorithm_confusion() {
        let signing = SigningKey::from_bytes(&[12; 32]);
        let jwks = serde_json::to_vec(&json!({
            "keys": [{
                "kid": "current",
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "use": "sig",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(signing.verifying_key().as_bytes())
            }]
        }))
        .unwrap();
        let authenticator = JwtAuthenticator::oidc_ed25519(configuration(), &jwks).unwrap();
        let header = encoded(&json!({"alg":"HS256","typ":"JWT","kid":"missing"}));
        let payload = encoded(&payload());
        let token = format!("{header}.{payload}.{}", encoded(&json!("not-a-signature")));
        assert!(authenticator
            .authenticate(&token, "trace-1".to_string(), None, None, now_ms() + 60_000,)
            .is_err());
    }

    #[test]
    fn signed_actor_context_rejects_a_client_modified_tenant_claim() {
        let key = vec![23_u8; 32];
        let authenticator = JwtAuthenticator::hs256(configuration(), key.clone()).unwrap();
        let header = encoded(&json!({"alg":"HS256","typ":"JWT"}));
        let tenant_a_payload = encoded(&payload());
        let signing_input = format!("{header}.{tenant_a_payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
        mac.update(signing_input.as_bytes());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());

        let mut forged = payload();
        forged["tenant_id"] = json!("tenant-b");
        let forged_token = format!("{header}.{}.{signature}", encoded(&forged));
        let error = authenticator
            .authenticate(
                &forged_token,
                "trace-forged".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .unwrap_err();
        assert!(error.to_string().contains("JWT signature is invalid"));
    }

    fn hs256_token(key: &[u8], issuer: &str, audience: &str) -> String {
        hs256_token_with_header(key, issuer, audience, json!({"alg":"HS256","typ":"JWT"}))
    }

    fn hs256_token_with_header(key: &[u8], issuer: &str, audience: &str, header: Value) -> String {
        let header = encoded(&header);
        let mut claims = payload();
        claims["iss"] = json!(issuer);
        claims["aud"] = json!(audience);
        let payload = encoded(&claims);
        let signing_input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(signing_input.as_bytes());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing_input}.{signature}")
    }

    #[test]
    fn rotating_hs256_verifier_accepts_overlap_and_rejects_unknown_key_ids() {
        let previous_key = vec![29_u8; 32];
        let current_key = vec![30_u8; 32];
        let authenticator = JwtAuthenticator::hs256_rotating(
            configuration(),
            [
                ("auth-v1".to_string(), previous_key.clone()),
                ("auth-v2".to_string(), current_key.clone()),
            ],
        )
        .unwrap();
        let previous = hs256_token_with_header(
            &previous_key,
            "https://issuer.example",
            "carrier",
            json!({"alg":"HS256","typ":"JWT","kid":"auth-v1"}),
        );
        let current = hs256_token_with_header(
            &current_key,
            "https://issuer.example",
            "carrier",
            json!({"alg":"HS256","typ":"JWT","kid":"auth-v2"}),
        );
        let unknown = hs256_token_with_header(
            &current_key,
            "https://issuer.example",
            "carrier",
            json!({"alg":"HS256","typ":"JWT","kid":"missing"}),
        );

        for token in [previous, current] {
            assert!(authenticator
                .authenticate(
                    &token,
                    "trace-rotation".to_string(),
                    None,
                    None,
                    now_ms() + 60_000,
                )
                .is_ok());
        }
        assert!(authenticator
            .authenticate(
                &unknown,
                "trace-unknown".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .is_err());
    }

    #[test]
    fn signed_route_contract_selects_one_operator_verifier() {
        let first_key = vec![31_u8; 32];
        let second_key = vec![32_u8; 32];
        let first = JwtAuthenticator::hs256(
            JwtConfiguration {
                issuer: "issuer-a".to_string(),
                audience: "audience-a".to_string(),
                authentication_method: "jwt-hs256".to_string(),
                maximum_lifetime_seconds: 3_600,
                clock_skew_seconds: 30,
            },
            first_key.clone(),
        )
        .unwrap();
        let second = JwtAuthenticator::hs256(
            JwtConfiguration {
                issuer: "issuer-b".to_string(),
                audience: "audience-b".to_string(),
                authentication_method: "jwt-hs256".to_string(),
                maximum_lifetime_seconds: 3_600,
                clock_skew_seconds: 30,
            },
            second_key,
        )
        .unwrap();
        let registry = JwtAuthenticator::multiple(vec![first, second]).unwrap();
        let first_contract = ApplicationAuthSchemeV1 {
            kind: ApplicationAuthKindV1::JwtHs256,
            issuer: "issuer-a".to_string(),
            audience: "audience-a".to_string(),
        };
        let second_contract = ApplicationAuthSchemeV1 {
            kind: ApplicationAuthKindV1::JwtHs256,
            issuer: "issuer-b".to_string(),
            audience: "audience-b".to_string(),
        };
        let token = hs256_token(&first_key, "issuer-a", "audience-a");

        assert!(registry
            .authenticate_for(
                &first_contract,
                &token,
                "trace-a".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .is_ok());
        assert!(registry
            .authenticate_for(
                &second_contract,
                &token,
                "trace-b".to_string(),
                None,
                None,
                now_ms() + 60_000,
            )
            .is_err());
    }
}
