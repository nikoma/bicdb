use std::collections::BTreeMap;

use base64::Engine;
use bicdb_extension::{ExtensionManifest, EXTENSION_ABI_V2};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AppRuntimeError, Result};

pub(crate) const MAX_FRONTEND_ASSETS: usize = 4_096;
pub(crate) const MAX_FRONTEND_ASSET_PATH_BYTES: usize = 1_024;
pub(crate) const MAX_APPLICATION_COMPONENTS: usize = 128;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionScope {
    Global,
    Cell,
    Device,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationDataClass {
    Public,
    Operational,
    Sensitive,
    #[serde(alias = "phi")]
    Regulated,
    #[serde(alias = "protected_data_local")]
    RegulatedLocal,
}

impl ApplicationDataClass {
    pub fn is_regulated(self) -> bool {
        matches!(self, Self::Regulated | Self::RegulatedLocal)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationComponentKind {
    Backend,
    Frontend,
    Migration,
    Worker,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationCapability {
    Database,
    HttpRoutes,
    FrontendAssets,
    Jobs,
    Secrets,
    Blobs,
    Egress,
    RawSql,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationDatabaseFeature {
    RowLevelSecurity,
    StoredFunctions,
    Triggers,
    Jobs,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationComponent {
    pub name: String,
    pub kind: ApplicationComponentKind,
    pub scope: ApplicationExecutionScope,
    pub data_class: ApplicationDataClass,
    #[serde(default)]
    pub capabilities: std::collections::BTreeSet<ApplicationCapability>,
    #[serde(default)]
    pub egress: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub database_features: std::collections::BTreeSet<ApplicationDatabaseFeature>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrontendAsset {
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationPackage {
    pub manifest: ExtensionManifest,
    /// Module name to raw WebAssembly bytes. The module matching the extension
    /// identity is the package entry point.
    #[serde(deserialize_with = "crate::package_input::deserialize_modules")]
    pub modules: BTreeMap<String, Vec<u8>>,
    /// Same-origin frontend files. Exact paths, media types, and bytes are
    /// covered by the package signature; the host never falls back to an
    /// ambient filesystem or CDN.
    #[serde(
        default,
        deserialize_with = "crate::package_input::deserialize_frontend_assets"
    )]
    pub frontend_assets: BTreeMap<String, FrontendAsset>,
    /// Signed deployment contract. General packages may omit this for ABI-v2
    /// compatibility; the hardened cell construction path requires it.
    #[serde(default)]
    pub components: Vec<ApplicationComponent>,
    pub dependency_lock: Vec<u8>,
    pub sbom: Vec<u8>,
    pub provenance: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DependencyLockV1 {
    #[serde(default, alias = "version")]
    pub format: u32,
    #[serde(default)]
    pub packages: Vec<LockedDependency>,
    #[serde(default, alias = "erp_modules")]
    pub application_modules: Option<ApplicationModuleLockV1>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LockedDependency {
    pub name: String,
    pub version: String,
    pub package_sha256: String,
    #[serde(default)]
    pub module_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationModuleRequirementV1 {
    pub namespace: String,
    pub version: String,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationModuleLockV1 {
    pub namespace: String,
    pub version: String,
    pub system: bool,
    #[serde(default)]
    pub requires: Vec<ApplicationModuleRequirementV1>,
    pub install_order: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationRecordTypeContractV1 {
    pub name: String,
    pub namespace: String,
    pub qualified_name: String,
    pub stable_key: String,
    pub owner: String,
    pub schema_version: u32,
    pub scope_level: String,
    pub model: String,
    #[serde(default)]
    pub traits: Vec<String>,
    #[serde(default)]
    pub links: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationModuleContractV1 {
    pub contract_version: u32,
    pub namespace: String,
    pub semantic_version: String,
    pub system: bool,
    #[serde(default)]
    pub requires: Vec<ApplicationModuleRequirementV1>,
    pub install_order: Vec<String>,
    #[serde(default, alias = "doctypes")]
    pub record_types: Vec<ApplicationRecordTypeContractV1>,
    #[serde(default)]
    pub traits: Vec<serde_json::Value>,
    #[serde(default)]
    pub extensions: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Default)]
pub struct TrustedSigningKeys {
    keys: BTreeMap<String, VerifyingKey>,
}

impl TrustedSigningKeys {
    pub fn insert_ed25519(&mut self, key_id: impl Into<String>, public_key: &[u8]) -> Result<()> {
        let key: [u8; 32] = public_key.try_into().map_err(|_| {
            AppRuntimeError::Signature("Ed25519 public key must contain 32 bytes".to_string())
        })?;
        let key = VerifyingKey::from_bytes(&key)
            .map_err(|error| AppRuntimeError::Signature(error.to_string()))?;
        self.keys.insert(key_id.into(), key);
        Ok(())
    }

    fn get(&self, key_id: &str) -> Option<&VerifyingKey> {
        self.keys.get(key_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageVerification {
    pub package_sha256: String,
    pub module_sha256: BTreeMap<String, String>,
    pub frontend_sha256: BTreeMap<String, String>,
    pub signing_key_id: String,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct PackageSigningEnvelope<'a> {
    format: &'static str,
    manifest: ExtensionManifest,
    module_sha256: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    frontend_assets: &'a BTreeMap<String, FrontendAssetSignature>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    components: &'a Vec<ApplicationComponent>,
    dependency_lock_sha256: String,
    sbom_sha256: String,
    provenance_sha256: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
struct FrontendAssetSignature {
    content_type: String,
    sha256: String,
}

#[derive(Clone, Debug)]
pub struct PackageVerifier {
    trusted_keys: TrustedSigningKeys,
    max_package_bytes: usize,
}

impl PackageVerifier {
    pub fn new(trusted_keys: TrustedSigningKeys, max_package_bytes: usize) -> Result<Self> {
        crate::encoded_package_byte_limit(max_package_bytes)?;
        Ok(Self {
            trusted_keys,
            max_package_bytes,
        })
    }

    pub fn max_package_bytes(&self) -> usize {
        self.max_package_bytes
    }

    pub(crate) fn restrict_package_bytes(mut self, limit: usize) -> Self {
        self.max_package_bytes = self.max_package_bytes.min(limit);
        self
    }

    pub fn verify(&self, package: &ApplicationPackage) -> Result<PackageVerification> {
        package.check_decoded_size(self.max_package_bytes)?;
        package.manifest.validate()?;
        if package.manifest.identity.abi_version != EXTENSION_ABI_V2 {
            return Err(AppRuntimeError::InvalidPackage(
                "application packages require extension ABI v2".to_string(),
            ));
        }
        let application = package.manifest.application.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage("missing ABI v2 application manifest".to_string())
        })?;
        if !package
            .modules
            .contains_key(&package.manifest.identity.name)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "package lacks entry module `{}`",
                package.manifest.identity.name
            )));
        }
        validate_components(package)?;
        validate_frontend_assets(package)?;
        let module_sha256 = package
            .modules
            .iter()
            .map(|(name, bytes)| (name.clone(), sha256(bytes)))
            .collect::<BTreeMap<_, _>>();
        let frontend_sha256 = package
            .frontend_assets
            .iter()
            .map(|(path, asset)| (path.clone(), sha256(&asset.bytes)))
            .collect::<BTreeMap<_, _>>();
        let dependency_lock_sha256 = sha256(&package.dependency_lock);
        let sbom_sha256 = sha256(&package.sbom);
        let provenance_sha256 = sha256(&package.provenance);
        if dependency_lock_sha256 != application.package.dependency_lock_sha256
            || sbom_sha256 != application.package.sbom_sha256
            || provenance_sha256 != application.package.provenance_sha256
        {
            return Err(AppRuntimeError::InvalidPackage(
                "dependency lock, SBOM, or provenance hash differs from the manifest".to_string(),
            ));
        }
        validate_supply_chain_documents(package)?;
        if application.package.signature_algorithm != "ed25519" {
            return Err(AppRuntimeError::Signature(format!(
                "installer supports only Ed25519 package signatures, not `{}`",
                application.package.signature_algorithm
            )));
        }
        let payload = canonical_signing_payload(package)?;
        let mut package_sha256 = sha256(&payload);
        if package_sha256 != application.package.package_sha256 {
            let carrier_legacy_sha256 =
                canonical_signing_payload_without_empty_invariants(package)?
                    .map(|payload| sha256(&payload));
            if carrier_legacy_sha256.as_deref() == Some(application.package.package_sha256.as_str())
            {
                package_sha256 = carrier_legacy_sha256.expect("matched legacy hash exists");
            } else {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "package hash mismatch: manifest={}, actual={package_sha256}",
                    application.package.package_sha256
                )));
            }
        }
        let key = self
            .trusted_keys
            .get(&application.package.signature_key_id)
            .ok_or_else(|| {
                AppRuntimeError::Signature(format!(
                    "signing key `{}` is not trusted",
                    application.package.signature_key_id
                ))
            })?;
        let signature_bytes = base64::engine::general_purpose::STANDARD
            .decode(&application.package.signature)
            .map_err(|error| AppRuntimeError::Signature(error.to_string()))?;
        let signature = Signature::from_slice(&signature_bytes)
            .map_err(|error| AppRuntimeError::Signature(error.to_string()))?;
        // Strict: a package's identity is its content hash, but a
        // malleable signature still lets the same package present two
        // distinct signature bytes to anything keyed on them.
        key.verify_strict(package_sha256.as_bytes(), &signature)
            .map_err(|error| AppRuntimeError::Signature(error.to_string()))?;
        Ok(PackageVerification {
            package_sha256,
            module_sha256,
            frontend_sha256,
            signing_key_id: application.package.signature_key_id.clone(),
        })
    }
}

fn validate_components(package: &ApplicationPackage) -> Result<()> {
    if package.components.len() > MAX_APPLICATION_COMPONENTS {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "application package exceeds {MAX_APPLICATION_COMPONENTS} component contracts"
        )));
    }
    let mut names = std::collections::BTreeSet::new();
    for component in &package.components {
        if component.name.trim().is_empty()
            || component.name.len() > 128
            || !component
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            || !names.insert(component.name.to_ascii_lowercase())
            || component.egress.len() > 128
            || component
                .egress
                .iter()
                .any(|name| name.trim().is_empty() || name.len() > 2_048)
        {
            return Err(AppRuntimeError::InvalidPackage(
                "application component names and egress declarations must be unique, bounded, and non-empty"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_frontend_assets(package: &ApplicationPackage) -> Result<()> {
    if package.frontend_assets.len() > MAX_FRONTEND_ASSETS {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "application package exceeds {MAX_FRONTEND_ASSETS} frontend assets"
        )));
    }
    const MEDIA_TYPES: &[&str] = &[
        "text/html; charset=utf-8",
        "text/css; charset=utf-8",
        "application/javascript; charset=utf-8",
        "application/json; charset=utf-8",
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/x-icon",
        "font/woff2",
    ];
    for (path, asset) in &package.frontend_assets {
        let path = std::path::Path::new(path);
        if path.as_os_str().is_empty()
            || path.as_os_str().len() > MAX_FRONTEND_ASSET_PATH_BYTES
            || path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
            || asset.bytes.is_empty()
            || !MEDIA_TYPES.contains(&asset.content_type.as_str())
        {
            return Err(AppRuntimeError::InvalidPackage(
                "frontend assets require a safe relative path, an allowed exact media type, and non-empty bytes"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn dependency_lock(package: &ApplicationPackage) -> Result<DependencyLockV1> {
    let lock: DependencyLockV1 =
        serde_json::from_slice(&package.dependency_lock).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!("dependency lock is invalid: {error}"))
        })?;
    if lock.format != 1 {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "dependency lock format {} is unsupported",
            lock.format
        )));
    }
    Ok(lock)
}

pub(crate) fn application_module_contract(
    package: &ApplicationPackage,
) -> Result<Option<ApplicationModuleContractV1>> {
    let provenance: serde_json::Value =
        serde_json::from_slice(&package.provenance).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!("provenance is not valid JSON: {error}"))
        })?;
    let Some(contract) = provenance
        .get("application_module_contract")
        .or_else(|| provenance.get("erp_module_contract"))
    else {
        return Ok(None);
    };
    if contract.is_null() {
        return Ok(None);
    }
    serde_json::from_value(contract.clone())
        .map(Some)
        .map_err(|error| {
            AppRuntimeError::InvalidPackage(format!(
                "application module contract is invalid: {error}"
            ))
        })
}

fn valid_application_namespace(value: &str) -> bool {
    let mut segments = value.split('.');
    let valid_segment = |segment: &str| {
        !segment.is_empty()
            && segment
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_lowercase())
            && segment.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
    };
    let first = segments.next();
    let second = segments.next();
    first.is_some_and(valid_segment)
        && second.is_some_and(valid_segment)
        && segments.all(valid_segment)
}

fn validate_application_module_contract(
    package: &ApplicationPackage,
    lock: &DependencyLockV1,
) -> Result<()> {
    let contract = application_module_contract(package)?;
    match (&lock.application_modules, &contract) {
        (None, None) => return Ok(()),
        (Some(_), None) | (None, Some(_)) => {
            return Err(AppRuntimeError::InvalidPackage(
                "application module contract must be present in both signed dependency lock and provenance"
                    .to_string(),
            ));
        }
        (Some(_), Some(_)) => {}
    }
    let locked = lock.application_modules.as_ref().expect("checked above");
    let contract = contract.as_ref().expect("checked above");
    if contract.contract_version != 1
        || locked.namespace != contract.namespace
        || locked.version != contract.semantic_version
        || locked.system != contract.system
        || locked.requires != contract.requires
        || locked.install_order != contract.install_order
        || contract.semantic_version != package.manifest.identity.version
        || !valid_application_namespace(&contract.namespace)
        || semver::Version::parse(&contract.semantic_version).is_err()
        || contract.install_order.last() != Some(&contract.namespace)
    {
        return Err(AppRuntimeError::InvalidPackage(
            "signed application module identity, version, or installation order is inconsistent"
                .to_string(),
        ));
    }

    let mut requirements = std::collections::BTreeSet::new();
    for requirement in &contract.requires {
        if !valid_application_namespace(&requirement.namespace)
            || requirement.namespace == contract.namespace
            || semver::VersionReq::parse(&requirement.version).is_err()
            || !requirements.insert(requirement.namespace.clone())
            || !contract.install_order.contains(&requirement.namespace)
        {
            return Err(AppRuntimeError::InvalidPackage(
                "application module requirements must be unique, compatible constraints in the signed installation order"
                    .to_string(),
            ));
        }
    }
    let mut order = std::collections::BTreeSet::new();
    if contract.install_order.iter().any(|namespace| {
        !valid_application_namespace(namespace) || !order.insert(namespace.clone())
    }) {
        return Err(AppRuntimeError::InvalidPackage(
            "application module installation order contains an invalid or duplicate namespace"
                .to_string(),
        ));
    }

    let mut stable_keys = std::collections::BTreeSet::new();
    let mut qualified_names = std::collections::BTreeSet::new();
    for record_type in &contract.record_types {
        if record_type.name.trim().is_empty()
            || record_type.model.trim().is_empty()
            || record_type.namespace != contract.namespace
            || record_type.owner != contract.namespace
            || record_type.qualified_name != format!("{}.{}", contract.namespace, record_type.name)
            || record_type.stable_key.trim().is_empty()
            || record_type.schema_version == 0
            || !matches!(
                record_type.scope_level.as_str(),
                "hub" | "cell" | "organization" | "user"
            )
            || !stable_keys.insert(record_type.stable_key.clone())
            || !qualified_names.insert(record_type.qualified_name.clone())
        {
            return Err(AppRuntimeError::InvalidPackage(
                "application record type ownership, stable identity, schema version, or scope is invalid"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_supply_chain_documents(package: &ApplicationPackage) -> Result<()> {
    let lock = dependency_lock(package)?;
    let mut names = std::collections::BTreeSet::new();
    for dependency in &lock.packages {
        if dependency.name.trim().is_empty()
            || !names.insert(dependency.name.to_ascii_lowercase())
            || semver::Version::parse(&dependency.version).is_err()
            || !is_sha256(&dependency.package_sha256)
            || dependency
                .module_sha256
                .as_deref()
                .is_some_and(|hash| !is_sha256(hash))
        {
            return Err(AppRuntimeError::InvalidPackage(
                "dependency lock contains an invalid or duplicate exact package".to_string(),
            ));
        }
    }
    for declared in &package.manifest.dependencies {
        let locked = lock
            .packages
            .iter()
            .find(|locked| locked.name.eq_ignore_ascii_case(&declared.name));
        if locked.is_none() && declared.optional {
            continue;
        }
        let locked = locked.ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "required dependency `{}` is absent from the exact dependency lock",
                declared.name
            ))
        })?;
        let requirement = semver::VersionReq::parse(&declared.version)
            .map_err(|error| AppRuntimeError::InvalidPackage(error.to_string()))?;
        let version = semver::Version::parse(&locked.version)
            .map_err(|error| AppRuntimeError::InvalidPackage(error.to_string()))?;
        if !requirement.matches(&version)
            || declared
                .module_sha256
                .as_deref()
                .is_some_and(|hash| locked.module_sha256.as_deref() != Some(hash))
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "locked dependency `{}` does not satisfy its signed declaration",
                declared.name
            )));
        }
    }
    let sbom: serde_json::Value = serde_json::from_slice(&package.sbom).map_err(|error| {
        AppRuntimeError::InvalidPackage(format!("SBOM is not valid JSON: {error}"))
    })?;
    if sbom.get("bomFormat").and_then(serde_json::Value::as_str) != Some("CycloneDX") {
        return Err(AppRuntimeError::InvalidPackage(
            "SBOM must be a CycloneDX JSON document".to_string(),
        ));
    }
    let provenance: serde_json::Value =
        serde_json::from_slice(&package.provenance).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!("provenance is not valid JSON: {error}"))
        })?;
    if provenance
        .get("builder")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(AppRuntimeError::InvalidPackage(
            "provenance must identify its builder".to_string(),
        ));
    }
    validate_application_module_contract(package, &lock)?;
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Canonical bytes signed by application packages.
///
/// The entry module contains an embedded copy of the extension manifest. If
/// raw module bytes were placed directly in the signed document, the package
/// hash and signature embedded in that module would make the format
/// self-referential and impossible to produce. The v1 signing envelope solves
/// that by:
///
/// * normalizing only the package hash and signature in the manifest;
/// * signing the SHA-256 digest of every exact module byte sequence; and
/// * signing the hashes of the dependency lock, SBOM, and provenance.
///
/// Consequently every executable byte remains covered while package builders
/// can embed well-formed placeholders for the two self-referential fields.
pub fn canonical_signing_payload(package: &ApplicationPackage) -> Result<Vec<u8>> {
    let mut manifest = package.manifest.clone();
    let application = manifest.application.as_deref_mut().ok_or_else(|| {
        AppRuntimeError::InvalidPackage("missing application manifest".to_string())
    })?;
    application.package.package_sha256.clear();
    application.package.signature.clear();
    let module_sha256 = package
        .modules
        .iter()
        .map(|(name, bytes)| (name.clone(), sha256(bytes)))
        .collect::<BTreeMap<_, _>>();
    let frontend_assets = package
        .frontend_assets
        .iter()
        .map(|(path, asset)| {
            (
                path.clone(),
                FrontendAssetSignature {
                    content_type: asset.content_type.clone(),
                    sha256: sha256(&asset.bytes),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    Ok(serde_json::to_vec(&PackageSigningEnvelope {
        format: "bicdb-application-package-signature-v1",
        manifest,
        module_sha256: &module_sha256,
        frontend_assets: &frontend_assets,
        components: &package.components,
        dependency_lock_sha256: sha256(&package.dependency_lock),
        sbom_sha256: sha256(&package.sbom),
        provenance_sha256: sha256(&package.provenance),
    })?)
}

/// Reconstruct the pre-2.3.27 BicDB application envelope that omitted an empty
/// `invariants` list. The fallback is deliberately byte-narrow: no other
/// canonical difference is accepted, and a non-empty list has no legacy form.
fn canonical_signing_payload_without_empty_invariants(
    package: &ApplicationPackage,
) -> Result<Option<Vec<u8>>> {
    if !package
        .manifest
        .application
        .as_deref()
        .is_some_and(|application| application.invariants.is_empty())
    {
        return Ok(None);
    }
    let mut payload = canonical_signing_payload(package)?;
    const EMPTY_INVARIANTS: &[u8] = b",\"invariants\":[]";
    let positions = payload
        .windows(EMPTY_INVARIANTS.len())
        .enumerate()
        .filter_map(|(position, window)| (window == EMPTY_INVARIANTS).then_some(position))
        .collect::<Vec<_>>();
    if positions.len() != 1 {
        return Err(AppRuntimeError::InvalidPackage(
            "canonical application envelope does not contain exactly one empty invariants field"
                .to_string(),
        ));
    }
    let position = positions[0];
    payload.drain(position..position + EMPTY_INVARIANTS.len());
    Ok(Some(payload))
}

/// Compares the signed package manifest to the entry module's embedded
/// manifest without requiring the module to contain its own resulting hash
/// and signature.
pub(crate) fn embedded_manifest_matches(
    signed: &ExtensionManifest,
    embedded: &ExtensionManifest,
) -> bool {
    let mut signed = signed.clone();
    let mut embedded = embedded.clone();
    for manifest in [&mut signed, &mut embedded] {
        if let Some(application) = manifest.application.as_deref_mut() {
            application.package.package_sha256.clear();
            application.package.signature.clear();
        }
    }
    signed == embedded
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use bicdb_extension::abi_v2::{
        ApplicationManifestV2, PackageMetadata, APPLICATION_COMPATIBILITY_PROFILE,
    };
    use bicdb_extension::{
        ExtensionIdentity, ExtensionLimits, ExtensionManifest, ExtensionPermissions,
    };
    use ed25519_dalek::{Signer, SigningKey};

    use super::*;

    fn signed_package() -> (ApplicationPackage, TrustedSigningKeys) {
        let dependency_lock = br#"{"version":1,"packages":[]}"#.to_vec();
        let sbom = br#"{"bomFormat":"CycloneDX"}"#.to_vec();
        let provenance = br#"{"builder":"bicdb-test"}"#.to_vec();
        let application = ApplicationManifestV2 {
            abi_version: 2,
            application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
            package: PackageMetadata {
                application: "carrier-test".to_string(),
                version: "1.0.0".to_string(),
                package_sha256: "0".repeat(64),
                dependency_lock_sha256: sha256(&dependency_lock),
                sbom_sha256: sha256(&sbom),
                provenance_sha256: sha256(&provenance),
                signature_key_id: "test-release".to_string(),
                signature_algorithm: "ed25519".to_string(),
                signature: "unsigned-placeholder".to_string(),
            },
            relation_permissions: vec![],
            raw_sql: vec![],
            service_imports: vec![],
            service_exports: vec![],
            secrets: vec![],
            egress: vec![],
            blobs: vec![],
            routes: vec![],
            response_headers: BTreeMap::new(),
            auth_schemes: BTreeMap::new(),
            realtime: vec![],
            resources: vec![],
            invariants: vec![],
            migrations: vec![],
            workers: vec![],
            schedules: vec![],
            application_program: None,
            required_features: BTreeSet::new(),
            max_call_depth: 16,
        };
        let manifest = ExtensionManifest {
            identity: ExtensionIdentity {
                name: "carrier-test".to_string(),
                version: "1.0.0".to_string(),
                abi_version: 2,
                description: "signed application test".to_string(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::new(),
            permissions: ExtensionPermissions::default(),
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(application)),
        };
        let mut package = ApplicationPackage {
            manifest,
            modules: BTreeMap::from([("carrier-test".to_string(), b"\0asm-test".to_vec())]),
            frontend_assets: BTreeMap::new(),
            components: Vec::new(),
            dependency_lock,
            sbom,
            provenance,
        };
        let payload = canonical_signing_payload(&package).unwrap();
        let package_hash = sha256(&payload);
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let signature = signing_key.sign(package_hash.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_hash;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        let mut keys = TrustedSigningKeys::default();
        keys.insert_ed25519("test-release", signing_key.verifying_key().as_bytes())
            .unwrap();
        (package, keys)
    }

    fn resign(package: &mut ApplicationPackage) {
        let payload = canonical_signing_payload(package).unwrap();
        let package_hash = sha256(&payload);
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let signature = signing_key.sign(package_hash.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_hash;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    }

    fn attach_application_module_contract(package: &mut ApplicationPackage) {
        let requirement = serde_json::json!({
            "namespace": "hub.identity",
            "version": "^1.0",
        });
        let module = serde_json::json!({
            "contract_version": 1,
            "namespace": "sample.foundation",
            "semantic_version": "1.0.0",
            "system": true,
            "requires": [requirement.clone()],
            "install_order": ["hub.identity", "sample.foundation"],
            "record_types": [{
                "name": "PartyRole",
                "namespace": "sample.foundation",
                "qualified_name": "sample.foundation.PartyRole",
                "stable_key": "sample.foundation.party-role",
                "owner": "sample.foundation",
                "schema_version": 1,
                "scope_level": "organization",
                "model": "PartyRole",
                "traits": ["OrganizationScoped"],
                "links": [],
            }],
            "traits": [],
            "extensions": [],
        });
        package.dependency_lock = serde_json::to_vec(&serde_json::json!({
            "format": 1,
            "packages": [],
            "application_modules": {
                "namespace": "sample.foundation",
                "version": "1.0.0",
                "system": true,
                "requires": [requirement],
                "install_order": ["hub.identity", "sample.foundation"],
            },
        }))
        .unwrap();
        package.provenance = serde_json::to_vec(&serde_json::json!({
            "builder": "bicdb-test",
            "target": "bicdb-application-v2",
            "application": "sample-test",
            "version": "1.0.0",
            "resource_contracts": [],
            "plugin_dependencies": [],
            "application_module_contract": module,
        }))
        .unwrap();
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.dependency_lock_sha256 = sha256(&package.dependency_lock);
        metadata.provenance_sha256 = sha256(&package.provenance);
        resign(package);
    }

    #[test]
    fn signing_envelope_covers_exact_artifacts_without_self_reference() {
        let (package, keys) = signed_package();
        let verification = PackageVerifier::new(keys, 1024 * 1024)
            .unwrap()
            .verify(&package)
            .unwrap();
        assert_eq!(
            verification.package_sha256,
            package
                .manifest
                .application
                .as_deref()
                .unwrap()
                .package
                .package_sha256
        );
    }

    #[test]
    fn signed_application_contract_must_match_the_lock_and_own_unique_record_types() {
        let (mut package, keys) = signed_package();
        attach_application_module_contract(&mut package);
        let verifier = PackageVerifier::new(keys.clone(), 1024 * 1024).unwrap();
        verifier.verify(&package).unwrap();

        let mut mismatched = package.clone();
        let mut lock: serde_json::Value =
            serde_json::from_slice(&mismatched.dependency_lock).unwrap();
        lock["application_modules"]["version"] = serde_json::json!("2.0.0");
        mismatched.dependency_lock = serde_json::to_vec(&lock).unwrap();
        let metadata = &mut mismatched
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .package;
        metadata.dependency_lock_sha256 = sha256(&mismatched.dependency_lock);
        resign(&mut mismatched);
        let error = verifier.verify(&mismatched).unwrap_err();
        assert!(
            error.to_string().contains("application module identity"),
            "{error}"
        );

        let mut duplicate = package;
        let mut provenance: serde_json::Value =
            serde_json::from_slice(&duplicate.provenance).unwrap();
        let copied = provenance["application_module_contract"]["record_types"][0].clone();
        provenance["application_module_contract"]["record_types"]
            .as_array_mut()
            .unwrap()
            .push(copied);
        duplicate.provenance = serde_json::to_vec(&provenance).unwrap();
        let metadata = &mut duplicate
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .package;
        metadata.provenance_sha256 = sha256(&duplicate.provenance);
        resign(&mut duplicate);
        let error = verifier.verify(&duplicate).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("application record type ownership"),
            "{error}"
        );
    }

    #[test]
    fn verifier_accepts_only_the_legacy_empty_invariants_omission() {
        let (mut package, keys) = signed_package();
        let payload = canonical_signing_payload_without_empty_invariants(&package)
            .unwrap()
            .unwrap();
        let package_hash = sha256(&payload);
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let signature = signing_key.sign(package_hash.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_hash.clone();
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());

        let verification = PackageVerifier::new(keys, 1024 * 1024)
            .unwrap()
            .verify(&package)
            .unwrap();
        assert_eq!(verification.package_sha256, package_hash);

        package
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .invariants = vec![serde_json::from_value(serde_json::json!({
            "version": 1,
            "name": "forged",
            "subject_resource": "absent",
            "kind": "must_always",
            "expression": {"op":"literal", "value":true},
            "dependency_resources": [],
            "source": "true"
        }))
        .unwrap()];
        assert!(canonical_signing_payload_without_empty_invariants(&package)
            .unwrap()
            .is_none());
    }

    #[test]
    fn tampering_with_a_module_is_rejected() {
        let (mut package, keys) = signed_package();
        package.modules.get_mut("carrier-test").unwrap().push(0xff);
        let error = PackageVerifier::new(keys, 1024 * 1024)
            .unwrap()
            .verify(&package)
            .unwrap_err();
        assert!(
            error.to_string().contains("package hash mismatch"),
            "{error}"
        );
    }

    #[test]
    fn frontend_bytes_and_component_authority_are_covered_by_the_signature() {
        let (mut package, keys) = signed_package();
        package.frontend_assets.insert(
            "index.html".to_string(),
            FrontendAsset {
                content_type: "text/html; charset=utf-8".to_string(),
                bytes: b"<!record_type html><title>BicDB</title>".to_vec(),
            },
        );
        package.components.push(ApplicationComponent {
            name: "web".to_string(),
            kind: ApplicationComponentKind::Frontend,
            scope: ApplicationExecutionScope::Cell,
            data_class: ApplicationDataClass::Sensitive,
            capabilities: BTreeSet::from([ApplicationCapability::FrontendAssets]),
            egress: BTreeSet::new(),
            database_features: BTreeSet::new(),
        });
        resign(&mut package);

        let verifier = PackageVerifier::new(keys.clone(), 1024 * 1024).unwrap();
        let verified = verifier.verify(&package).unwrap();
        assert_eq!(
            verified.frontend_sha256["index.html"],
            sha256(b"<!record_type html><title>BicDB</title>")
        );

        let mut tampered_asset = package.clone();
        tampered_asset
            .frontend_assets
            .get_mut("index.html")
            .unwrap()
            .bytes
            .push(b'!');
        assert!(verifier.verify(&tampered_asset).is_err());

        let mut tampered_media_type = package.clone();
        tampered_media_type
            .frontend_assets
            .get_mut("index.html")
            .unwrap()
            .content_type = "application/javascript; charset=utf-8".to_string();
        assert!(verifier.verify(&tampered_media_type).is_err());

        let mut tampered_authority = package;
        tampered_authority.components[0].scope = ApplicationExecutionScope::Global;
        assert!(verifier.verify(&tampered_authority).is_err());
    }

    #[test]
    fn legacy_protected_data_data_classes_decode_but_canonical_output_is_industry_neutral() {
        assert_eq!(
            serde_json::from_str::<ApplicationDataClass>("\"phi\"").unwrap(),
            ApplicationDataClass::Regulated
        );
        assert_eq!(
            serde_json::to_string(&ApplicationDataClass::Regulated).unwrap(),
            "\"regulated\""
        );
    }

    #[test]
    fn embedded_manifest_may_differ_only_in_self_referential_fields() {
        let (package, _) = signed_package();
        let mut embedded = package.manifest.clone();
        let metadata = &mut embedded.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = "0".repeat(64);
        metadata.signature = "unsigned-placeholder".to_string();
        assert!(embedded_manifest_matches(&package.manifest, &embedded));
        embedded.identity.version = "2.0.0".to_string();
        assert!(!embedded_manifest_matches(&package.manifest, &embedded));
    }
    #[test]
    fn bounded_package_input_preserves_signed_compact_and_pretty_packages() {
        let (package, keys) = signed_package();
        let limit = package.check_decoded_size(usize::MAX).unwrap() + 1024;
        let verifier = PackageVerifier::new(keys, limit).unwrap();
        let expected = verifier.verify(&package).unwrap();
        for encoded in [
            serde_json::to_vec(&package).unwrap(),
            serde_json::to_vec_pretty(&package).unwrap(),
        ] {
            let decoded = verifier.read_package(encoded.as_slice()).unwrap();
            assert_eq!(decoded, package);
            assert_eq!(verifier.verify(&decoded).unwrap(), expected);
        }
    }

    #[test]
    fn encoded_limit_precedes_file_decode_and_does_not_turn_a_stream_prefix_into_eof() {
        use std::io::Write;
        let (package, keys) = signed_package();
        let limit = package.check_decoded_size(usize::MAX).unwrap() + 1024;
        let verifier = PackageVerifier::new(keys, limit).unwrap();
        let encoded_limit = crate::encoded_package_byte_limit(limit).unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"not JSON").unwrap();
        file.as_file().set_len(encoded_limit + 1).unwrap();
        let error = verifier.read_package_file(file.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("encoded package exceeds byte limit"),
            "{error}"
        );

        let mut encoded = serde_json::to_vec(&package).unwrap();
        encoded.resize(encoded_limit as usize, b' ');
        let decoded = verifier.read_package(encoded.as_slice()).unwrap();
        verifier.verify(&decoded).unwrap();
        encoded.push(b' ');
        let error = verifier.read_package(encoded.as_slice()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("encoded package exceeds byte limit"),
            "{error}"
        );
    }

    #[test]
    fn module_metadata_limits_reject_before_decoding_the_associated_value() {
        let verifier = PackageVerifier::new(TrustedSigningKeys::default(), 1024 * 1024).unwrap();
        for name in [
            "x".repeat(crate::MAX_MODULE_NAME_BYTES + 1),
            "é".repeat(crate::MAX_MODULE_NAME_BYTES / 2 + 1),
        ] {
            let encoded = format!(
                "{{\"modules\":{{{}:!",
                serde_json::to_string(&name).unwrap()
            );
            let error = verifier.read_package(encoded.as_bytes()).unwrap_err();
            assert!(
                error.to_string().contains("module name must contain"),
                "{error}"
            );
        }
        let entries = (0..crate::MAX_PACKAGE_MODULES)
            .map(|n| format!("\"m{n}\":[]"))
            .collect::<Vec<_>>()
            .join(",");
        let encoded = format!("{{\"modules\":{{{entries},\"excess\":!");
        let error = verifier.read_package(encoded.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("4096 module entries"), "{error}");
        let error = verifier
            .read_package(b"{\"modules\":{\"same\":[],\"same\":!".as_slice())
            .unwrap_err();
        assert!(
            error.to_string().contains("duplicate module name"),
            "{error}"
        );
    }

    #[test]
    fn decoded_limit_counts_names_manifest_assets_and_components_before_signatures() {
        let (package, keys) = signed_package();
        let base = package.check_decoded_size(usize::MAX).unwrap();
        let verifier = PackageVerifier::new(keys, base + 128).unwrap();
        let mut variants = Vec::new();
        let mut modules = package.clone();
        modules.modules.insert("m".repeat(512), vec![0]);
        variants.push(modules);
        let mut manifest = package.clone();
        manifest
            .manifest
            .identity
            .description
            .push_str(&"m".repeat(512));
        variants.push(manifest);
        let mut assets = package.clone();
        assets.frontend_assets.insert(
            format!("{}.png", "a".repeat(512)),
            FrontendAsset {
                content_type: "image/png".into(),
                bytes: vec![0],
            },
        );
        variants.push(assets);
        let mut components = package.clone();
        components.components.push(ApplicationComponent {
            name: "background".into(),
            kind: ApplicationComponentKind::Worker,
            scope: ApplicationExecutionScope::Global,
            data_class: ApplicationDataClass::Public,
            capabilities: Default::default(),
            database_features: Default::default(),
            egress: [format!("https://example.test/{}", "a".repeat(512))]
                .into_iter()
                .collect(),
        });
        variants.push(components);
        for candidate in variants {
            let error = verifier.verify(&candidate).unwrap_err();
            assert!(
                error.to_string().contains("decoded byte limit")
                    || error
                        .to_string()
                        .contains("decoded package exceeds byte limit"),
                "{error}"
            );
        }
        let mut long_name = package.clone();
        long_name
            .modules
            .insert("m".repeat(crate::MAX_MODULE_NAME_BYTES + 1), vec![0]);
        let verifier = PackageVerifier::new(TrustedSigningKeys::default(), 1024 * 1024).unwrap();
        assert!(verifier
            .verify(&long_name)
            .unwrap_err()
            .to_string()
            .contains("module name must contain"));
        let mut many = package;
        for n in 0..crate::MAX_PACKAGE_MODULES {
            many.modules.insert(format!("m{n}"), vec![]);
        }
        assert!(verifier
            .verify(&many)
            .unwrap_err()
            .to_string()
            .contains("module entries"));
    }
}
