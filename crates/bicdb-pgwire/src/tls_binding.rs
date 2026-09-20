//! RFC 5929 tls-server-end-point uses the leaf certificate's signature hash.
use super::*;
use sha2::{Sha224, Sha384, Sha512, Sha512_224, Sha512_256};
use x509_parser::{prelude::*, signature_algorithm::RsaSsaPssParams};

/// Retain binding bytes from the same certificate snapshot used by rustls.
/// Replacing the PEM on disk must not change an already loaded TLS identity.
#[derive(Debug)]
pub(crate) struct PgWireTlsConfig {
    pub(crate) server: Arc<ServerConfig>,
    pub(crate) channel_binding: std::result::Result<Vec<u8>, String>,
}

pub(crate) fn tls_server_endpoint_binding(der: &[u8]) -> Result<Vec<u8>> {
    let (remaining, certificate) = X509Certificate::from_der(der).map_err(|error| {
        PgWireError::Server(format!(
            "cannot parse TLS leaf certificate for channel binding: {error}"
        ))
    })?;
    if !remaining.is_empty() {
        return Err(PgWireError::Server(
            "trailing data in TLS leaf certificate".into(),
        ));
    }
    let hash_oid = certificate_signature_hash_oid(&certificate.signature_algorithm)?;
    Ok(match hash_oid.as_str() {
        "1.2.840.113549.2.5" | "1.3.14.3.2.26" | "2.16.840.1.101.3.4.2.1" => {
            Sha256::digest(der).to_vec()
        }
        "2.16.840.1.101.3.4.2.2" => Sha384::digest(der).to_vec(),
        "2.16.840.1.101.3.4.2.3" => Sha512::digest(der).to_vec(),
        "2.16.840.1.101.3.4.2.4" => Sha224::digest(der).to_vec(),
        "2.16.840.1.101.3.4.2.5" => Sha512_224::digest(der).to_vec(),
        "2.16.840.1.101.3.4.2.6" => Sha512_256::digest(der).to_vec(),
        _ => {
            return Err(PgWireError::Server(format!(
                "TLS certificate uses unsupported channel-binding hash {hash_oid}"
            )))
        }
    })
}

fn certificate_signature_hash_oid(
    algorithm: &x509_parser::x509::AlgorithmIdentifier<'_>,
) -> Result<String> {
    let oid = algorithm.algorithm.to_id_string();
    let hash_oid = if oid == "1.2.840.113549.1.1.10" {
        let parameters = algorithm.parameters.as_ref().ok_or_else(|| {
            PgWireError::Server("RSASSA-PSS certificate signature has no parameters".into())
        })?;
        let parameters = RsaSsaPssParams::try_from(parameters).map_err(|error| {
            PgWireError::Server(format!(
                "invalid RSASSA-PSS certificate parameters: {error}"
            ))
        })?;
        // The message hash determines the binding, not the MGF1 hash. An
        // omitted hashAlgorithm inside the parameter SEQUENCE means SHA-1.
        parameters.hash_algorithm_oid().to_id_string()
    } else {
        match oid.as_str() {
            // RSA/DSA/ECDSA with MD5 or SHA-1: RFC 5929 requires SHA-256.
            "1.2.840.113549.1.1.4" | "1.2.840.113549.1.1.5"
            | "1.3.14.3.2.29" | "1.2.840.10040.4.3"
            | "1.3.14.3.2.27" | "1.2.840.10045.4.1" => "2.16.840.1.101.3.4.2.1",
            "1.2.840.113549.1.1.14" | "1.2.840.10045.4.3.1"
            | "2.16.840.1.101.3.4.3.1" => "2.16.840.1.101.3.4.2.4",
            "1.2.840.113549.1.1.11" | "1.2.840.10045.4.3.2"
            | "2.16.840.1.101.3.4.3.2" => "2.16.840.1.101.3.4.2.1",
            "1.2.840.113549.1.1.12" | "1.2.840.10045.4.3.3"
            | "2.16.840.1.101.3.4.3.3" => "2.16.840.1.101.3.4.2.2",
            "1.2.840.113549.1.1.13" | "1.2.840.10045.4.3.4"
            | "2.16.840.1.101.3.4.3.4" => "2.16.840.1.101.3.4.2.3",
            "1.2.840.113549.1.1.15" => "2.16.840.1.101.3.4.2.5",
            "1.2.840.113549.1.1.16" => "2.16.840.1.101.3.4.2.6",
            _ => return Err(PgWireError::Server(format!(
                "TLS certificate signature algorithm {oid} has no supported tls-server-end-point hash"
            ))),
        }.to_owned()
    };
    Ok(hash_oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certificate(name: &str) -> Vec<u8> {
        let pem = std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/tls")
                .join(format!("{name}.pem")),
        )
        .unwrap();
        let der = rustls_pemfile::certs(&mut &pem[..])
            .next()
            .unwrap()
            .unwrap()
            .as_ref()
            .to_vec();
        der
    }

    #[test]
    fn certificate_signature_hashes_include_pss_parameters_and_legacy_fallbacks() {
        for (name, bits) in [
            ("sha256", 256),
            ("sha384", 384),
            ("sha224", 224),
            ("sha512", 512),
            ("sha1", 256),
            ("md5", 256),
            ("pss384", 384),
            ("pss_default", 256),
        ] {
            let der = certificate(name);
            let expected = match bits {
                224 => Sha224::digest(&der).to_vec(),
                256 => Sha256::digest(&der).to_vec(),
                384 => Sha384::digest(&der).to_vec(),
                512 => Sha512::digest(&der).to_vec(),
                _ => unreachable!(),
            };
            assert_eq!(
                tls_server_endpoint_binding(&der).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn unknown_or_invalid_signature_hashes_do_not_silently_use_sha256() {
        assert!(tls_server_endpoint_binding(&certificate("ed25519"))
            .unwrap_err()
            .to_string()
            .contains("no supported tls-server-end-point hash"));
        let der = certificate("pss384");
        let (_, cert) = X509Certificate::from_der(&der).unwrap();
        let mut algorithm = cert.signature_algorithm.clone();
        algorithm.parameters = None;
        assert!(certificate_signature_hash_oid(&algorithm)
            .unwrap_err()
            .to_string()
            .contains("no parameters"));
        let rsa = certificate("sha512");
        let (_, rsa) = X509Certificate::from_der(&rsa).unwrap();
        algorithm.parameters = rsa.signature_algorithm.parameters.clone();
        assert!(certificate_signature_hash_oid(&algorithm)
            .unwrap_err()
            .to_string()
            .contains("invalid RSASSA-PSS"));
        assert!(tls_server_endpoint_binding(&der[..der.len() - 1]).is_err());
        let mut trailing = der;
        trailing.push(0);
        assert!(tls_server_endpoint_binding(&trailing)
            .unwrap_err()
            .to_string()
            .contains("trailing data"));
    }

    #[test]
    fn binding_uses_only_the_loaded_leaf_and_survives_pem_replacement() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let directory = tempfile::tempdir().unwrap();
        let cert_path = directory.path().join("chain.pem");
        let key_path = directory.path().join("key.pem");
        let mut chain = std::fs::read(fixtures.join("sha384.pem")).unwrap();
        chain.extend(std::fs::read(fixtures.join("sha256.pem")).unwrap());
        std::fs::write(&cert_path, chain).unwrap();
        std::fs::copy(fixtures.join("sha384.key"), &key_path).unwrap();
        let config = PgWireConfig {
            tls_cert: Some(cert_path.clone()),
            tls_key: Some(key_path),
            ..PgWireConfig::default()
        };
        let loaded = load_tls_config(&config).unwrap().unwrap();
        let expected = Sha384::digest(certificate("sha384")).to_vec();
        assert_eq!(loaded.channel_binding.as_ref().unwrap(), &expected);
        std::fs::copy(fixtures.join("sha256.pem"), cert_path).unwrap();
        assert_eq!(loaded.channel_binding.as_ref().unwrap(), &expected);
    }
}
