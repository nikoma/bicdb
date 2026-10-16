//! Operator-held encryption separates peer credentials from recovery images.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};

pub struct PeerCipher {
    key: aead::LessSafeKey,
    binding: Vec<u8>,
}

impl PeerCipher {
    pub fn read(path: &Path, application: &str, cluster: &str) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                "edge sync key must have mode 0600"
            );
        }
        let bytes = crate::store::read_bounded(path, 128)?;
        let text = std::str::from_utf8(&bytes)?.trim();
        ensure!(
            text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "edge sync key must contain 32 hexadecimal bytes"
        );
        let mut key = [0u8; 32];
        for (index, byte) in key.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)?;
        }
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .map_err(|_| anyhow::anyhow!("invalid edge sync key"))?;
        Ok(Self {
            key: aead::LessSafeKey::new(key),
            binding: serde_json::to_vec(&(1u32, application, cluster))?,
        })
    }

    pub fn seal(&self, peers: &BTreeMap<String, Peer>) -> Result<String> {
        let mut body = serde_json::to_vec(peers)?;
        ensure!(
            peers.len() <= 128 && body.len() <= 1024 * 1024,
            "peer grants too large"
        );
        let mut nonce = [0u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("peer encryption entropy unavailable"))?;
        self.key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(&self.binding),
                &mut body,
            )
            .map_err(|_| anyhow::anyhow!("peer grants encryption failed"))?;
        let mut framed = nonce.to_vec();
        framed.extend(body);
        Ok(STANDARD.encode(framed))
    }

    pub fn open(&self, ciphertext: &str) -> Result<BTreeMap<String, Peer>> {
        ensure!(ciphertext.len() <= 2 * 1024 * 1024, "peer grants too large");
        let mut bytes = STANDARD.decode(ciphertext)?;
        ensure!(
            bytes.len() >= 12 + self.key.algorithm().tag_len(),
            "invalid peer grants"
        );
        let nonce: [u8; 12] = bytes[..12].try_into()?;
        let body = self
            .key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(&self.binding),
                &mut bytes[12..],
            )
            .map_err(|_| anyhow::anyhow!("peer grants key or application binding differs"))?;
        let peers: BTreeMap<String, Peer> = serde_json::from_slice(body)?;
        ensure!(peers.len() <= 128, "too many synchronization peers");
        for (id, peer) in &peers {
            peer.validate()?;
            ensure!(*id == peer.node_id.to_string(), "peer identity mismatch");
        }
        Ok(peers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grants_are_bound_to_the_operator_key_application_and_cluster() {
        let root = tempfile::tempdir().unwrap();
        let key = root.path().join("key");
        crate::store::write_synced(&key, "ab".repeat(32).as_bytes()).unwrap();
        let cipher = PeerCipher::read(&key, "notes", "state-a").unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let peers = BTreeMap::from([(
            id.clone(),
            Peer {
                node_id: id.parse().unwrap(),
                public_key: "12".repeat(32),
                endpoint: "https://edge.example".into(),
                token: "peer-transport-secret-00000000000000000000".into(),
            },
        )]);
        let ciphertext = cipher.seal(&peers).unwrap();
        assert!(!ciphertext.contains(&peers[&id].token));
        assert!(cipher.open(&ciphertext).unwrap() == peers);
        assert!(PeerCipher::read(&key, "other", "state-a")
            .unwrap()
            .open(&ciphertext)
            .is_err());
        assert!(PeerCipher::read(&key, "notes", "state-b")
            .unwrap()
            .open(&ciphertext)
            .is_err());
        crate::store::atomic_write(&key, "cd".repeat(32).as_bytes()).unwrap();
        assert!(PeerCipher::read(&key, "notes", "state-a")
            .unwrap()
            .open(&ciphertext)
            .is_err());
        let mut corrupt = STANDARD.decode(&ciphertext).unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(cipher.open(&STANDARD.encode(corrupt)).is_err());
    }
}
