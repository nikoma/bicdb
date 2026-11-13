//! Future-only allocation identities. A birth ID alone is deliberately not a
//! portable origin proof: only an authenticated Native producer can bind an
//! actual source birth to an actual destination allocation and generation.
use super::*;

pub(super) fn validate_catalog(
    metas: &[CollectionMeta],
    active: bool,
    namespace_identity_active: bool,
) -> Result<()> {
    if active && !namespace_identity_active {
        return Err(BicDbError::FormatCompatibility(
            "Native collection birth has no local namespace identity fence".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    for meta in metas {
        let Some(birth) = meta.native_backing_birth_id else {
            // Legacy, pre-activation and flag-only interruption remain unborn.
            continue;
        };
        if !active
            || birth.is_nil()
            || !seen.insert(birth)
            || meta.local_namespace_id.is_none_or(|id| id.is_nil())
        {
            return Err(BicDbError::FormatCompatibility(format!(
                "collection `{}` has an unfenced, nil, duplicate or unbound Native backing birth",
                meta.name
            )));
        }
    }
    Ok(())
}

impl BicDb {
    /// Enable allocation-time identities for future local backings. Existing
    /// collections stay unborn, including when activation is retried. Local
    /// namespace identities must already be explicitly enabled. This grants
    /// no source/import/rename recovery authority and installs no origin map.
    pub fn activate_native_collection_birth_v1(&mut self) -> Result<()> {
        self.ensure_writable("activate Native collection birth")?;
        self.check_catalog_recovery_quiescent()?;
        validate_catalog(
            &self.collections(),
            self.native_collection_birth_active,
            self.local_namespace_identity_active,
        )?;
        if !self.local_namespace_identity_active {
            return Err(BicDbError::InvalidConfig(
                "Native collection birth requires explicit local namespace activation".into(),
            ));
        }
        if let Err(error) = format::enable_native_collection_birth(&self.path) {
            self.fence_durability("native_collection_birth.format_fence", &error);
            return Err(error);
        }
        self.native_collection_birth_active = true;
        Ok(())
    }

    /// Read this node's allocation identity. None must never be promoted to a
    /// birth by inference from a name, storage alias or migrated namespace ID.
    /// Some is still an identity, not proof of authenticated portable origin.
    pub fn native_collection_birth_id(&self, collection: &str) -> Result<Option<Uuid>> {
        self.check_recovery_state()?;
        let state = self.collection_state(collection)?;
        validate_catalog(
            std::slice::from_ref(&state.meta),
            self.native_collection_birth_active,
            self.local_namespace_identity_active,
        )?;
        Ok(state.meta.native_backing_birth_id)
    }

    pub(super) fn fresh_native_collection_birth(&self) -> Option<Uuid> {
        self.native_collection_birth_active.then(Uuid::new_v4)
    }
}

#[cfg(test)]
mod tests;
