//! Persisted Decimal predicate comparison semantics belong to the generation,
//! not merely the unchanged predicate JSON. Admission precedes page recovery
//! and every public handle, including application-specific recovery openers.
use super::*;

pub(super) const COMPARISON_VERSION: u32 = 1;

pub(super) fn affected(definition: &IndexDefinition) -> bool {
    fn decimal_comparison(predicate: &IndexPredicate) -> bool {
        match predicate {
            IndexPredicate::Binary {
                value_type,
                left,
                right,
                ..
            } => {
                *value_type == IndexPredicateValueType::Decimal
                    || decimal_comparison(left)
                    || decimal_comparison(right)
            }
            IndexPredicate::Unary { value, .. } => decimal_comparison(value),
            IndexPredicate::Field { .. } | IndexPredicate::Literal { .. } => false,
        }
    }
    definition
        .predicate
        .as_ref()
        .is_some_and(decimal_comparison)
}

pub(super) fn check_open_admission(
    root: &Path,
    encryption: &EncryptionRuntime,
    mode: &StorageMode,
) -> Result<()> {
    let Some(bytes) = read_optional_sidecar(
        &root.join(DEFAULT_INDEX_CATALOG),
        FrameKind::Index,
        encryption,
    )?
    else {
        return Ok(());
    };
    if bytes.is_empty() {
        return Ok(());
    }
    let catalog: IndexCatalog = serde_json::from_slice(&bytes)?;
    match catalog.decimal_predicate_semantics_version {
        None => {
            if catalog.indexes.iter().any(affected) {
                // Do not certify an existing generation merely because its
                // definition decodes under the new comparator. The bounded,
                // owned-shadow transition is a separate required next stage.
                return Err(BicDbError::FormatCompatibility(
                    "legacy Decimal predicate index generation requires a verified semantic transition before read/write admission".into(),
                ));
            }
        }
        Some(COMPARISON_VERSION) => {
            if !format::read_or_legacy_metadata(root)?
                .feature_flags
                .contains(format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG)
            {
                return Err(BicDbError::FormatCompatibility(
                    "unfenced Decimal predicate semantic certificate".into(),
                ));
            }
            if *mode == StorageMode::ServerPaged {
                for definition in catalog.indexes.iter().filter(|definition| {
                    affected(definition) && definition.kind == IndexKind::BTree
                }) {
                    if !catalog.btree_bindings.contains_key(&definition.name) {
                        return Err(BicDbError::FormatCompatibility(
                            "Decimal predicate semantic certificate lacks an authoritative physical generation binding".into(),
                        ));
                    }
                }
            }
        }
        Some(_) => {
            return Err(BicDbError::FormatCompatibility(
                "unsupported Decimal predicate comparison semantic version".into(),
            ));
        }
    }
    Ok(())
}
