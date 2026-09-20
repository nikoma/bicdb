use std::collections::HashMap;

/// The real BicDB build version. PostgreSQL compatibility overrides must never
/// change this value.
pub const BICDB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// PostgreSQL release whose wire and SQL surface BicDB targets by default.
pub const POSTGRES_COMPATIBILITY_VERSION: &str = "18.4";
pub const POSTGRES_COMPATIBILITY_VERSION_NUM: &str = "180004";

pub fn bicdb_version_banner(postgres_version: &str) -> String {
    format!("BicDB {BICDB_VERSION} (PostgreSQL {postgres_version} wire compatible)")
}

/// PostgreSQL-shaped identity for compatibility clients which select their
/// session strategy from `version()`. BicDB remains discoverable through the
/// dedicated, immutable `bicdb_version()` function.
pub fn postgres_version_banner(postgres_version: &str) -> String {
    format!("PostgreSQL {postgres_version} (BicDB compatibility mode)")
}

pub(crate) fn sql_version_banner(session_gucs: &HashMap<String, String>) -> String {
    let postgres_version = postgres_compatibility_version_from_gucs(session_gucs);
    if session_gucs
        .get("bicdb.postgres_version_banner")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "on" | "true" | "1"))
    {
        postgres_version_banner(postgres_version)
    } else {
        bicdb_version_banner(postgres_version)
    }
}

pub(crate) fn postgres_compatibility_version_from_gucs(
    session_gucs: &HashMap<String, String>,
) -> &str {
    session_gucs
        .get("server_version")
        .map(String::as_str)
        .unwrap_or(POSTGRES_COMPATIBILITY_VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_leads_with_the_real_engine_identity() {
        assert_eq!(
            bicdb_version_banner("18.4"),
            format!("BicDB {BICDB_VERSION} (PostgreSQL 18.4 wire compatible)")
        );
    }

    #[test]
    fn compatibility_banner_is_postgresql_shaped_but_names_bicdb() {
        assert_eq!(
            postgres_version_banner("18.4"),
            "PostgreSQL 18.4 (BicDB compatibility mode)"
        );
    }
}
