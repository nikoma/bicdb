//! Operator-owned bindings to the cluster's immutable event data plane.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Result};
use bicdb_core::{ClusterEventClient, DistributionStore};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    format: u32,
    /// Directory of a running member on this trusted host. Private node TLS
    /// keys stay outside releases and guest environments.
    member_root: PathBuf,
    applications: BTreeMap<String, String>,
}

pub(crate) struct Host {
    pub client: ClusterEventClient,
    applications: BTreeMap<String, String>,
}

impl Host {
    pub fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "event configuration must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "event configuration must have mode 0600"
            );
        }
        let config: Config = serde_json::from_slice(&crate::store::read_bounded(path, 64 * 1024)?)?;
        ensure!(
            config.format == 1,
            "unsupported event cluster configuration"
        );
        ensure!(
            config.member_root.is_absolute(),
            "event member_root must be absolute"
        );
        ensure!(
            !config.applications.is_empty() && config.applications.len() <= 1024,
            "event configuration requires 1..=1024 application allocations"
        );
        let mut namespaces = BTreeSet::new();
        for (app, namespace) in &config.applications {
            ensure!(
                crate::manifest::valid_name(app),
                "invalid event application allocation"
            );
            bicdb_core::cluster_event_record_id(namespace, "validation")?;
            ensure!(
                namespaces.insert(namespace),
                "event namespaces must be unique per application"
            );
        }
        let mut distribution = bicdb_core::load_distribution_config(&config.member_root)?;
        let store = DistributionStore::open(&config.member_root, distribution.clone(), true)?;
        // Guest-triggered work cannot multiply the cluster's operator I/O
        // timeout by every member and retry after an HTTP client disconnects.
        distribution.transport.connect_timeout_ms =
            distribution.transport.connect_timeout_ms.min(250);
        distribution.transport.io_timeout_ms = distribution.transport.io_timeout_ms.min(1000);
        let client = ClusterEventClient::new(&distribution, store.topology().clone())?
            .with_request_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self {
            client,
            applications: config.applications,
        })
    }

    pub fn namespace(&self, app: &str) -> Result<&str> {
        self.applications
            .get(app)
            .map(String::as_str)
            .ok_or_else(|| anyhow::anyhow!("application lacks an operator event allocation"))
    }
}
