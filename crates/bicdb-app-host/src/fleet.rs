//! Operator-configured remote compute. The state service remains authoritative;
//! a retired execution loses its database capability before another can launch.
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    manifest::{Isolation, Release},
    runtime::Process,
    store::read_bounded,
};

pub const LEASE: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format: u32,
    pub workers: BTreeMap<String, Worker>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub endpoint: String,
    pub token_file: std::path::PathBuf,
    pub region: String,
    pub capacity: usize,
    pub isolation: Isolation,
}

struct Entry {
    config: Worker,
    token: String,
    assigned: usize,
    retry_after: Instant,
}

pub struct Fleet {
    entries: Mutex<BTreeMap<String, Entry>>,
}

pub fn validate_origin(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)?;
    let local = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/"
            && (url.scheme() == "https" || (url.scheme() == "http" && local)),
        "service endpoint must be an HTTPS origin (HTTP is allowed only on loopback)"
    );
    Ok(())
}

fn client() -> Result<reqwest::blocking::Client> {
    Ok(reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(750))
        .timeout(Duration::from_secs(2))
        .build()?)
}

impl Fleet {
    pub fn read(path: &Path) -> Result<Arc<Self>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                "fleet configuration must have mode 0600"
            );
        }
        let config: Config = serde_json::from_slice(&read_bounded(path, 1024 * 1024)?)?;
        ensure!(
            config.format == 1 && !config.workers.is_empty() && config.workers.len() <= 128,
            "invalid fleet configuration"
        );
        let base = path.parent().unwrap_or(Path::new("."));
        let mut entries = BTreeMap::new();
        let mut endpoints = std::collections::BTreeSet::new();
        for (name, mut worker) in config.workers {
            ensure!(
                crate::manifest::valid_name(&name)
                    && crate::manifest::valid_name(&worker.region)
                    && (1..=4096).contains(&worker.capacity),
                "invalid worker allocation"
            );
            validate_origin(&worker.endpoint)?;
            worker.endpoint = worker.endpoint.trim_end_matches('/').to_owned();
            ensure!(
                endpoints.insert(worker.endpoint.clone()),
                "duplicate worker endpoint"
            );
            if worker.token_file.is_relative() {
                worker.token_file = base.join(&worker.token_file);
            }
            let token = crate::cli::token(&worker.token_file)?;
            entries.insert(
                name,
                Entry {
                    config: worker,
                    token,
                    assigned: 0,
                    retry_after: Instant::now(),
                },
            );
        }
        Ok(Arc::new(Self {
            entries: Mutex::new(entries),
        }))
    }

    pub fn validate(&self, release: &Release) -> Result<()> {
        let placement = release
            .manifest
            .placement
            .as_ref()
            .context("remote placement missing")?;
        ensure!(
            self.entries
                .lock()
                .values()
                .any(|entry| entry.config.region == placement.region
                    && entry.config.isolation == release.manifest.isolation),
            "no configured worker supports this region and isolation"
        );
        Ok(())
    }

    pub fn launch(
        self: &Arc<Self>,
        release: &Release,
        install_token: impl Fn(String),
        authority: Option<bicdb_pgwire::HaWriterLease>,
        state_endpoint: Option<String>,
    ) -> Result<Execution> {
        self.validate(release)?;
        let region = &release.manifest.placement.as_ref().unwrap().region;
        let mut candidates: Vec<_> = self
            .entries
            .lock()
            .iter()
            .filter(|(_, entry)| {
                entry.config.region == *region
                    && entry.config.isolation == release.manifest.isolation
                    && entry.assigned < entry.config.capacity
                    && entry.retry_after <= Instant::now()
            })
            .map(|(name, entry)| (name.clone(), entry.assigned * 4096 / entry.config.capacity))
            .collect();
        candidates.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        for (name, _) in candidates {
            ensure!(
                authority.as_ref().is_none_or(|lease| lease.active()),
                "state writer authority is unavailable"
            );
            let (endpoint, management) = {
                let mut entries = self.entries.lock();
                let entry = entries.get_mut(&name).unwrap();
                if entry.assigned >= entry.config.capacity {
                    continue;
                }
                entry.assigned += 1;
                (entry.config.endpoint.clone(), entry.token.clone())
            };
            let token = crate::server::fresh_token();
            install_token(token.clone());
            let mut remote = Remote {
                fleet: self.clone(),
                worker: name.clone(),
                endpoint,
                management,
                id: uuid::Uuid::new_v4().simple().to_string(),
                token: token.clone(),
                retired: false,
                alive: Arc::new(AtomicBool::new(true)),
            };
            let result = (|| -> Result<()> {
                let response = client()?
                    .post(format!("{}/v1/executions", remote.endpoint))
                    .bearer_auth(&remote.management)
                    .timeout(Duration::from_secs(15))
                    .json(&Launch {
                        id: remote.id.clone(),
                        release: release.clone(),
                        token: token.clone(),
                        state_endpoint: state_endpoint.clone(),
                    })
                    .send()?;
                ensure!(response.status().is_success(), "worker refused activation");
                Ok(())
            })();
            if result.is_ok() {
                ensure!(
                    authority.as_ref().is_none_or(|lease| lease.active()),
                    "state writer authority was lost during activation"
                );
                remote.heartbeat(authority.clone());
                return Ok(Execution::Remote(
                    remote,
                    crate::runtime::upstream_client()?,
                ));
            }
            // Retire even an indeterminate activation; its lease is never renewed.
            remote.retire();
            self.entries.lock().get_mut(&name).unwrap().retry_after =
                Instant::now() + Duration::from_secs(3);
        }
        bail!("no available worker could activate this application")
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Launch {
    pub id: String,
    pub release: Release,
    pub token: String,
    /// Must match one of the worker's operator-pinned state origins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_endpoint: Option<String>,
}

pub enum Execution {
    Local(Process, String),
    Remote(Remote, reqwest::Client),
}

impl Execution {
    pub(crate) fn revoke(&self) {
        if let Self::Remote(remote, _) = self {
            remote.alive.store(false, Ordering::Release);
        }
    }
    pub fn route(&self) -> (String, reqwest::Client, Option<String>, String) {
        match self {
            Self::Local(process, token) => (
                format!("http://{}", process.address),
                process.client.clone(),
                None,
                token.clone(),
            ),
            Self::Remote(remote, client) => (
                format!("{}/run/{}", remote.endpoint, remote.id),
                client.clone(),
                Some(remote.token.clone()),
                remote.token.clone(),
            ),
        }
    }
    pub fn token(&self) -> &str {
        match self {
            Self::Local(_, token) => token,
            Self::Remote(remote, _) => &remote.token,
        }
    }
    pub fn log_request(&self) -> Option<(String, String)> {
        match self {
            Self::Local(_, _) => None,
            Self::Remote(remote, _) => Some((
                format!("{}/v1/executions/{}/logs", remote.endpoint, remote.id),
                remote.management.clone(),
            )),
        }
    }
    pub fn worker(&self) -> Option<&str> {
        match self {
            Self::Local(_, _) => None,
            Self::Remote(remote, _) => Some(&remote.worker),
        }
    }
    pub fn exited(&mut self) -> Result<bool> {
        match self {
            Self::Local(process, _) => process.exited(),
            Self::Remote(remote, _) => {
                let dead = !remote.alive.load(Ordering::Acquire);
                if dead {
                    if let Some(entry) = remote.fleet.entries.lock().get_mut(&remote.worker) {
                        entry.retry_after = Instant::now() + Duration::from_secs(3);
                    }
                }
                Ok(dead)
            }
        }
    }
}

pub struct Remote {
    fleet: Arc<Fleet>,
    worker: String,
    endpoint: String,
    management: String,
    id: String,
    token: String,
    retired: bool,
    alive: Arc<AtomicBool>,
}
impl Remote {
    fn heartbeat(&self, authority: Option<bicdb_pgwire::HaWriterLease>) {
        let endpoint = format!("{}/v1/executions/{}/renew", self.endpoint, self.id);
        let token = self.management.clone();
        let alive = self.alive.clone();
        std::thread::spawn(move || {
            let Ok(client) = client() else {
                alive.store(false, Ordering::Release);
                return;
            };
            while alive.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(750));
                if !alive.load(Ordering::Acquire)
                    || authority.as_ref().is_some_and(|lease| !lease.active())
                {
                    alive.store(false, Ordering::Release);
                    break;
                }
                let result = client.post(&endpoint).bearer_auth(&token).send();
                if !result
                    .as_ref()
                    .is_ok_and(|response| response.status().is_success())
                {
                    eprintln!(
                        "worker lease renewal failed: status={:?}",
                        result.as_ref().ok().map(|r| r.status())
                    );
                    alive.store(false, Ordering::Release);
                    break;
                }
            }
        });
    }
    fn retire(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        self.alive.store(false, Ordering::Release);
        if let Some(entry) = self.fleet.entries.lock().get_mut(&self.worker) {
            entry.assigned = entry.assigned.saturating_sub(1);
        }
        // Drop also runs on async request paths. Perform bounded best-effort
        // retirement off that executor; lease expiry remains the hard stop.
        let endpoint = format!("{}/v1/executions/{}/stop", self.endpoint, self.id);
        let token = self.management.clone();
        std::thread::spawn(move || {
            if let Ok(client) = client() {
                let _ = client.post(endpoint).bearer_auth(token).send();
            }
        });
    }
}
impl Drop for Remote {
    fn drop(&mut self) {
        self.retire();
    }
}
