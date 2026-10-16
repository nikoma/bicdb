use std::fs::{self, File, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::manifest::{Release, Runtime};
use crate::store::atomic_write;

pub struct Process {
    child: Child,
    pub address: SocketAddr,
}

impl Process {
    pub fn start(
        release: &Release,
        directory: &Path,
        app_dir: &Path,
        endpoint: &str,
        token: &str,
        node: &Path,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        // The child binds before becoming ready; failed/racing binds fail activation.
        drop(listener);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(app_dir.join("application.log"))?;
        let mut command = Command::new(node);
        command.arg(format!(
            "--max-old-space-size={}",
            release.manifest.resources.memory_mb
        ));
        match release.manifest.runtime {
            Runtime::Node => {
                let adapter = app_dir.join("adapter.mjs");
                atomic_write(&adapter, include_bytes!("adapter.mjs"))?;
                command.arg(adapter);
            }
            Runtime::NodeHttp => {
                command.arg(directory.join(&release.manifest.entrypoint));
            }
        }
        command
            .current_dir(directory)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("PORT", address.port().to_string())
            .env("HOST", "127.0.0.1")
            .env("NODE_ENV", "production")
            .env(
                "BICDB_ENTRYPOINT",
                directory.join(&release.manifest.entrypoint),
            )
            .env("BICDB_DATABASE_URL", endpoint)
            .env("BICDB_DATABASE_TOKEN", token)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let parent_pid = std::process::id();
            unsafe {
                command.pre_exec(move || {
                    if libc::setpgid(0, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    #[cfg(target_os = "linux")]
                    {
                        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        if libc::getppid() != parent_pid as i32 {
                            return Err(std::io::Error::other("supervisor exited before launch"));
                        }
                    }
                    Ok(())
                });
            }
        }
        let child = command.spawn().context("start application runtime")?;
        Ok(Self { child, address })
    }

    pub fn exited(&mut self) -> Result<bool> {
        Ok(self.child.try_wait()?.is_some())
    }

    pub fn wait_ready(&mut self, release: &Release, token: &str) -> Result<()> {
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(250))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(10) {
            if self.exited()? {
                bail!("application exited before readiness; inspect application logs");
            }
            let path = if release.manifest.runtime == Runtime::Node {
                "/__bic/ready"
            } else {
                "/health"
            };
            if let Ok(response) = client
                .get(format!("http://{}{path}", self.address))
                .header("x-bic-internal-token", token)
                .send()
            {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(30));
        }
        bail!("application did not become ready within 10 seconds")
    }

    pub fn stop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn find_node(requested: Option<&Path>) -> Result<std::path::PathBuf> {
    if let Some(path) = requested {
        return validate_node(&path.canonicalize()?);
    }
    for directory in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let path = directory.join("node");
        if path.is_file() {
            return validate_node(&fs::canonicalize(path)?);
        }
    }
    bail!("Node.js 22 or newer is required; use --node to select an executable")
}

fn validate_node(path: &Path) -> Result<std::path::PathBuf> {
    let version = Command::new(path)
        .arg("--version")
        .env_clear()
        .output()
        .context("inspect Node.js version")?;
    let text = std::str::from_utf8(&version.stdout)?.trim();
    let major = text
        .strip_prefix('v')
        .and_then(|text| text.split('.').next())
        .and_then(|major| major.parse::<u32>().ok());
    anyhow::ensure!(
        version.status.success() && major.is_some_and(|major| major >= 22),
        "Node.js 22 or newer is required"
    );
    Ok(path.to_owned())
}

pub fn read_log(path: &Path) -> Result<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(64 * 1024)))?;
    let mut bytes = Vec::new();
    file.take(64 * 1024).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
