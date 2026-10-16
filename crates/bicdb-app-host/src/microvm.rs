//! Optional Linux microVM execution. Images, network allocations and host
//! privilege are operator-owned; application packages cannot select host paths.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    manifest::{valid_name, Release},
    runtime::{spawn_supervised, Process},
    store::{atomic_write, read_bounded, write_synced},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format: u32,
    pub firecracker: PathBuf,
    pub firecracker_sha256: String,
    pub jailer: PathBuf,
    pub jailer_sha256: String,
    pub kernel: PathBuf,
    pub kernel_sha256: String,
    pub rootfs: PathBuf,
    pub rootfs_sha256: String,
    pub allocations: BTreeMap<String, Allocation>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Allocation {
    pub host_ip: Ipv4Addr,
    pub guest_ip: Ipv4Addr,
    pub api_port: u16,
    pub uid: u32,
    pub gid: u32,
    #[serde(default = "one_cpu")]
    pub vcpus: u8,
}
fn one_cpu() -> u8 {
    1
}

pub struct Host {
    config: Config,
    _networks: Vec<Network>,
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher
        .finish()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
fn verify_file(path: &Path, digest: &str) -> Result<()> {
    ensure!(
        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid microVM asset digest"
    );
    ensure!(
        hash_file(path)? == digest.to_ascii_lowercase(),
        "microVM asset digest mismatch: {}",
        path.display()
    );
    Ok(())
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::metadata(path)?;
        ensure!(
            metadata.uid() == 0 && metadata.mode() & 0o022 == 0,
            "microVM host configuration must be root-owned and not writable by other users"
        );
        let mut config: Self = serde_json::from_slice(&read_bounded(path, 1024 * 1024)?)?;
        let base = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        for path in [
            &mut config.firecracker,
            &mut config.jailer,
            &mut config.kernel,
            &mut config.rootfs,
        ] {
            if path.is_relative() {
                *path = base.join(&*path);
            }
            *path = path.canonicalize()?;
        }
        config.validate()?;
        Ok(config)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.format == 1 && !self.allocations.is_empty() && self.allocations.len() <= 128,
            "invalid microVM host configuration"
        );
        let mut networks = BTreeSet::new();
        let mut users = BTreeSet::new();
        let mut groups = BTreeSet::new();
        for (name, allocation) in &self.allocations {
            ensure!(valid_name(name), "invalid microVM application name");
            let host = u32::from(allocation.host_ip);
            let guest = u32::from(allocation.guest_ip);
            ensure!(
                allocation.host_ip.is_private()
                    && allocation.guest_ip.is_private()
                    && host & !3 == guest & !3
                    && host != guest
                    && (1..=2).contains(&(host & 3))
                    && (1..=2).contains(&(guest & 3)),
                "microVM addresses must be distinct private hosts in the same /30"
            );
            ensure!(
                networks.insert(host & !3),
                "microVM network allocations overlap"
            );
            ensure!(
                allocation.api_port >= 1024
                    && allocation.uid >= 10_000
                    && allocation.gid >= 10_000
                    && users.insert(allocation.uid)
                    && groups.insert(allocation.gid),
                "microVMs require distinct unprivileged host UIDs and an unprivileged API port"
            );
            ensure!(
                (1..=8).contains(&allocation.vcpus),
                "microVM vCPU count outside supported range"
            );
        }
        for (path, digest) in [
            (&self.firecracker, &self.firecracker_sha256),
            (&self.jailer, &self.jailer_sha256),
            (&self.kernel, &self.kernel_sha256),
            (&self.rootfs, &self.rootfs_sha256),
        ] {
            verify_file(path, digest)?;
        }
        Ok(())
    }
}

impl Host {
    pub fn open(path: &Path, platform_root: &Path) -> Result<Self> {
        ensure!(
            cfg!(target_os = "linux"),
            "microVM execution requires Linux"
        );
        let config = Config::read(path)?;
        ensure!(unsafe { libc::geteuid() } == 0, "the microVM host currently requires a privileged supervisor for jailer and network setup");
        fence_previous(platform_root)?;
        let mut networks = Vec::new();
        for (name, allocation) in &config.allocations {
            networks.push(Network::prepare(platform_root, name, allocation)?);
        }
        Ok(Self {
            config,
            _networks: networks,
        })
    }

    pub fn allocations(&self) -> &BTreeMap<String, Allocation> {
        &self.config.allocations
    }

    pub fn require_application(&self, name: &str) -> Result<()> {
        ensure!(
            self.config.allocations.contains_key(name),
            "no operator-provisioned microVM allocation for application {name}"
        );
        Ok(())
    }

    pub fn launch(&self, release: &Release, app_dir: &Path, token: &str) -> Result<Process> {
        let name = &release.manifest.name;
        let allocation = self
            .config
            .allocations
            .get(name)
            .context("missing microVM allocation")?;
        self.config.validate()?;
        let state = app_dir.join("microvm-process.json");
        cleanup_previous(&state, &app_dir.join("vm-jails"))?;
        let id = format!("vm-{}", Uuid::new_v4().simple());
        let base = app_dir.join("vm-jails");
        let executable_name = self
            .config
            .firecracker
            .file_name()
            .context("VMM filename")?;
        let jail = base.join(executable_name).join(&id).join("root");
        fs::create_dir_all(&jail)?;
        let mut resources = Resources {
            jail_parent: jail.parent().context("jail parent")?.to_owned(),
            state,
            pid: None,
        };
        for (source, target) in [
            (&self.config.kernel, "kernel"),
            (&self.config.rootfs, "rootfs.ext4"),
        ] {
            fs::copy(source, jail.join(target))?;
            fs::set_permissions(
                jail.join(target),
                std::os::unix::fs::PermissionsExt::from_mode(0o444),
            )?;
        }
        verify_file(&jail.join("kernel"), &self.config.kernel_sha256)?;
        verify_file(&jail.join("rootfs.ext4"), &self.config.rootfs_sha256)?;
        let package = jail.join("package");
        fs::create_dir(&package)?;
        for path in release.files.keys() {
            let target = package.join("release").join(path);
            fs::create_dir_all(target.parent().context("package directory")?)?;
            write_synced(&target, &release.decode(path)?)?;
            fs::set_permissions(&target, std::os::unix::fs::PermissionsExt::from_mode(0o555))?;
        }
        write_synced(&package.join("adapter.mjs"), include_bytes!("adapter.mjs"))?;
        write_synced(
            &package.join("guest-launch.mjs"),
            include_bytes!("guest-launch.mjs"),
        )?;
        write_synced(
            &package.join("launch.json"),
            &serde_json::to_vec(&json!({
                "runtime":release.manifest.runtime, "entrypoint":release.manifest.entrypoint,
                "heap_mb":release.manifest.resources.memory_mb.saturating_sub(64).max(16),
                "database_url":format!("http://{}:{}/internal/{name}", allocation.host_ip, allocation.api_port),
                "database_token":token,
            }))?,
        )?;
        readable_tree(&package)?;
        let disk = jail.join("package.ext4");
        File::create(&disk)?.set_len(96 * 1024 * 1024)?;
        run(host_command("mkfs.ext4")
            .args(["-q", "-F", "-d"])
            .arg(&package)
            .arg(&disk))?;
        fs::set_permissions(&disk, std::os::unix::fs::PermissionsExt::from_mode(0o444))?;
        fs::remove_dir_all(&package)?;
        let tap = network_name(
            app_dir
                .parent()
                .and_then(Path::parent)
                .context("platform root")?,
            name,
        );
        // The network name is derived from the canonical platform root, not a
        // caller-supplied name or a value chosen by the application artifact.
        let boot = format!("console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda ro init=/sbin/bic-init ip={}::{}:255.255.255.252:bic:eth0:off", allocation.guest_ip, allocation.host_ip);
        let mac = format!(
            "02:{}:{}:{}:{}:{}",
            &tap[3..5],
            &tap[5..7],
            &tap[7..9],
            &tap[9..11],
            &tap[11..13]
        );
        let cgroup_parent = format!("bic-platform-{tap}");
        let config = json!({
            "entropy":{"rate_limiter":{"bandwidth":{"size":65536,"refill_time":1000}}},
            "boot-source":{"kernel_image_path":"/kernel","boot_args":boot},
            "drives":[
                {"drive_id":"rootfs","path_on_host":"/rootfs.ext4","is_root_device":true,"is_read_only":true},
                {"drive_id":"package","path_on_host":"/package.ext4","is_root_device":false,"is_read_only":true}
            ],
            "machine-config":{"vcpu_count":allocation.vcpus,"mem_size_mib":release.manifest.resources.memory_mb.max(128),"smt":false},
            "network-interfaces":[{"iface_id":"eth0","host_dev_name":tap,"guest_mac":mac}]
        });
        write_synced(&jail.join("vm.json"), &serde_json::to_vec(&config)?)?;
        fs::set_permissions(
            jail.join("vm.json"),
            std::os::unix::fs::PermissionsExt::from_mode(0o444),
        )?;
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(app_dir.join("application.log"))?;
        let arguments = vec![
            "--id".into(),
            id,
            "--exec-file".into(),
            self.config.firecracker.to_string_lossy().into_owned(),
            "--uid".into(),
            allocation.uid.to_string(),
            "--gid".into(),
            allocation.gid.to_string(),
            "--cgroup-version".into(),
            "2".into(),
            "--parent-cgroup".into(),
            cgroup_parent,
            "--chroot-base-dir".into(),
            base.to_string_lossy().into_owned(),
            "--".into(),
            "--no-api".into(),
            "--config-file".into(),
            "/vm.json".into(),
        ];
        let specification = jail.join("executor.json");
        write_synced(
            &specification,
            &serde_json::to_vec(&Executor {
                program: self.config.jailer.clone(),
                arguments,
            })?,
        )?;
        let mut command = Command::new("/proc/self/exe");
        command
            .arg("vm-execute")
            .arg(&specification)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .env_clear();
        let client = crate::runtime::upstream_client()?;
        let mut child = spawn_supervised(&mut command)?;
        let pid = child.id();
        let admitted = (|| -> Result<()> {
            let record = ProcessRecord {
                pid,
                start_time: process_start(pid).context("read microVM process identity")?,
                boot_id: boot_id()?,
                jail_parent: resources.jail_parent.clone(),
            };
            atomic_write(&resources.state, &serde_json::to_vec(&record)?)?;
            resources.pid = Some(pid);
            child
                .stdin
                .take()
                .context("microVM launch barrier")?
                .write_all(&[1])?;
            Ok(())
        })();
        if let Err(error) = admitted {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Ok(Process::from_microvm(
            child,
            SocketAddr::from((allocation.guest_ip, 8080)),
            resources,
            client,
        ))
    }
}

fn readable_tree(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            readable_tree(&path)?;
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(0o555))?;
        }
    }
    Ok(())
}

fn host_command(name: &str) -> Command {
    let path = ["/usr/sbin", "/usr/bin", "/sbin", "/bin"]
        .iter()
        .map(|directory| Path::new(directory).join(name))
        .find(|path| path.is_file())
        .unwrap_or_else(|| Path::new("/usr/sbin").join(name));
    Command::new(path)
}

fn run(command: &mut Command) -> Result<()> {
    let output = command.output().context("run microVM host tool")?;
    ensure!(
        output.status.success(),
        "microVM host tool failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}
fn succeeds(program: &str, arguments: &[&str]) -> bool {
    host_command(program)
        .args(arguments)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}
fn network_name(root: &Path, name: &str) -> String {
    format!(
        "bic{}",
        &format!(
            "{:x}",
            Sha256::digest(format!("{}:{name}", root.display()).as_bytes())
        )[..11]
    )
}
fn boot_id() -> Result<String> {
    Ok(fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .into())
}
fn process_start(pid: u32) -> Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    Ok(stat
        .rsplit_once(") ")
        .context("invalid process metadata")?
        .1
        .split_whitespace()
        .nth(19)
        .context("missing process start time")?
        .parse()?)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessRecord {
    pid: u32,
    start_time: u64,
    boot_id: String,
    jail_parent: PathBuf,
}

fn process_is_fenced(pid: u32, expected_start: u64) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return false;
    };
    let parts: Vec<_> = fields.split_whitespace().collect();
    parts.first() == Some(&"Z")
        || parts
            .get(19)
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|start| start != expected_start)
}

fn cleanup_previous(state: &Path, base: &Path) -> Result<()> {
    if !state.exists() {
        return Ok(());
    }
    let record: ProcessRecord = serde_json::from_slice(&read_bounded(state, 16384)?)?;
    ensure!(
        record.jail_parent.starts_with(base)
            && !record
                .jail_parent
                .components()
                .any(|p| matches!(p, std::path::Component::ParentDir)),
        "invalid microVM process record"
    );
    if record.boot_id == boot_id()?
        && process_start(record.pid).is_ok_and(|start| start == record.start_time)
    {
        ensure!(
            unsafe { libc::kill(-(record.pid as i32), libc::SIGKILL) } == 0
                || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
            "cannot fence previous microVM process"
        );
        // A process retaining the same identity must disappear before its host
        // files or network allocation can be reused.
        for _ in 0..100 {
            if process_is_fenced(record.pid, record.start_time) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        ensure!(
            process_is_fenced(record.pid, record.start_time),
            "previous microVM has not exited"
        );
    }
    if record.jail_parent.exists() {
        fs::remove_dir_all(record.jail_parent)?;
    }
    fs::remove_file(state)?;
    Ok(())
}

/// Fence every recorded VMM, including applications whose allocation has been
/// removed from the new host configuration. Call only while owning Store's lock.
pub(crate) fn fence_previous(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root.join("apps"))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        ensure!(
            name.to_str().is_some_and(valid_name),
            "invalid application directory"
        );
        let directory = entry.path();
        cleanup_previous(
            &directory.join("microvm-process.json"),
            &directory.join("vm-jails"),
        )?;
    }
    Ok(())
}

pub(crate) struct Resources {
    jail_parent: PathBuf,
    state: PathBuf,
    pid: Option<u32>,
}
impl Drop for Resources {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            if let Ok(bytes) = read_bounded(&self.state, 16384) {
                if serde_json::from_slice::<ProcessRecord>(&bytes).is_ok_and(|r| r.pid == pid) {
                    let _ = fs::remove_file(&self.state);
                }
            }
        }
        let _ = fs::remove_dir_all(&self.jail_parent);
    }
}

#[derive(Serialize, Deserialize)]
struct NetworkRecord {
    interface: String,
    ifindex: String,
    boot_id: String,
    allocation: Allocation,
}
struct Network {
    interface: String,
    chain: String,
    owned: bool,
    ifindex: Option<String>,
}
impl Network {
    fn prepare(root: &Path, name: &str, allocation: &Allocation) -> Result<Self> {
        let interface = network_name(root, name);
        let chain = format!("BIC_{}", &interface[3..]);
        let directory = root.join("microvm-networks");
        fs::create_dir_all(&directory)?;
        let record_path = directory.join(format!("{name}.json"));
        let ifindex_path = PathBuf::from(format!("/sys/class/net/{interface}/ifindex"));
        let mut network = Self {
            interface: interface.clone(),
            chain: chain.clone(),
            owned: false,
            ifindex: None,
        };
        if ifindex_path.exists() {
            let previous: NetworkRecord = serde_json::from_slice(
                &read_bounded(&record_path, 16384)
                    .context("existing network interface has no platform ownership record")?,
            )?;
            ensure!(
                previous.interface == interface
                    && previous.ifindex.trim() == fs::read_to_string(&ifindex_path)?.trim()
                    && previous.boot_id == boot_id()?
                    && previous.allocation == *allocation,
                "existing network interface does not match its ownership record"
            );
            network.owned = true;
        } else {
            run(host_command("ip").args([
                "tuntap",
                "add",
                "dev",
                &interface,
                "mode",
                "tap",
                "user",
                &allocation.uid.to_string(),
                "group",
                &allocation.gid.to_string(),
            ]))?;
            network.owned = true;
        }
        network.ifindex = Some(fs::read_to_string(&ifindex_path)?.trim().into());
        run(host_command("ip").args([
            "address",
            "replace",
            &format!("{}/30", allocation.host_ip),
            "dev",
            &interface,
        ]))?;
        run(host_command("ip").args(["link", "set", "dev", &interface, "up"]))?;
        if !succeeds("iptables", &["-w", "2", "-L", &chain]) {
            run(host_command("iptables").args(["-w", "2", "-N", &chain]))?;
        }
        run(host_command("iptables").args(["-w", "2", "-F", &chain]))?;
        run(host_command("iptables").args([
            "-w",
            "2",
            "-A",
            &chain,
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ]))?;
        run(host_command("iptables").args([
            "-w",
            "2",
            "-A",
            &chain,
            "-d",
            &allocation.host_ip.to_string(),
            "-p",
            "tcp",
            "--dport",
            &allocation.api_port.to_string(),
            "-j",
            "ACCEPT",
        ]))?;
        run(host_command("iptables").args(["-w", "2", "-A", &chain, "-j", "DROP"]))?;
        for (program, table, direction, target) in [
            ("iptables", "INPUT", "-i", chain.as_str()),
            ("iptables", "FORWARD", "-i", "DROP"),
            ("iptables", "FORWARD", "-o", "DROP"),
            ("ip6tables", "INPUT", "-i", "DROP"),
            ("ip6tables", "FORWARD", "-i", "DROP"),
            ("ip6tables", "FORWARD", "-o", "DROP"),
        ] {
            if !succeeds(
                program,
                &["-w", "2", "-C", table, direction, &interface, "-j", target],
            ) {
                run(host_command(program).args([
                    "-w", "2", "-I", table, "1", direction, &interface, "-j", target,
                ]))?;
            }
        }
        let record = NetworkRecord {
            interface,
            ifindex: fs::read_to_string(ifindex_path)?.trim().into(),
            boot_id: boot_id()?,
            allocation: allocation.clone(),
        };
        atomic_write(&record_path, &serde_json::to_vec(&record)?)?;
        Ok(network)
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        if self.ifindex.as_ref().is_some_and(|expected| {
            fs::read_to_string(format!("/sys/class/net/{}/ifindex", self.interface))
                .is_ok_and(|actual| actual.trim() != expected)
        }) {
            return;
        }
        for (program, table, direction, target) in [
            ("iptables", "INPUT", "-i", self.chain.as_str()),
            ("iptables", "FORWARD", "-i", "DROP"),
            ("iptables", "FORWARD", "-o", "DROP"),
            ("ip6tables", "INPUT", "-i", "DROP"),
            ("ip6tables", "FORWARD", "-i", "DROP"),
            ("ip6tables", "FORWARD", "-o", "DROP"),
        ] {
            let _ = host_command(program)
                .args([
                    "-w",
                    "2",
                    "-D",
                    table,
                    direction,
                    &self.interface,
                    "-j",
                    target,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = host_command("iptables")
            .args(["-w", "2", "-F", &self.chain])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = host_command("iptables")
            .args(["-w", "2", "-X", &self.chain])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = host_command("ip")
            .args(["link", "delete", "dev", &self.interface])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Executor {
    program: PathBuf,
    arguments: Vec<String>,
}

/// Internal launch barrier. The supervisor durably records the process identity
/// before allowing jailer to drop privileges (which clears Linux PDEATHSIG).
pub fn execute(specification: &Path) -> Result<()> {
    use std::os::unix::process::CommandExt;
    let mut permit = [0u8; 1];
    std::io::stdin().read_exact(&mut permit)?;
    ensure!(permit == [1], "invalid microVM launch permit");
    let executor: Executor = serde_json::from_slice(&read_bounded(specification, 64 * 1024)?)?;
    let error = Command::new(executor.program)
        .args(executor.arguments)
        .env_clear()
        .stdin(Stdio::null())
        .exec();
    Err(error.into())
}
