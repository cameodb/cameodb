//! One process per data directory.
//!
//! redb locks its own files, but only within one kernel: a Linux container on macOS or Windows
//! reaches a bind-mounted directory through a file-sharing layer that does not carry the lock,
//! WSL2 on `/mnt/c` is the same, and NFS depends on its configuration. Two nodes on one
//! directory across that boundary both open its stores — and a node opens every store before it
//! binds its port, so the second one fails on the bind only after it has opened, and possibly
//! committed to, both. Two writers with separate views of a redb file damage it.
//!
//! So every directory in `storage.data_paths` is claimed before anything in it is read, with two
//! files:
//!
//! - `cameodb.lock`, held with an exclusive OS lock (`flock` on Unix, `LockFileEx` on Windows)
//!   for the life of the process, and released by the OS however the process ends. It stops a
//!   second process on the same machine.
//! - `cameodb.owner`, written once the lock is held, naming who holds it: host, process, start
//!   time, version, command, and the node once its identity is read. Removed on a clean exit. It
//!   is what a process on the other side of a container boundary can see. A separate file
//!   because a `LockFileEx` lock is mandatory: on Windows nobody else could read the locked file.
//!
//! What a start decides, with no timer and no heartbeat:
//!
//! - the OS lock is held → refused, naming the owner: another process on this machine;
//! - no owner record → claimed;
//! - a record naming **this host** → its process is gone, or its OS lock would have stopped us →
//!   taken over, logged;
//! - a record naming **another host** → refused: the OS lock cannot see that process. A
//!   container's host name is its own, so a container and its host are two hosts. Once that
//!   process is known not to be running, `cameodb unlock <data path>` removes the record.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// The file held with the OS lock.
pub const LOCK_FILE: &str = "cameodb.lock";
/// The file that names the holder.
pub const OWNER_FILE: &str = "cameodb.owner";

/// Who holds a data directory: the content of `cameodb.owner`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Owner {
    pub host: String,
    pub pid: u32,
    /// RFC 3339, UTC.
    pub started: String,
    pub version: String,
    /// `node`, `compact`, …: what the process is doing with the directory.
    pub command: String,
    /// The node's name and id, once its identity has been read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

impl Owner {
    fn describe(&self) -> String {
        let what = match &self.node {
            Some(node) => format!("node {node}"),
            None => format!("`cameodb {}`", self.command),
        };
        format!(
            "{what}, process {} on host {}, started {}, version {}",
            self.pid, self.host, self.started, self.version
        )
    }
}

/// The name this machine goes by in an owner record: `gethostname` on Unix, the computer name
/// on Windows. A container has its own.
pub fn this_host() -> String {
    sysinfo::System::host_name()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// Every data directory of this process, claimed. Dropping it removes the owner records and
/// releases the OS locks.
#[derive(Debug)]
pub struct DataDirLock {
    held: Vec<Held>,
    owner: Owner,
}

#[derive(Debug)]
struct Held {
    dir: PathBuf,
    /// Kept open: the OS lock lives as long as this handle.
    _file: File,
}

impl DataDirLock {
    /// Claims every directory in `dirs` for `command`, creating any that does not exist yet.
    /// All or none: on a refusal, the directories already claimed are released.
    pub fn acquire(dirs: &[PathBuf], command: &str) -> Result<Self> {
        Self::acquire_as(dirs, command, this_host())
    }

    fn acquire_as(dirs: &[PathBuf], command: &str, host: String) -> Result<Self> {
        let owner = Owner {
            host,
            pid: std::process::id(),
            started: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            version: env!("CARGO_PKG_VERSION").to_string(),
            command: command.to_string(),
            node: None,
        };
        let mut lock = DataDirLock {
            held: Vec::with_capacity(dirs.len()),
            owner,
        };
        for dir in dirs {
            // A path listed twice is one directory, already ours.
            if lock.held.iter().any(|h| same_dir(&h.dir, dir)) {
                continue;
            }
            let file = claim(dir, &lock.owner)?;
            lock.held.push(Held {
                dir: dir.clone(),
                _file: file,
            });
        }
        Ok(lock)
    }

    /// Adds the node to every owner record, once its identity has been read — which happens
    /// only after the directory is claimed.
    pub fn record_node(&mut self, node: String) -> Result<()> {
        self.owner.node = Some(node);
        for held in &self.held {
            write_owner_replacing(&held.dir, &self.owner)?;
        }
        Ok(())
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        // The record first, while the OS lock still stops anyone from reading it as stale.
        for held in &self.held {
            let _ = std::fs::remove_file(held.dir.join(OWNER_FILE));
        }
    }
}

fn same_dir(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// Opens and locks `dir`'s lock file, and writes the owner record.
fn claim(dir: &Path, owner: &Owner) -> Result<File> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating the data directory {}", dir.display()))?;
    let lock_path = dir.join(LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("opening {}", lock_path.display()))?;

    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            let holder = match read_owner(dir) {
                Ok(Some(o)) => o.describe(),
                Ok(None) => "a process that has not written its owner record yet".to_string(),
                Err(e) => format!("a process whose owner record cannot be read ({e:#})"),
            };
            bail!(
                "{} is in use by another CameoDB process on this machine: {holder}.\n\
                 A data directory serves one process at a time; two writers damage its files. \
                 Stop that process first, or give this one a data directory of its own.",
                dir.display()
            );
        }
        // A filesystem without locks (some NFS mounts): the owner record still stops a second
        // process that can see it, so the claim goes on without the OS lock rather than
        // refusing a deployment that ran before.
        Err(std::fs::TryLockError::Error(e)) => warn!(
            path = %lock_path.display(),
            error = %e,
            "this filesystem does not lock files; the data directory is claimed by its owner \
             record alone, which a second process on this machine started at the same moment \
             could miss"
        ),
    }

    // Twice at most: a record found stale is removed, and the create below may then lose to a
    // process on another host that wrote one in between.
    for _ in 0..2 {
        match read_owner(dir) {
            Ok(None) => {}
            Ok(Some(found)) if found.host == owner.host => {
                // Same host, and the OS lock is ours: whoever wrote it has gone.
                warn!(
                    dir = %dir.display(),
                    previous = %found.describe(),
                    "taking over a data directory whose last owner on this host did not exit \
                     cleanly"
                );
                remove_owner(dir)?;
            }
            Ok(Some(found)) => bail!(
                "{} is claimed by {}.\n\
                 That host is not this one ({}), so this machine's file locks cannot tell \
                 whether that process is still running — a container is a host of its own, \
                 and a bind-mounted directory does not carry a lock across. A data directory \
                 serves one process at a time; two writers damage its files.\n\
                 Stop that process. If it is known not to be running — a container removed \
                 after a crash, say — remove its record with `cameodb unlock {}` and start \
                 again.",
                dir.display(),
                found.describe(),
                owner.host,
                dir.display(),
            ),
            Err(e) => bail!(
                "{} cannot be read: {e:#}.\n\
                 It names the process that holds this data directory. If no process does, \
                 remove it with `cameodb unlock {}`.",
                dir.join(OWNER_FILE).display(),
                dir.display()
            ),
        }
        match write_owner_new(dir, owner) {
            Ok(()) => return Ok(file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(anyhow::Error::new(e)
                    .context(format!("writing {}", dir.join(OWNER_FILE).display())));
            }
        }
    }
    bail!(
        "{} was claimed by another process while this one was claiming it",
        dir.display()
    )
}

/// The owner record of `dir`, if there is one.
pub fn read_owner(dir: &Path) -> Result<Option<Owner>> {
    let path = dir.join(OWNER_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("{} is not an owner record", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
    }
}

fn remove_owner(dir: &Path) -> Result<()> {
    match std::fs::remove_file(dir.join(OWNER_FILE)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            Err(anyhow::Error::new(e)
                .context(format!("removing {}", dir.join(OWNER_FILE).display())))
        }
    }
}

/// Creates the record only if there is none: of two processes on either side of a boundary,
/// one wins.
fn write_owner_new(dir: &Path, owner: &Owner) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(OWNER_FILE))?;
    file.write_all(&serde_json::to_vec_pretty(owner).map_err(std::io::Error::other)?)?;
    file.sync_all()
}

/// Replaces a record this process wrote, through a temporary file so it is never read half
/// written.
fn write_owner_replacing(dir: &Path, owner: &Owner) -> Result<()> {
    let tmp = dir.join(format!("{OWNER_FILE}.{}.tmp", std::process::id()));
    let path = dir.join(OWNER_FILE);
    let write = || -> std::io::Result<()> {
        let mut file = File::create(&tmp)?;
        file.write_all(&serde_json::to_vec_pretty(owner).map_err(std::io::Error::other)?)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)
    };
    write().with_context(|| format!("writing {}", path.display()))
}

/// `cameodb unlock <data path>...`: removes a stale owner record after showing it.
pub fn run_unlock(args: impl Iterator<Item = String>) -> Result<()> {
    const USAGE: &str = "\
cameodb unlock — remove the record that claims a data directory for a process that is gone

Usage:
  cameodb unlock <DATA PATH>...

A node, and `cameodb compact`, claim each data directory with cameodb.lock and cameodb.owner,
and remove the record when they exit cleanly. A process on this machine that ended any other
way is taken over by the next start on its own; a record left by another host — a container
removed after a crash, say — is not, because this machine cannot see whether that process is
still running.

Run this only once the process the record names is known not to be running. It shows the
record before removing it, and refuses while a process on this machine holds the directory.";

    let dirs: Vec<PathBuf> = args
        .filter(|a| {
            if a == "-h" || a == "--help" {
                println!("{USAGE}");
                std::process::exit(0);
            }
            true
        })
        .map(PathBuf::from)
        .collect();
    if dirs.is_empty() {
        bail!("{USAGE}");
    }
    for dir in &dirs {
        println!("{}", unlock(dir)?);
    }
    Ok(())
}

/// Removes `dir`'s owner record unless a process on this machine holds its lock; returns what
/// was done, for the operator.
pub fn unlock(dir: &Path) -> Result<String> {
    if !dir.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    let lock_path = dir.join(LOCK_FILE);
    if lock_path.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening {}", lock_path.display()))?;
        if let Err(std::fs::TryLockError::WouldBlock) = file.try_lock() {
            let holder = read_owner(dir)
                .ok()
                .flatten()
                .map(|o| o.describe())
                .unwrap_or_else(|| "a process whose record cannot be read".to_string());
            bail!(
                "{} is held by a running process on this machine: {holder}. Stop it; its \
                 record is removed when it exits.",
                dir.display()
            );
        }
        // Held until this returns, so nothing claims the directory between the check and the
        // removal; dropped with `file`.
        let shown = match read_owner(dir) {
            Ok(Some(o)) => format!("removed the record of {}", o.describe()),
            Ok(None) => return Ok(format!("{}: not claimed, nothing to do", dir.display())),
            Err(e) => format!("removed an unreadable record ({e:#})"),
        };
        remove_owner(dir)?;
        drop(file);
        return Ok(format!("{}: {shown}", dir.display()));
    }
    match read_owner(dir) {
        Ok(None) => Ok(format!("{}: not claimed, nothing to do", dir.display())),
        found => {
            let shown = match found {
                Ok(Some(o)) => format!("removed the record of {}", o.describe()),
                _ => "removed an unreadable record".to_string(),
            };
            remove_owner(dir)?;
            Ok(format!("{}: {shown}", dir.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner_on(host: &str) -> Owner {
        Owner {
            host: host.to_string(),
            pid: 4242,
            started: "2026-11-03T08:00:00Z".to_string(),
            version: "0.3.8".to_string(),
            command: "node".to_string(),
            node: Some("16IK (d73cad89-2730-5e93-9820-f97241614410)".to_string()),
        }
    }

    fn plant(dir: &Path, owner: &Owner) {
        std::fs::write(dir.join(OWNER_FILE), serde_json::to_vec(owner).unwrap()).unwrap();
    }

    #[test]
    fn a_held_directory_is_refused_naming_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = vec![dir.path().to_path_buf()];
        let mut first = DataDirLock::acquire_as(&dirs, "node", "here".into()).unwrap();
        first.record_node("16IK (d73c)".into()).unwrap();

        let refusal = format!(
            "{:#}",
            DataDirLock::acquire_as(&dirs, "node", "here".into()).unwrap_err()
        );
        assert!(refusal.contains("on this machine"), "{refusal}");
        assert!(refusal.contains("node 16IK (d73c)"), "{refusal}");
        assert!(
            refusal.contains(&format!("process {}", std::process::id())),
            "{refusal}"
        );

        drop(first);
        assert!(!dir.path().join(OWNER_FILE).exists(), "removed on release");
        DataDirLock::acquire_as(&dirs, "node", "here".into()).expect("free again");
    }

    #[test]
    fn a_record_from_another_host_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        plant(dir.path(), &owner_on("a1b2c3d4e5f6"));
        let refusal = format!(
            "{:#}",
            DataDirLock::acquire_as(&[dir.path().to_path_buf()], "node", "here".into())
                .unwrap_err()
        );
        assert!(refusal.contains("host a1b2c3d4e5f6"), "{refusal}");
        assert!(refusal.contains("process 4242"), "{refusal}");
        assert!(refusal.contains("2026-11-03T08:00:00Z"), "{refusal}");
        assert!(refusal.contains("cameodb unlock"), "{refusal}");
        assert_eq!(
            read_owner(dir.path()).unwrap(),
            Some(owner_on("a1b2c3d4e5f6")),
            "the record is left alone"
        );
    }

    #[test]
    fn a_stale_record_from_this_host_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        plant(dir.path(), &owner_on("here"));
        let lock =
            DataDirLock::acquire_as(&[dir.path().to_path_buf()], "node", "here".into()).unwrap();
        let now = read_owner(dir.path()).unwrap().unwrap();
        assert_eq!(now.pid, std::process::id());
        assert_eq!(now.node, None);
        drop(lock);
    }

    #[test]
    fn an_unreadable_record_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(OWNER_FILE), b"{ half").unwrap();
        let refusal = format!(
            "{:#}",
            DataDirLock::acquire_as(&[dir.path().to_path_buf()], "node", "here".into())
                .unwrap_err()
        );
        assert!(refusal.contains("cameodb unlock"), "{refusal}");
    }

    #[test]
    fn every_data_path_is_claimed_and_a_refusal_releases_the_rest() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let missing = first.path().join("created");
        let dirs = vec![
            first.path().to_path_buf(),
            second.path().to_path_buf(),
            missing.clone(),
            first.path().to_path_buf(),
        ];
        let lock = DataDirLock::acquire_as(&dirs, "node", "here".into()).unwrap();
        for dir in &dirs[..3] {
            assert!(dir.join(LOCK_FILE).is_file());
            assert!(read_owner(dir).unwrap().is_some());
        }
        // Each one is held, not only the first.
        assert!(
            DataDirLock::acquire_as(std::slice::from_ref(&missing), "compact", "here".into())
                .is_err()
        );
        drop(lock);

        // A refusal on the second directory leaves the first unclaimed.
        plant(second.path(), &owner_on("elsewhere"));
        assert!(DataDirLock::acquire_as(&dirs, "node", "here".into()).is_err());
        assert!(read_owner(first.path()).unwrap().is_none());
        DataDirLock::acquire_as(&[first.path().to_path_buf()], "node", "here".into())
            .expect("the first directory was released");
    }

    #[test]
    fn unlock_refuses_while_held_and_removes_a_stale_record() {
        let dir = tempfile::tempdir().unwrap();
        let lock =
            DataDirLock::acquire_as(&[dir.path().to_path_buf()], "node", "here".into()).unwrap();
        let refusal = format!("{:#}", unlock(dir.path()).unwrap_err());
        assert!(
            refusal.contains("running process on this machine"),
            "{refusal}"
        );
        assert!(read_owner(dir.path()).unwrap().is_some());
        drop(lock);

        plant(dir.path(), &owner_on("a1b2c3d4e5f6"));
        let done = unlock(dir.path()).unwrap();
        assert!(done.contains("host a1b2c3d4e5f6"), "{done}");
        assert!(read_owner(dir.path()).unwrap().is_none());
        assert!(unlock(dir.path()).unwrap().contains("nothing to do"));
        DataDirLock::acquire_as(&[dir.path().to_path_buf()], "node", "here".into())
            .expect("claimable after unlock");
    }
}
