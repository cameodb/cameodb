//! Reload the key ring when an included key or tenant file changes.
//!
//! The same reload SIGHUP and `POST /_admin/keys/reload` run, triggered by the file system
//! instead of by someone remembering to: a tenant file written by an operator, by a deploy, or
//! by the node itself takes effect without a second step. It is also the only reload Windows
//! has without HTTP, since it has no SIGHUP.
//!
//! The include folders are watched, not the files. An editor saves by writing a new file and
//! renaming it over the old one, a mounted secret is swapped through a symlink, and a tenant
//! file is written to a temp name and renamed into place — all of which are events on the
//! folder that a watch on the old file would miss. Any event there schedules one reload after
//! a short quiet period, so a burst of writes is one reload; the loader decides what is read.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::auth::KeyReloader;

/// How long the folders must be quiet before the reload runs.
const QUIET: Duration = Duration::from_millis(250);

/// Watch the include folders and reload on change, for as long as the node runs.
///
/// Does nothing for a config with no `include`. A watcher that cannot be created is a warning,
/// not a refusal to start: SIGHUP and the endpoint still reload.
pub fn spawn(keys: Arc<KeyReloader>) {
    if keys.include_folders().is_empty() {
        return;
    }
    let (events, mut changed) = mpsc::unbounded_channel::<()>();
    let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        // Access events are reads, the reload's own among them; anything else is a change.
        if event.is_ok_and(|event| !event.kind.is_access()) {
            let _ = events.send(());
        }
    });
    let mut watcher = match watcher {
        Ok(watcher) => watcher,
        Err(err) => {
            warn!(error = %err, "cannot watch the include folders; key files reload on SIGHUP or POST /_admin/keys/reload only");
            return;
        }
    };

    let mut watched: HashSet<PathBuf> = HashSet::new();
    rewatch(&mut watcher, &mut watched, &keys);

    tokio::spawn(async move {
        while changed.recv().await.is_some() {
            tokio::time::sleep(QUIET).await;
            while changed.try_recv().is_ok() {}

            match keys.reload() {
                Ok(report) => info!(keys = %report.summary, "Key file changed: key ring reloaded"),
                Err(err) => warn!("Key file changed, reload refused: {err:#}"),
            }
            // A reload may have changed `include` itself.
            rewatch(&mut watcher, &mut watched, &keys);
        }
    });
}

/// Bring the watched set in line with the folders the last successful load read from.
fn rewatch(watcher: &mut impl Watcher, watched: &mut HashSet<PathBuf>, keys: &KeyReloader) {
    let wanted: HashSet<PathBuf> = keys.include_folders().iter().cloned().collect();
    for folder in watched.difference(&wanted) {
        let _ = watcher.unwatch(folder);
    }
    watched.retain(|folder| wanted.contains(folder));
    for folder in wanted {
        if watched.contains(&folder) {
            continue;
        }
        match watcher.watch(&folder, RecursiveMode::NonRecursive) {
            Ok(()) => {
                info!(folder = %folder.display(), "Watching for key file changes");
                watched.insert(folder);
            }
            Err(err) => {
                warn!(folder = %folder.display(), error = %err, "cannot watch an include folder")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ApiKey;

    /// Poll `check` until it holds, for up to ten seconds: file system events arrive when the
    /// platform delivers them, and FSEvents batches.
    async fn eventually(mut check: impl FnMut() -> bool) -> bool {
        for _ in 0..100 {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    fn key_file(key: &ApiKey, role: &str) -> String {
        format!(
            "[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"{role}\"\n",
            key.digest().to_config_value()
        )
    }

    #[tokio::test]
    async fn a_key_file_added_changed_and_removed_reloads_the_ring() {
        let dir = tempfile::tempdir().unwrap();
        // Canonical, since FSEvents reports /private/var for a /var temp dir.
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("keys.d")).unwrap();
        let admin = ApiKey::generate().unwrap();
        let config = root.join("cameodb.toml");
        std::fs::write(
            &config,
            format!(
                "include = [\"keys.d/*.toml\"]\n[node]\nprofile = \"local\"\n\
                 [security]\nenabled = true\n{}",
                key_file(&admin, "admin")
            ),
        )
        .unwrap();
        let cli = crate::config::CliOverrides::parse([
            "--config".to_string(),
            config.to_string_lossy().to_string(),
        ])
        .unwrap();
        let security = crate::config::CameoDbConfig::load_with_cli(&cli)
            .unwrap()
            .security;
        let keys = Arc::new(KeyReloader::new(cli, security).unwrap());
        spawn(Arc::clone(&keys));

        let jane = ApiKey::generate().unwrap();
        let file = root.join("keys.d/acme.toml");
        // Written as the node will write it: a temp file renamed into place.
        let temp = root.join("keys.d/.acme.toml.tmp");
        std::fs::write(&temp, key_file(&jane, "reader")).unwrap();
        std::fs::rename(&temp, &file).unwrap();
        assert!(
            eventually(|| keys.current().authenticate(jane.expose()).is_some()).await,
            "an added key file was not picked up"
        );

        std::fs::write(&file, key_file(&jane, "writer")).unwrap();
        assert!(
            eventually(|| keys
                .current()
                .authenticate(jane.expose())
                .is_some_and(|entry| entry.role() == crate::auth::Role::Writer))
            .await,
            "a changed key file was not picked up"
        );

        // A broken file is refused and the ring stays as it was.
        std::fs::write(root.join("keys.d/broken.toml"), "not toml at all").unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(keys.current().authenticate(jane.expose()).is_some());
        std::fs::remove_file(root.join("keys.d/broken.toml")).unwrap();

        std::fs::remove_file(&file).unwrap();
        assert!(
            eventually(|| keys.current().authenticate(jane.expose()).is_none()).await,
            "a removed key file was not picked up"
        );
        assert!(keys.current().authenticate(admin.expose()).is_some());
    }
}
