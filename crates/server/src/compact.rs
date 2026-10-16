//! `cameodb compact`: return the space redb holds free inside its files to the filesystem.
//!
//! redb never shrinks a file on its own. A delete frees pages inside it, and later writes reuse
//! them, but the file stays at its high-water mark; only `Database::compact` moves the live pages
//! down and truncates the rest. That needs the database to itself — no reader, no writer — and a
//! running node shares every shard's store between its writer thread and the search workers, so
//! this runs with the node stopped. redb locks its files, so a running node is not a hazard: the
//! open fails and the command says why.
//!
//! Measured on 1 GB shards with 75% of their documents deleted: 1,028 MB → 310 MB in about 1.5 s
//! each. Compaction rewrites the file in place, so it needs no free disk beyond what it frees.
//! Tantivy's index files are not touched here; their deleted documents go when segments merge.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::config::{self, CameoDbConfig};

/// What compacting one file did, or under `--dry-run` would do.
#[derive(Debug)]
pub(crate) struct Outcome {
    pub before: u64,
    /// The file's size afterwards; under `--dry-run`, about the size compaction would leave.
    pub after: u64,
    /// redb repaired the file before it opened: the node did not close it cleanly.
    pub repaired: bool,
    pub elapsed: Duration,
}

/// Run the subcommand. `args` is everything after `compact`.
pub fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let mut dry_run = false;
    // Taken off before the override parser sees them, which rejects a flag it does not know.
    let args: Vec<String> = args
        .filter(|arg| {
            if arg == "--dry-run" {
                dry_run = true;
                false
            } else {
                true
            }
        })
        .collect();
    let overrides = config::CliOverrides::parse(args)?;
    let config = CameoDbConfig::load_unvalidated(&overrides)?;

    let files = redb_files(&config);
    if files.is_empty() {
        bail!(
            "no redb files under {:?}: is this the data path the node runs with?",
            config.storage.data_paths
        );
    }

    println!(
        "{} {} redb file(s){}",
        if dry_run { "Measuring" } else { "Compacting" },
        files.len(),
        if dry_run {
            " (dry run: nothing is changed)"
        } else {
            ""
        }
    );
    let (mut before, mut after, mut failed) = (0u64, 0u64, 0usize);
    for path in &files {
        match compact_file(path, dry_run) {
            Ok(o) => {
                before += o.before;
                after += o.after;
                println!(
                    "  {}  {} -> {}{}  ({:.1?}{})",
                    path.display(),
                    mb(o.before),
                    if dry_run { "~" } else { "" },
                    mb(o.after),
                    o.elapsed,
                    if o.repaired {
                        ", repaired first: the node did not close it cleanly"
                    } else {
                        ""
                    }
                );
            }
            // Every file belongs to the same node, so one that is locked means they all are.
            Err(e) if is_already_open(&e) => bail!(
                "{} is open in another process: stop the node first, then run compact",
                path.display()
            ),
            Err(e) => {
                failed += 1;
                println!("  {}  FAILED: {:#}", path.display(), e);
            }
        }
    }
    println!(
        "{} {} -> {}: {} {}",
        if dry_run { "Would take" } else { "Took" },
        mb(before),
        if dry_run {
            format!("about {}", mb(after))
        } else {
            mb(after)
        },
        mb(before.saturating_sub(after)),
        if dry_run { "reclaimable" } else { "reclaimed" }
    );
    if failed > 0 {
        bail!("{failed} file(s) failed");
    }
    Ok(())
}

/// Every redb file a node keeps under its data paths: each shard's `store.redb`, and the
/// cluster metadata the primary path holds. Sorted, so a run reads the same way twice.
pub(crate) fn redb_files(config: &CameoDbConfig) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for base in &config.storage.data_paths {
        let Ok(entries) = std::fs::read_dir(base) else {
            continue;
        };
        let mut shards: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix("shard-"))
                    .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
            })
            .map(|p| p.join("store.redb"))
            .filter(|p| p.is_file())
            .collect();
        shards.sort();
        files.extend(shards);
    }
    if let Some(primary) = config.storage.primary_path() {
        let metadata = primary.join("metadata.redb");
        if metadata.is_file() {
            files.push(metadata);
        }
    }
    files
}

/// Compact one file, or under `dry_run` measure what compacting it would free.
///
/// The measurement is the allocated size, which compaction ends the file at: it moves every
/// allocated page below the first free one and truncates the rest. It is close rather than
/// exact, either way: compaction first frees pages still pending release, which a measurement
/// taken without committing counts as allocated, and its own commits allocate a little. Within
/// 1% on 1 GB shards here, and a few hundred KB on a file under 1 MB.
pub(crate) fn compact_file(path: &Path, dry_run: bool) -> Result<Outcome> {
    let started = Instant::now();
    let before = std::fs::metadata(path)?.len();

    // A file the node did not close cleanly is repaired before `open` returns, and without a
    // recorded allocator state that walks the whole file. Say so, rather than appear to hang.
    let repaired = Arc::new(AtomicBool::new(false));
    let mut builder = redb::Builder::new();
    builder.set_repair_callback({
        let repaired = Arc::clone(&repaired);
        let shown = path.display().to_string();
        move |session| {
            if !repaired.swap(true, Ordering::Relaxed) {
                println!("  {shown}: not closed cleanly, repairing before it can be compacted");
            }
            let _ = session.progress();
        }
    });
    let mut db = builder.open(path)?;

    let after = if dry_run {
        let txn = db.begin_write()?;
        let stats = txn.stats()?;
        txn.abort()?;
        (stats.allocated_pages() * stats.page_size() as u64).min(before)
    } else {
        // `compact` returns whether it moved anything; one pass normally finishes, but a pass
        // can leave work behind, and it is the call that says when there is none.
        while db
            .compact()
            .with_context(|| format!("compacting {}", path.display()))?
        {}
        drop(db);
        std::fs::metadata(path)?.len()
    };

    Ok(Outcome {
        before,
        after,
        repaired: repaired.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
    })
}

fn is_already_open(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<redb::DatabaseError>(),
        Some(redb::DatabaseError::DatabaseAlreadyOpen)
    )
}

fn mb(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};

    const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");

    /// A redb file of `n` 1 KB rows with every row not divisible by 4 deleted: the shape a
    /// mass delete leaves, live pages scattered through a file at its high-water mark.
    fn file_after_a_mass_delete(dir: &Path, n: u64) -> PathBuf {
        let path = dir.join("store.redb");
        let db = redb::Database::create(&path).unwrap();
        let body = vec![7u8; 1024];
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(ROWS).unwrap();
            for i in 0..n {
                t.insert(i, body.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(ROWS).unwrap();
            t.retain(|k, _| k % 4 == 0).unwrap();
        }
        txn.commit().unwrap();
        path
    }

    fn rows(path: &Path) -> u64 {
        let db = redb::Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        txn.open_table(ROWS).unwrap().len().unwrap()
    }

    #[test]
    fn compaction_shrinks_a_file_to_what_the_dry_run_said_and_keeps_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = file_after_a_mass_delete(dir.path(), 20_000);

        let measured = compact_file(&path, true).unwrap();
        assert!(
            measured.after < measured.before / 2,
            "three quarters of the rows are gone, so most of the file is free: {measured:?}"
        );

        let done = compact_file(&path, false).unwrap();
        assert_eq!(done.before, measured.before);
        assert!(
            done.after.abs_diff(measured.after) <= measured.after / 20,
            "the dry run is within a few percent of the size compaction leaves: \
             measured {measured:?}, compacted {done:?}"
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), done.after);
        assert_eq!(rows(&path), 5_000);
        assert!(!done.repaired);
    }

    #[test]
    fn a_dry_run_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = file_after_a_mass_delete(dir.path(), 20_000);
        let size = std::fs::metadata(&path).unwrap().len();

        compact_file(&path, true).unwrap();

        assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
        assert_eq!(rows(&path), 5_000);
    }

    #[test]
    fn a_file_another_handle_holds_is_reported_as_open_and_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = file_after_a_mass_delete(dir.path(), 1_000);
        let size = std::fs::metadata(&path).unwrap().len();
        let _node = redb::Database::open(&path).unwrap();

        let err = compact_file(&path, false).unwrap_err();

        assert!(is_already_open(&err), "{err:#}");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
    }

    #[test]
    fn the_files_are_every_shard_store_and_the_metadata_and_nothing_else() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let shard = |base: &Path, id: &str| {
            let dir = base.join(format!("shard-{id}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("store.redb"), b"").unwrap();
            dir.join("store.redb")
        };
        let a = shard(first.path(), "2969b294-be91-4052-9fde-2639c199ec40");
        let b = shard(second.path(), "5f725b24-f2bf-4460-988d-c3fe70c11ffc");
        // Not a shard: the name has no uuid, so the node would not open it either.
        shard(first.path(), "scratch");
        // A shard directory with no store yet.
        std::fs::create_dir_all(
            first
                .path()
                .join("shard-80027ede-d143-4f70-912a-862dc7c8c69b"),
        )
        .unwrap();
        std::fs::write(first.path().join("metadata.redb"), b"").unwrap();
        // Metadata lives on the primary path only.
        std::fs::write(second.path().join("metadata.redb"), b"").unwrap();

        let mut config = CameoDbConfig::default();
        config.storage.data_paths = vec![first.path().to_path_buf(), second.path().to_path_buf()];

        assert_eq!(
            redb_files(&config),
            vec![a, b, first.path().join("metadata.redb")]
        );
    }
}
