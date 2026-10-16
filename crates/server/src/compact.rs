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
//!
//! Inside one file redb works in rounds: each moves up to a million of the highest pages into the
//! lowest free ones, commits durably, and truncates the freed tail, so the file shrinks as it
//! goes and an interruption loses only the round in progress. Nothing is lost either way, and a
//! second run picks up from the file as it is. Its commits record no allocator state, so a file
//! interrupted mid-compaction is repaired in full the next time it opens.
//! Tantivy's index files are not touched here; their deleted documents go when segments merge.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::config::{self, CameoDbConfig};

/// redb's page cache for each file being compacted.
///
/// redb's own default is 1 GB, which made memory the limit on running files in parallel: four
/// at once peaked at 4.6 GB. Measured on four 1.4 GB shards, all at once: 1 GB of cache took
/// 13.6 s, 256 MB 17.1 s at 1.5 GB peak, and 64 MB or 16 MB no slower than 256 MB. The large
/// cache only helped because those files nearly fit in it; production shards do not.
const CACHE_PER_FILE: usize = 256 * 1024 * 1024;

/// What compacting one file did, or under `--dry-run` would do.
#[derive(Debug)]
pub(crate) struct Outcome {
    pub before: u64,
    /// The file's size afterwards; under `--dry-run`, about the size compaction would leave.
    pub after: u64,
    /// redb repaired the file before it opened: whatever last had it open, the node or an
    /// interrupted compaction, did not close it cleanly.
    pub repaired: bool,
    pub elapsed: Duration,
}

/// Run the subcommand. `args` is everything after `compact`.
///
/// Every file is compacted at once unless `--jobs N` says otherwise. Compaction is bound by the
/// disk, not the CPU — four 1.4 GB shards at once kept under two cores busy — and memory is
/// bounded per file by [`CACHE_PER_FILE`], so the disk is the only reason to hold back: `--jobs`
/// is for a spinning disk, or a server busy with other work.
///
/// Ctrl+C stops it cleanly: no file is started after it, the ones in progress finish, and the
/// summary names what was left. redb cannot stop between the rounds inside one file, so a file
/// interrupted mid-compaction is repaired in full the next time anything opens it; finishing it
/// avoids that. A second Ctrl+C stops at once, and leaves that repair to whoever opens next.
pub async fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let mut dry_run = false;
    let mut jobs: Option<usize> = None;
    // Taken off before the override parser sees them, which rejects a flag it does not know.
    let mut rest = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "--dry-run" => {
                dry_run = true;
                continue;
            }
            "--jobs" => args.next(),
            _ => match arg.strip_prefix("--jobs=") {
                Some(v) => Some(v.to_string()),
                None => {
                    rest.push(arg);
                    continue;
                }
            },
        };
        jobs = Some(
            value
                .as_deref()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .context("--jobs takes a number of files to compact at once, 1 or more")?,
        );
    }
    let overrides = config::CliOverrides::parse(rest)?;
    let config = CameoDbConfig::load_unvalidated(&overrides)?;

    let files = redb_files(&config);
    if files.is_empty() {
        bail!(
            "no redb files under {:?}: is this the data path the node runs with?",
            config.storage.data_paths
        );
    }
    let jobs = jobs.unwrap_or(files.len()).min(files.len());

    println!(
        "{} {} redb file(s), {} at a time{}",
        if dry_run { "Measuring" } else { "Compacting" },
        files.len(),
        jobs,
        if dry_run {
            " (dry run: nothing is changed)"
        } else {
            ""
        }
    );

    let stop = Arc::new(AtomicBool::new(false));
    tokio::spawn({
        let stop = Arc::clone(&stop);
        async move {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            stop.store(true, Ordering::SeqCst);
            eprintln!(
                "\nStopping: the files in progress finish, no more start. Ctrl+C again to stop \
                 now — a file stopped mid-compaction is repaired in full the next time it opens."
            );
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!(
                    "Stopped. Run compact again to continue; it picks up where the files are."
                );
                std::process::exit(130);
            }
        }
    });

    let report = tokio::task::spawn_blocking({
        let stop = Arc::clone(&stop);
        move || {
            compact_all(&files, dry_run, jobs, &stop, |path, result| match result {
                Ok(o) => println!(
                    "  {}  {} -> {}{}  ({:.1?}{})",
                    path.display(),
                    mb(o.before),
                    if dry_run { "~" } else { "" },
                    mb(o.after),
                    o.elapsed,
                    if o.repaired {
                        ", repaired first: it was not closed cleanly"
                    } else {
                        ""
                    }
                ),
                Err(e) if is_already_open(e) => {}
                Err(e) => println!("  {}  FAILED: {:#}", path.display(), e),
            })
        }
    })
    .await?;

    if let Some(path) = &report.locked {
        bail!(
            "{} is open in another process: stop the node first, then run compact",
            path.display()
        );
    }
    println!(
        "{} {} -> {}: {} {}",
        if dry_run { "Would take" } else { "Took" },
        mb(report.before),
        if dry_run {
            format!("about {}", mb(report.after))
        } else {
            mb(report.after)
        },
        mb(report.before.saturating_sub(report.after)),
        if dry_run { "reclaimable" } else { "reclaimed" }
    );
    if !report.not_started.is_empty() {
        println!(
            "Stopped before {} file(s); run compact again to do them:",
            report.not_started.len()
        );
        for path in &report.not_started {
            println!("  {}", path.display());
        }
    }
    if report.failed > 0 {
        bail!("{} file(s) failed", report.failed);
    }
    if !report.not_started.is_empty() {
        bail!("stopped before every file was done");
    }
    Ok(())
}

/// What a run over several files came to.
#[derive(Debug, Default)]
pub(crate) struct Report {
    pub before: u64,
    pub after: u64,
    pub done: usize,
    pub failed: usize,
    /// Files never started: `stop` was set, or another was found locked.
    pub not_started: Vec<PathBuf>,
    /// A file another process holds. Every file belongs to the same node, so one locked file
    /// means the node is up, and nothing more is started.
    pub locked: Option<PathBuf>,
}

/// Compact `files`, `jobs` at a time, calling `on_done` as each one ends.
///
/// `stop` is read before each file starts, never during one: a file once started is finished,
/// because redb has no point inside a compaction to stop at that leaves the file clean.
pub(crate) fn compact_all(
    files: &[PathBuf],
    dry_run: bool,
    jobs: usize,
    stop: &AtomicBool,
    on_done: impl Fn(&Path, &Result<Outcome>) + Sync,
) -> Report {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let report = std::sync::Mutex::new(Report::default());
    std::thread::scope(|scope| {
        for _ in 0..jobs.max(1) {
            scope.spawn(|| {
                loop {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    let Some(path) = files.get(i) else {
                        return;
                    };
                    let result = compact_file(path, dry_run);
                    on_done(path, &result);
                    let mut report = report.lock().unwrap_or_else(|p| p.into_inner());
                    match result {
                        Ok(o) => {
                            report.done += 1;
                            report.before += o.before;
                            report.after += o.after;
                        }
                        Err(e) if is_already_open(&e) => {
                            stop.store(true, Ordering::SeqCst);
                            report.locked.get_or_insert_with(|| path.clone());
                        }
                        Err(_) => report.failed += 1,
                    }
                }
            });
        }
    });
    let mut report = report.into_inner().unwrap_or_else(|p| p.into_inner());
    // Every index below `next` was taken by a worker; the rest were never started. A worker
    // that took one past the end, or found `stop` already set, took none.
    let started = next.load(Ordering::SeqCst).min(files.len());
    report.not_started = files[started..].to_vec();
    report
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
    builder.set_cache_size(CACHE_PER_FILE);
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
    use std::sync::atomic::AtomicBool;

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

    /// Three files after a mass delete, each in its own directory, and their sizes.
    fn three_files(dir: &Path) -> (Vec<PathBuf>, Vec<u64>) {
        let files: Vec<PathBuf> = (0..3)
            .map(|i| {
                let sub = dir.join(format!("shard-{i}"));
                std::fs::create_dir_all(&sub).unwrap();
                file_after_a_mass_delete(&sub, 8_000)
            })
            .collect();
        let sizes = files
            .iter()
            .map(|f| std::fs::metadata(f).unwrap().len())
            .collect();
        (files, sizes)
    }

    fn size(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().len()
    }

    #[test]
    fn several_at_a_time_every_file_is_compacted_and_keeps_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());

        let report = compact_all(&files, false, 2, &AtomicBool::new(false), |_, _| {});

        assert_eq!((report.done, report.failed), (3, 0));
        assert!(report.not_started.is_empty() && report.locked.is_none());
        assert_eq!(report.before, sizes.iter().sum::<u64>());
        // Sizes before `rows` reopens the files: a close writes a little of its own.
        assert_eq!(report.after, files.iter().map(|f| size(f)).sum::<u64>());
        for (file, before) in files.iter().zip(&sizes) {
            assert!(size(file) < before / 2, "{}", file.display());
            assert_eq!(rows(file), 2_000);
        }
    }

    #[test]
    fn a_stop_finishes_the_file_in_progress_and_starts_no_other() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());
        let stop = AtomicBool::new(false);

        // Ctrl+C arriving while the first file compacts: that file ends, nothing more starts.
        let report = compact_all(&files, false, 1, &stop, |_, _| {
            stop.store(true, Ordering::SeqCst)
        });

        assert_eq!(report.done, 1);
        assert!(size(&files[0]) < sizes[0] / 2);
        assert_eq!(report.not_started, files[1..].to_vec());
        for (file, before) in files[1..].iter().zip(&sizes[1..]) {
            assert_eq!(size(file), *before, "an unstarted file is left as it was");
        }
    }

    #[test]
    fn a_stop_before_the_run_starts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());

        let report = compact_all(&files, false, 2, &AtomicBool::new(true), |_, _| {});

        assert_eq!(report.done, 0);
        assert_eq!(report.not_started, files);
        assert_eq!(files.iter().map(|f| size(f)).collect::<Vec<_>>(), sizes);
    }

    #[test]
    fn a_locked_file_stops_the_run_and_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());
        let _node = redb::Database::open(&files[0]).unwrap();

        let report = compact_all(&files, false, 1, &AtomicBool::new(false), |_, _| {});

        assert_eq!(report.locked.as_ref(), Some(&files[0]));
        assert_eq!(report.done, 0);
        assert_eq!(report.not_started, files[1..].to_vec());
        assert_eq!(files.iter().map(|f| size(f)).collect::<Vec<_>>(), sizes);
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
