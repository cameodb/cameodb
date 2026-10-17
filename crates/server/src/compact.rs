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
//!
//! `--rewrite` is the other way: copy every table into a new file beside the old one, row by
//! row in key order, and switch to it; see [`rewrite_file`]. It ends at the same size in-place
//! compaction can reach — redb packs a copy no tighter than the original — at the cost of free
//! disk for the copy, and it does not depend on how the old file's free space is laid out.
//! In-place compaction does: a round ends at the first page it finds no free run of its size
//! below. That has not bitten in testing — a dense file of mixed 2–72 KB documents, every 20th
//! deleted, compacted in place to its allocated size — so `--dry-run` first: a file compaction
//! leaves well above the size the dry run reports is what a rewrite is for.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use redb::{
    Database, Key, MultimapTableHandle, ReadTransaction, ReadableDatabase, TableDefinition,
    TableError, TableHandle, Value,
};

use crate::config::{self, CameoDbConfig};

/// redb's page cache for each file being compacted.
///
/// redb's own default is 1 GB, which made memory the limit on running files in parallel: four
/// at once peaked at 4.6 GB. Measured on four 1.4 GB shards, all at once: 1 GB of cache took
/// 13.6 s, 256 MB 17.1 s at 1.5 GB peak, and 64 MB or 16 MB no slower than 256 MB. The large
/// cache only helped because those files nearly fit in it; production shards do not.
const CACHE_PER_FILE: usize = 256 * 1024 * 1024;

/// How much a rewrite copies into the new file per transaction. Each commit is durable, and the
/// rows of an uncommitted transaction are held in memory, so this bounds memory per file.
const REWRITE_BATCH_BYTES: usize = 256 * 1024 * 1024;

/// How a file gives its free space back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// redb's own compaction, in place: needs no free disk, but stops at the first page it has
    /// no room below for.
    InPlace,
    /// Copy every table into a new file and switch to it: needs free disk for the data the file
    /// holds, and leaves no free space behind. The original is deleted, or with `keep_old`
    /// stays beside the new file as `store.redb.old`.
    Rewrite { keep_old: bool },
}

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
    /// Where a rewrite left the original file, which still takes `before` on disk.
    pub kept: Option<PathBuf>,
}

/// Why a rewrite did not start, or did not finish. The original file is as it was either way.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RewriteError {
    #[error("stopped mid-rewrite; the original is as it was")]
    Stopped,
    #[error(
        "not started: the copy needs about {} free on its disk, and {} is{}",
        mb(*.needed),
        mb(*.available),
        if *.reserved > 0 {
            format!(", {} of it claimed by rewrites in progress", mb(*.reserved))
        } else {
            String::new()
        }
    )]
    NoSpace {
        needed: u64,
        available: u64,
        reserved: u64,
    },
    #[error("{} is left from an earlier rewrite: delete it, or rewrite without --keep", .0.display())]
    OldExists(PathBuf),
}

/// Run the subcommand. `args` is everything after `compact`.
///
/// Every file is compacted at once unless `--jobs N` says otherwise. Compaction is bound by the
/// disk, not the CPU — four 1.4 GB shards at once kept under two cores busy — and memory is
/// bounded per file by [`CACHE_PER_FILE`], so the disk is the only reason to hold back: `--jobs`
/// is for a spinning disk, or a server busy with other work. `--rewrite` does one file at a
/// time unless told otherwise, since each file in progress needs free disk for its copy.
///
/// Ctrl+C stops it cleanly: no file is started after it, the ones in progress finish, and the
/// summary names what was left. redb cannot stop between the rounds inside one file, so a file
/// interrupted mid-compaction is repaired in full the next time anything opens it; finishing it
/// avoids that. A second Ctrl+C stops at once, and leaves that repair to whoever opens next.
/// A rewrite has no such cost — its original is untouched until the copy is done — so Ctrl+C
/// abandons the rewrites in progress too, and deletes their partial copies.
pub async fn run(args: impl Iterator<Item = String>) -> Result<()> {
    const USAGE: &str = "\
cameodb compact — return the disk a delete freed inside the redb files

Usage:
  cameodb compact [-c <PATH>] [--dry-run] [--jobs <N>]
  cameodb compact --rewrite [--keep] [-c <PATH>] [--dry-run] [--jobs <N>]

Run it with the node stopped: redb locks its files, so it refuses to start while the node holds
them. It works on every shard's store.redb and on metadata.redb under the configured data paths.

Options:
  -c, --config <PATH>   The node's config file, for its data paths
  --dry-run             Report the size each file would come down to, and change nothing
  --jobs <N>            Files worked on at once (default: every file; with --rewrite, 1)
  --rewrite             Copy each file into a new one beside it, then switch to the copy.
                        Needs free disk for the copy, about the size --dry-run reports, and
                        checks for it before each file: one without room is left alone.
  --keep                With --rewrite, keep each original as store.redb.old instead of
                        deleting it. Nothing is freed on disk until those files are deleted.
  -h, --help            Show this message

Without --rewrite, compaction moves the data down inside the file and truncates the rest: no
free disk needed. It ends at the size --dry-run reports unless a page it cannot move stops it;
a file left well above that size is what --rewrite is for.

Ctrl+C stops it cleanly. Compaction finishes the files in progress, starts no more, and lists
the ones it did not reach. A rewrite abandons the copies in progress and deletes them; the
originals are as they were. Running the command again continues either way.";

    let mut dry_run = false;
    let mut rewrite = false;
    let mut keep_old = false;
    let mut jobs: Option<usize> = None;
    // Taken off before the override parser sees them, which rejects a flag it does not know.
    let mut rest = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "--dry-run" => {
                dry_run = true;
                continue;
            }
            "--rewrite" => {
                rewrite = true;
                continue;
            }
            "--keep" => {
                keep_old = true;
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
    if keep_old && !rewrite {
        bail!("--keep goes with --rewrite: in-place compaction leaves no original to keep");
    }
    let mode = if rewrite {
        Mode::Rewrite { keep_old }
    } else {
        Mode::InPlace
    };
    let overrides = config::CliOverrides::parse(rest)?;
    let config = CameoDbConfig::load_unvalidated(&overrides)?;

    let files = redb_files(&config);
    if files.is_empty() {
        bail!(
            "no redb files under {:?}: is this the data path the node runs with?",
            config.storage.data_paths
        );
    }
    let default_jobs = match mode {
        Mode::InPlace => files.len(),
        Mode::Rewrite { .. } => 1,
    };
    let jobs = jobs.unwrap_or(default_jobs).min(files.len());

    println!(
        "{} {} redb file(s), {} at a time{}{}",
        match (dry_run, mode) {
            (true, _) => "Measuring",
            (false, Mode::InPlace) => "Compacting",
            (false, Mode::Rewrite { .. }) => "Rewriting",
        },
        files.len(),
        jobs,
        match mode {
            Mode::Rewrite { keep_old: true } =>
                ", each into a new file; the originals are kept as store.redb.old",
            Mode::Rewrite { keep_old: false } =>
                ", each into a new file; each original is deleted once its copy is in place",
            Mode::InPlace => "",
        },
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
            match mode {
                Mode::InPlace => eprintln!(
                    "\nStopping: the files in progress finish, no more start. Ctrl+C again to \
                     stop now — a file stopped mid-compaction is repaired in full the next time \
                     it opens."
                ),
                Mode::Rewrite { .. } => eprintln!(
                    "\nStopping: rewrites in progress are abandoned and their partial copies \
                     deleted; the originals are as they were."
                ),
            }
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!(
                    "Stopped. Run compact again to continue; it picks up where the files are{}.",
                    match mode {
                        Mode::InPlace => "",
                        Mode::Rewrite { .. } => " and deletes any partial copy left behind",
                    }
                );
                std::process::exit(130);
            }
        }
    });

    let report = tokio::task::spawn_blocking({
        let stop = Arc::clone(&stop);
        move || {
            compact_all(
                &files,
                mode,
                dry_run,
                jobs,
                &stop,
                |path, result| match result {
                    Ok(o) => println!(
                        "  {}  {} -> {}{}  ({:.1?}{}{})",
                        path.display(),
                        mb(o.before),
                        if dry_run { "~" } else { "" },
                        mb(o.after),
                        o.elapsed,
                        if o.repaired {
                            ", repaired first: it was not closed cleanly"
                        } else {
                            ""
                        },
                        match &o.kept {
                            Some(old) => format!(", original kept as {}", old.display()),
                            None => String::new(),
                        }
                    ),
                    Err(e) if is_already_open(e) => {}
                    Err(e) if is_stopped(e) => {
                        println!("  {}  {}", path.display(), RewriteError::Stopped)
                    }
                    Err(e) => println!("  {}  FAILED: {:#}", path.display(), e),
                },
            )
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
        match (dry_run, report.kept) {
            (true, _) => "reclaimable",
            (false, 0) => "reclaimed",
            (false, _) => "smaller",
        }
    );
    if report.kept > 0 {
        println!(
            "The originals still take {} beside the new files, as store.redb.old: nothing is \
             freed on disk until they go. Start the node, check it, then delete them.",
            mb(report.kept)
        );
    }
    if report.short_of_space > 0 {
        println!(
            "{} file(s) not rewritten for lack of free disk{}",
            report.short_of_space,
            if report.kept > 0 {
                ": deleting the store.redb.old files kept so far frees room, or rewrite \
                 without --keep"
            } else {
                ""
            }
        );
    }
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
    /// Failures, those for lack of disk included.
    pub failed: usize,
    /// Rewrites not started because their disk had no room for the copy.
    pub short_of_space: usize,
    /// What the originals a rewrite kept still take on disk.
    pub kept: u64,
    /// Files left as they were: never started, because `stop` was set or another was found
    /// locked, or a rewrite abandoned on `stop`.
    pub not_started: Vec<PathBuf>,
    /// A file another process holds. Every file belongs to the same node, so one locked file
    /// means the node is up, and nothing more is started.
    pub locked: Option<PathBuf>,
}

/// Compact `files`, `jobs` at a time, calling `on_done` as each one ends.
///
/// `stop` is read before each file starts. In place, it is never read during one: a file once
/// started is finished, because redb has no point inside a compaction to stop at that leaves the
/// file clean. A rewrite reads it between batches, and abandons its copy.
pub(crate) fn compact_all(
    files: &[PathBuf],
    mode: Mode,
    dry_run: bool,
    jobs: usize,
    stop: &AtomicBool,
    on_done: impl Fn(&Path, &Result<Outcome>) + Sync,
) -> Report {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let report = Mutex::new(Report::default());
    let abandoned = Mutex::new(Vec::new());
    let disk = DiskReservations::default();
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
                    let result = match mode {
                        Mode::InPlace => compact_file(path, dry_run),
                        Mode::Rewrite { keep_old } => {
                            rewrite_file(path, dry_run, keep_old, stop, &disk)
                        }
                    };
                    on_done(path, &result);
                    let mut report = report.lock().unwrap_or_else(|p| p.into_inner());
                    match result {
                        Ok(o) => {
                            report.done += 1;
                            report.before += o.before;
                            report.after += o.after;
                            if o.kept.is_some() {
                                report.kept += o.before;
                            }
                        }
                        Err(e) if is_already_open(&e) => {
                            stop.store(true, Ordering::SeqCst);
                            report.locked.get_or_insert_with(|| path.clone());
                        }
                        Err(e) if is_stopped(&e) => abandoned
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .push(path.clone()),
                        Err(e) => {
                            if matches!(
                                e.downcast_ref::<RewriteError>(),
                                Some(RewriteError::NoSpace { .. })
                            ) {
                                report.short_of_space += 1;
                            }
                            report.failed += 1;
                        }
                    }
                }
            });
        }
    });
    let mut report = report.into_inner().unwrap_or_else(|p| p.into_inner());
    // Every index below `next` was taken by a worker; the rest were never started. A worker
    // that took one past the end, or found `stop` already set, took none.
    let started = next.load(Ordering::SeqCst).min(files.len());
    report.not_started = abandoned.into_inner().unwrap_or_else(|p| p.into_inner());
    report.not_started.sort();
    report.not_started.extend_from_slice(&files[started..]);
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
    let (mut db, repaired) = open(path)?;

    let after = if dry_run {
        allocated_bytes(&db)?.min(before)
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
        kept: None,
    })
}

/// Open `path` for this command alone, and say whether redb repaired it first.
///
/// A file the node did not close cleanly is repaired before `open` returns, and without a
/// recorded allocator state that walks the whole file. Say so, rather than appear to hang.
fn open(path: &Path) -> Result<(Database, Arc<AtomicBool>)> {
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
    Ok((builder.open(path)?, repaired))
}

/// What the file's allocated pages take: the size in-place compaction ends at when nothing
/// blocks it.
fn allocated_bytes(db: &Database) -> Result<u64> {
    let txn = db.begin_write()?;
    let stats = txn.stats()?;
    txn.abort()?;
    Ok(stats.allocated_pages() * stats.page_size() as u64)
}

/// Rewrite one file into a new one and switch to it, or under `dry_run` estimate the result.
///
/// The copy goes to `store.redb.rewrite` beside the original, one table at a time in key order,
/// committed every [`REWRITE_BATCH_BYTES`]; the original is only read. Once every table's row
/// count matches, the copy replaces the original by rename, so at every moment `store.redb` is
/// one complete file or the other. The rename is the last anyone sees of the original, unless
/// `keep_old`: then it is first hard-linked as `store.redb.old`.
///
/// Anything that ends the rewrite early — `stop`, an error, a crash — leaves the original as it
/// was. A partial copy is deleted, here or by the next rewrite of the same file.
///
/// The copy needs free disk about the size of the data the file holds: the allocated size,
/// which `dry_run` reports. That is claimed from [`DiskReservations`] before the copy starts,
/// and a rewrite with no room is not started — under `dry_run` too, so the dry run says which
/// files would be. Should the disk fill anyway, by some other writer, the write that fails
/// ends the rewrite like any other error: the partial copy goes, the original is untouched.
pub(crate) fn rewrite_file(
    path: &Path,
    dry_run: bool,
    keep_old: bool,
    stop: &AtomicBool,
    disk: &DiskReservations,
) -> Result<Outcome> {
    let started = Instant::now();
    let before = std::fs::metadata(path)?.len();
    let copy = sibling(path, "rewrite");
    let old = sibling(path, "old");
    if keep_old && old.exists() {
        return Err(RewriteError::OldExists(old).into());
    }
    let (src, repaired) = open(path)?;
    let estimate = allocated_bytes(&src)?.min(before);
    if dry_run {
        // Only the check: the claim is given back as this returns.
        disk.reserve(path, &copy, estimate)?;
        return Ok(Outcome {
            before,
            after: estimate,
            repaired: repaired.load(Ordering::Relaxed),
            elapsed: started.elapsed(),
            kept: None,
        });
    }

    // Only a rewrite of this file makes this name, and only one runs at a time: redb's lock
    // on the original, held above, says so.
    remove_if_present(&copy)?;
    let reservation = disk.reserve(path, &copy, estimate)?;
    let copied = copy_into(&src, &copy, stop);
    drop(src);
    drop(reservation);
    if let Err(e) = copied {
        let _ = remove_if_present(&copy);
        return Err(e);
    }

    let swapped = (|| -> Result<()> {
        if keep_old {
            std::fs::hard_link(path, &old)
                .with_context(|| format!("keeping the original as {}", old.display()))?;
        }
        std::fs::rename(&copy, path)
            .with_context(|| format!("moving {} into place", copy.display()))?;
        sync_dir(path)
    })();
    if let Err(e) = swapped {
        // Either the rename happened, and `store.redb` is the copy, or it did not and it is the
        // original. A link made without the rename is the original under a second name.
        if copy.exists() {
            let _ = remove_if_present(&copy);
            if keep_old {
                let _ = remove_if_present(&old);
            }
        }
        return Err(e);
    }

    Ok(Outcome {
        before,
        after: std::fs::metadata(path)?.len(),
        repaired: repaired.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
        kept: keep_old.then_some(old),
    })
}

/// Copy every table of `src` into a new file at `dest`, and check each one's row count.
fn copy_into(src: &Database, dest: &Path, stop: &AtomicBool) -> Result<()> {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(CACHE_PER_FILE);
    let dst = builder
        .create(dest)
        .with_context(|| format!("creating {}", dest.display()))?;
    let txn = src.begin_read()?;
    if let Some(table) = txn.list_multimap_tables()?.next() {
        bail!(
            "holds a multimap table, {}, which a rewrite does not copy",
            table.name()
        );
    }
    for table in txn.list_tables()? {
        copy_table(&txn, &dst, table.name(), stop)
            .with_context(|| format!("copying table {}", table.name()))?;
    }
    // Each batch commit frees the pages it rewrote, so the copy grows past what it holds: by
    // two thirds, for 40 MB of mixed rows. Compacting it returns that, and nothing in it can
    // stop at a page: its free space is only what the copy itself just left.
    let mut dst = dst;
    while dst.compact().context("compacting the copy")? {}
    Ok(())
}

/// Copy one table, whatever its types are among those cameodb's files use.
///
/// redb opens a table only under the key and value types it was created with, and a rewrite has
/// no list of tables: shard stores have one data and one WAL table per index. So each pairing in
/// use is tried in turn. A table under none of them fails the rewrite, naming it, rather than be
/// left behind.
fn copy_table(src: &ReadTransaction, dst: &Database, name: &str, stop: &AtomicBool) -> Result<()> {
    if try_copy::<&str, &[u8]>(src, dst, name, stop)?
        || try_copy::<u64, &[u8]>(src, dst, name, stop)?
        || try_copy::<&str, u64>(src, dst, name, stop)?
        || try_copy::<&[u8], &[u8]>(src, dst, name, stop)?
    {
        return Ok(());
    }
    bail!("its key and value types are none a cameodb file uses")
}

/// Copy table `name` as `K` to `V` if that is what it holds; `false` if it is not.
fn try_copy<K: Key + 'static, V: Value + 'static>(
    src: &ReadTransaction,
    dst: &Database,
    name: &str,
    stop: &AtomicBool,
) -> Result<bool> {
    let def = TableDefinition::<K, V>::new(name);
    let table = match src.open_table(def) {
        Ok(table) => table,
        Err(TableError::TableTypeMismatch { .. }) => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let expected = redb::ReadableTableMetadata::len(&table)?;
    let mut rows = redb::ReadableTable::iter(&table)?;
    let mut copied = 0u64;
    loop {
        if stop.load(Ordering::SeqCst) {
            return Err(RewriteError::Stopped.into());
        }
        let txn = dst.begin_write()?;
        let mut done = false;
        {
            // Opened even when there is nothing to copy, so an empty table is kept too.
            let mut out = txn.open_table(def)?;
            let mut bytes = 0;
            while bytes < REWRITE_BATCH_BYTES {
                let Some(row) = rows.next() else {
                    done = true;
                    break;
                };
                let (key, value) = row?;
                let (key, value) = (key.value(), value.value());
                bytes += K::as_bytes(&key).as_ref().len() + V::as_bytes(&value).as_ref().len();
                out.insert(key, value)?;
                copied += 1;
            }
        }
        txn.commit()?;
        if done {
            break;
        }
    }
    if copied != expected {
        bail!("copied {copied} rows of the {expected} it holds");
    }
    Ok(true)
}

/// `store.redb` → `store.redb.<suffix>`, beside it: the same filesystem, so a rename is atomic
/// and a hard link possible.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(suffix);
    path.with_file_name(name)
}

fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(e).with_context(|| format!("deleting {}", path.display()))
        }
        _ => Ok(()),
    }
}

/// Make a rename in the directory holding `path` durable.
fn sync_dir(path: &Path) -> Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing {}", dir.display()))
}

/// The free disk each rewrite in progress has claimed for its copy, so that two at once do not
/// both count the same free space as theirs.
#[derive(Default)]
pub(crate) struct DiskReservations {
    held: Mutex<Vec<Claim>>,
    /// The free space to assume in place of asking the filesystem.
    #[cfg(test)]
    available: Option<u64>,
}

struct Claim {
    id: u64,
    filesystem: u64,
    copy: PathBuf,
    bytes: u64,
}

/// A claim on free disk, given back on drop.
pub(crate) struct Reservation<'a> {
    owner: &'a DiskReservations,
    id: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut held = self.owner.held.lock().unwrap_or_else(|p| p.into_inner());
        held.retain(|c| c.id != self.id);
    }
}

impl DiskReservations {
    /// Claim `bytes` of free disk for `copy`, the rewrite of `path`, or say why there is not
    /// enough. A claim already made on the same filesystem counts for what its copy has yet to
    /// write: what it has written, the filesystem already reports as used.
    fn reserve(&self, path: &Path, copy: &Path, bytes: u64) -> Result<Reservation<'_>> {
        let dir = path.parent().unwrap_or(Path::new("."));
        let filesystem = filesystem_of(dir)?;
        let mut held = self.held.lock().unwrap_or_else(|p| p.into_inner());
        let available = self.available(dir)?;
        let reserved: u64 = held
            .iter()
            .filter(|c| c.filesystem == filesystem)
            .map(|c| {
                let written = std::fs::metadata(&c.copy).map(|m| m.len()).unwrap_or(0);
                c.bytes.saturating_sub(written)
            })
            .sum();
        // redb grows a file in steps, and the last can overshoot what it holds; the copy is
        // compacted only once it is whole.
        let needed = bytes + bytes / 20;
        if available.saturating_sub(reserved) < needed {
            return Err(RewriteError::NoSpace {
                needed,
                available,
                reserved,
            }
            .into());
        }
        let id = held.iter().map(|c| c.id + 1).max().unwrap_or(0);
        held.push(Claim {
            id,
            filesystem,
            copy: copy.to_path_buf(),
            bytes: needed,
        });
        Ok(Reservation { owner: self, id })
    }

    /// What an unprivileged process may still write on the filesystem holding `dir`: less than
    /// what is free, by what the filesystem keeps back for root.
    fn available(&self, dir: &Path) -> Result<u64> {
        #[cfg(test)]
        if let Some(available) = self.available {
            return Ok(available);
        }
        fs4::available_space(dir)
            .with_context(|| format!("reading the free space of {}", dir.display()))
    }
}

/// Which filesystem `dir` is on, to tell the claims sharing its free space.
#[cfg(unix)]
fn filesystem_of(dir: &Path) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::metadata(dir)?.dev())
}

/// Every path counts as on one filesystem: claims on different disks then share, which can
/// only refuse a rewrite that would have fit, never start one that will not.
#[cfg(not(unix))]
fn filesystem_of(_dir: &Path) -> Result<u64> {
    Ok(0)
}

fn is_already_open(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<redb::DatabaseError>(),
        Some(redb::DatabaseError::DatabaseAlreadyOpen)
    )
}

fn is_stopped(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<RewriteError>(),
        Some(RewriteError::Stopped)
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

        let report = compact_all(
            &files,
            Mode::InPlace,
            false,
            2,
            &AtomicBool::new(false),
            |_, _| {},
        );

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
        let report = compact_all(&files, Mode::InPlace, false, 1, &stop, |_, _| {
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

        let report = compact_all(
            &files,
            Mode::InPlace,
            false,
            2,
            &AtomicBool::new(true),
            |_, _| {},
        );

        assert_eq!(report.done, 0);
        assert_eq!(report.not_started, files);
        assert_eq!(files.iter().map(|f| size(f)).collect::<Vec<_>>(), sizes);
    }

    #[test]
    fn a_locked_file_stops_the_run_and_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());
        let _node = redb::Database::open(&files[0]).unwrap();

        let report = compact_all(
            &files,
            Mode::InPlace,
            false,
            1,
            &AtomicBool::new(false),
            |_, _| {},
        );

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

    /// The tables a shard store holds, under the types the node gives them.
    const SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");
    const RECOVERY: TableDefinition<&str, u64> = TableDefinition::new("_recovery_meta");
    const WAL: TableDefinition<u64, &[u8]> = TableDefinition::new("wal_docs");
    const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("data_docs");
    const ASSIGNMENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("shard_assignments");

    /// A file holding one table of every key and value type cameodb uses, the data table with
    /// three quarters of its rows deleted.
    fn store_with_every_table(dir: &Path) -> PathBuf {
        let path = dir.join("store.redb");
        let db = redb::Database::create(&path).unwrap();
        let body = vec![7u8; 1024];
        let txn = db.begin_write().unwrap();
        {
            txn.open_table(SCHEMA)
                .unwrap()
                .insert("docs", b"{}".as_slice())
                .unwrap();
            txn.open_table(RECOVERY)
                .unwrap()
                .insert("docs", 42)
                .unwrap();
            let mut wal = txn.open_table(WAL).unwrap();
            for i in 0..100u64 {
                wal.insert(i, body.as_slice()).unwrap();
            }
            let mut data = txn.open_table(DATA).unwrap();
            for i in 0..8_000u64 {
                data.insert(format!("doc-{i:06}").as_str(), body.as_slice())
                    .unwrap();
            }
            txn.open_table(ASSIGNMENTS)
                .unwrap()
                .insert(b"shard".as_slice(), b"node".as_slice())
                .unwrap();
            // Kept, though empty.
            txn.open_table(TableDefinition::<u64, &[u8]>::new("wal_empty"))
                .unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(DATA)
            .unwrap()
            .retain(|k, _| k.ends_with('0') || k.ends_with('4') || k.ends_with('8'))
            .unwrap();
        txn.commit().unwrap();
        path
    }

    type Rows = Vec<(Vec<u8>, Vec<u8>)>;

    /// Every row of every table, as bytes, for comparing two files.
    fn contents(path: &Path) -> Vec<(String, Rows)> {
        fn rows<K: Key + 'static, V: Value + 'static>(
            txn: &ReadTransaction,
            def: TableDefinition<K, V>,
        ) -> Vec<(Vec<u8>, Vec<u8>)> {
            let table = txn.open_table(def).unwrap();
            redb::ReadableTable::iter(&table)
                .unwrap()
                .map(|row| {
                    let (k, v) = row.unwrap();
                    (
                        K::as_bytes(&k.value()).as_ref().to_vec(),
                        V::as_bytes(&v.value()).as_ref().to_vec(),
                    )
                })
                .collect()
        }
        let db = redb::Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        let mut names: Vec<String> = txn
            .list_tables()
            .unwrap()
            .map(|t| t.name().to_string())
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let rows = match name.as_str() {
                    "schema" => rows(&txn, SCHEMA),
                    "_recovery_meta" => rows(&txn, RECOVERY),
                    "shard_assignments" => rows(&txn, ASSIGNMENTS),
                    "rows" => rows(&txn, ROWS),
                    n if n.starts_with("wal_") => rows(&txn, TableDefinition::<u64, &[u8]>::new(n)),
                    n => rows(&txn, TableDefinition::<&str, &[u8]>::new(n)),
                };
                (name, rows)
            })
            .collect()
    }

    fn rewrite(path: &Path, keep_old: bool) -> Result<Outcome> {
        rewrite_file(
            path,
            false,
            keep_old,
            &AtomicBool::new(false),
            &DiskReservations::default(),
        )
    }

    /// Many small rows, then large ones written last, at the top of the file; then most of the
    /// small rows deleted: free space scattered below, multi-page rows above it.
    fn file_of_mixed_rows(dir: &Path) -> PathBuf {
        let path = dir.join("store.redb");
        let db = redb::Database::create(&path).unwrap();
        let small = vec![1u8; 1024];
        let large = vec![2u8; 100 * 1024];
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(ROWS).unwrap();
            for i in 0..40_000u64 {
                t.insert(i, small.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(ROWS).unwrap();
            for i in 1_000_000..1_000_200u64 {
                t.insert(i, large.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(ROWS)
            .unwrap()
            .retain(|k, _| k >= 1_000_000 || k % 4 == 0)
            .unwrap();
        txn.commit().unwrap();
        path
    }

    #[test]
    fn a_rewrite_ends_where_in_place_compaction_does() {
        let dir = tempfile::tempdir().unwrap();
        let in_place = dir.path().join("a");
        let rewritten = dir.path().join("b");
        std::fs::create_dir_all(&in_place).unwrap();
        std::fs::create_dir_all(&rewritten).unwrap();
        let a = file_of_mixed_rows(&in_place);
        let b = file_of_mixed_rows(&rewritten);
        let expected = contents(&a);

        let compacted = compact_file(&a, false).unwrap();
        let done = rewrite(&b, false).unwrap();

        // Without compacting the copy, the rewrite ended at 67 MB against 40 MB in place.
        assert!(
            done.after <= compacted.after + compacted.after / 50,
            "in place {compacted:?}, rewritten {done:?}"
        );
        assert_eq!(contents(&b), expected);
    }

    #[test]
    fn a_rewrite_copies_every_table_and_row_and_deletes_the_original() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        assert_eq!(expected.len(), 6);

        let done = rewrite(&path, false).unwrap();

        assert!(done.after < done.before / 2, "{done:?}");
        assert_eq!(done.kept, None);
        assert_eq!(size(&path), done.after);
        assert_eq!(contents(&path), expected);
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            left,
            vec!["store.redb"],
            "no copy and no original left beside it"
        );
    }

    #[test]
    fn with_keep_the_original_stays_beside_it_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        let before = size(&path);

        let done = rewrite(&path, true).unwrap();

        let old = dir.path().join("store.redb.old");
        assert_eq!(done.kept.as_deref(), Some(old.as_path()));
        assert_eq!(size(&old), before);
        assert_eq!(contents(&old), expected);
        assert_eq!(contents(&path), expected);
        assert!(size(&path) < before / 2);
    }

    #[test]
    fn with_keep_an_earlier_original_still_there_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let old = dir.path().join("store.redb.old");
        std::fs::write(&old, b"an earlier original").unwrap();
        let before = size(&path);

        let err = rewrite(&path, true).unwrap_err();

        assert!(
            matches!(err.downcast_ref(), Some(RewriteError::OldExists(p)) if *p == old),
            "{err:#}"
        );
        assert_eq!(std::fs::read(&old).unwrap(), b"an earlier original");
        assert_eq!(size(&path), before);
        // Without --keep it is not in the way.
        rewrite(&path, false).unwrap();
        assert_eq!(std::fs::read(&old).unwrap(), b"an earlier original");
    }

    #[test]
    fn a_stopped_rewrite_leaves_the_original_and_no_copy() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        let before = size(&path);

        let err = rewrite_file(
            &path,
            false,
            true,
            &AtomicBool::new(true),
            &DiskReservations::default(),
        )
        .unwrap_err();

        assert!(is_stopped(&err), "{err:#}");
        assert_eq!(size(&path), before);
        assert_eq!(contents(&path), expected);
        assert!(!dir.path().join("store.redb.rewrite").exists());
        assert!(!dir.path().join("store.redb.old").exists());
    }

    #[test]
    fn a_copy_left_by_an_interrupted_rewrite_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        std::fs::write(dir.path().join("store.redb.rewrite"), b"half a copy").unwrap();

        rewrite(&path, false).unwrap();

        assert_eq!(contents(&path), expected);
        assert!(!dir.path().join("store.redb.rewrite").exists());
    }

    #[test]
    fn a_rewrite_dry_run_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        let before = size(&path);

        let measured = rewrite_file(
            &path,
            true,
            false,
            &AtomicBool::new(false),
            &DiskReservations::default(),
        )
        .unwrap();

        assert!(measured.after < before);
        assert_eq!(contents(&path), expected);
        assert!(!dir.path().join("store.redb.rewrite").exists());
    }

    #[test]
    fn a_rewrite_of_a_file_another_handle_holds_is_reported_as_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let _node = redb::Database::open(&path).unwrap();

        let err = rewrite(&path, false).unwrap_err();

        assert!(is_already_open(&err), "{err:#}");
        assert!(!dir.path().join("store.redb.rewrite").exists());
    }

    #[test]
    fn rewriting_several_files_reports_what_the_kept_originals_take() {
        let dir = tempfile::tempdir().unwrap();
        let (files, sizes) = three_files(dir.path());

        let report = compact_all(
            &files,
            Mode::Rewrite { keep_old: true },
            false,
            2,
            &AtomicBool::new(false),
            |_, _| {},
        );

        assert_eq!((report.done, report.failed), (3, 0));
        assert_eq!(report.kept, sizes.iter().sum::<u64>());
        for file in &files {
            assert_eq!(rows(file), 2_000);
            assert!(sibling(file, "old").exists());
        }
    }

    #[test]
    fn without_room_for_the_copy_a_rewrite_does_not_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = store_with_every_table(dir.path());
        let expected = contents(&path);
        let disk = DiskReservations {
            available: Some(1024),
            ..Default::default()
        };

        for dry_run in [true, false] {
            let err =
                rewrite_file(&path, dry_run, false, &AtomicBool::new(false), &disk).unwrap_err();

            assert!(
                matches!(
                    err.downcast_ref(),
                    Some(RewriteError::NoSpace {
                        available: 1024,
                        ..
                    })
                ),
                "{err:#}"
            );
        }
        assert_eq!(contents(&path), expected);
        assert!(!dir.path().join("store.redb.rewrite").exists());
    }

    #[test]
    fn a_claim_counts_what_its_copy_has_yet_to_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.redb");
        let copy = dir.path().join("store.redb.rewrite");
        let disk = DiskReservations {
            available: Some(10_000),
            ..Default::default()
        };

        let first = disk.reserve(&path, &copy, 6_000).unwrap();
        // 6,300 claimed with the margin, 3,700 left: no room for a second 6,000.
        assert!(disk.reserve(&path, &copy, 6_000).is_err());
        // Once written, the copy owes nothing more: the filesystem reports it as used.
        std::fs::write(&copy, vec![0u8; 6_300]).unwrap();
        assert!(disk.reserve(&path, &copy, 6_000).is_ok());
        std::fs::remove_file(&copy).unwrap();
        drop(first);
        assert!(disk.reserve(&path, &copy, 6_000).is_ok());
    }

    #[test]
    fn the_free_space_read_is_the_filesystems() {
        let dir = tempfile::tempdir().unwrap();
        let available = DiskReservations::default().available(dir.path()).unwrap();
        assert!(
            available > 0,
            "a temp directory with no free space: {available}"
        );
    }
}
