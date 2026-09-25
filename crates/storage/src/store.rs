//! `HybridStore` state and the write side: index and writer lifecycle, batch
//! and single writes, commits and checkpoints, WAL persistence and recovery,
//! warmup, and schema storage.
use crate::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde_json::Value as JsonValue;
use tantivy::collector::TopDocs;
use tantivy::query::AllQuery;
use tantivy::schema::{FAST, INDEXED, STORED, STRING, Schema, TEXT, Value as TantivyValue};
use tantivy::{Index, IndexReader, IndexWriter, Order, doc};
use tracing::trace;

/// Schema metadata table: maps index names to their schema definitions.
pub(crate) const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Recovery metadata table: maps index names to their last committed Tantivy sequence.
///
/// Written in the same transaction that truncates the WAL after a successful Tantivy commit.
/// Since the checkpoint moved into Tantivy's own commit payload this is a fallback rather
/// than the primary record: it is what tells recovery where an index stands when that index
/// was last committed by a build that predates the payload stamp. Nothing on the boot path
/// reads it for an index whose WAL is empty.
pub(crate) const TABLE_RECOVERY_META: TableDefinition<&str, u64> =
    TableDefinition::new("_recovery_meta");

/// Tag byte introducing a WAL entry that records only the document id.
///
/// The WAL exists to say *which documents* Tantivy may be behind on, and it is written in the
/// same redb transaction as the `data_<index>` row for that document. The row is therefore
/// always there to be read at replay time, and storing the document a second time in the WAL
/// bought nothing while doubling the bytes every write serialises and fsyncs.
///
/// Recovery reads the id, looks it up in `data_<index>`, and lets the answer decide the
/// operation: a row means the document should be indexed as it now stands, no row means it was
/// deleted. That is not a weaker record than the old one — replay converges on the committed
/// state of each id rather than re-enacting a log, so a put later overwritten, or deleted, in
/// the same tail resolves in one step instead of several.
///
/// `0x01` cannot begin a legacy entry: those are JSON objects, so they begin with `{`.
pub(crate) const WAL_ENTRY_ID_ONLY: u8 = 0x01;

/// Encode a WAL entry: the tag byte followed by the document id.
pub(crate) fn encode_wal_entry(id: &str) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(id.len() + 1);
    encoded.push(WAL_ENTRY_ID_ONLY);
    encoded.extend_from_slice(id.as_bytes());
    encoded
}

/// Read the document id out of a WAL entry, in either format.
///
/// Entries written before the id-only format are whole `WalOp` JSON values. They still decode
/// here — only their id is taken, and the document body they carry is ignored in favour of the
/// `data_<index>` row, so one replay path serves both formats and an upgrade needs no migration
/// of a WAL tail left behind by the previous build.
pub(crate) fn decode_wal_entry(bytes: &[u8]) -> Result<String, StoreError> {
    if let Some((&WAL_ENTRY_ID_ONLY, id_bytes)) = bytes.split_first() {
        return std::str::from_utf8(id_bytes)
            .map(str::to_string)
            .map_err(|e| {
                StoreError::Serialization(format!("WAL entry id is not valid UTF-8: {e}"))
            });
    }

    let legacy: WalOp =
        serde_json::from_slice(bytes).map_err(|e| StoreError::Serialization(e.to_string()))?;
    Ok(match legacy {
        WalOp::Put { id, .. } => id,
        WalOp::Delete { id } => id,
    })
}

/// Prefix of the string cameodb stamps into every Tantivy commit payload. The number after
/// it is the `wal_<index>` sequence that commit covers.
///
/// Tantivy writes the payload into `meta.json` as part of the commit itself, which is the
/// whole reason the checkpoint lives there. A checkpoint kept anywhere else is a second
/// write that can be interrupted, and the two orderings fail differently: recorded too early
/// it claims documents Tantivy never got, recorded too late it forces a replay of a tail
/// Tantivy already has. Inside the commit there is no window at all — if the segments are on
/// disk, so is the sequence that describes them.
pub(crate) const CHECKPOINT_PAYLOAD_PREFIX: &str = "cameodb:wal_seq=";

pub(crate) fn encode_checkpoint_payload(seq: u64) -> String {
    format!("{CHECKPOINT_PAYLOAD_PREFIX}{seq}")
}

pub(crate) fn decode_checkpoint_payload(payload: &str) -> Option<u64> {
    payload
        .strip_prefix(CHECKPOINT_PAYLOAD_PREFIX)?
        .parse()
        .ok()
}

/// Commit `writer`, recording `seq` as the WAL sequence the commit covers.
///
/// Every Tantivy commit in the process goes through here. One that skips the stamp leaves
/// the checkpoint behind on an index that is in fact up to date, and the next boot pays for
/// it by replaying a tail that is already indexed.
pub(crate) fn commit_writer_at(writer: &mut IndexWriter, seq: u64) -> Result<(), StoreError> {
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&encode_checkpoint_payload(seq));
    prepared.commit()?;
    Ok(())
}

/// The WAL sequence recorded in `tantivy_index`'s last commit.
///
/// Reads `meta.json` and nothing else, so the cost does not move with the size of the index.
/// `None` means the last commit predates the stamp; the caller resolves those from redb.
pub(crate) fn tantivy_checkpoint_seq(tantivy_index: &Index) -> Option<u64> {
    tantivy_index
        .load_metas()
        .ok()?
        .payload
        .as_deref()
        .and_then(decode_checkpoint_payload)
}

/// How long startup warmup may spend faulting in segment structures before it gives up on
/// the indices it has not reached. Indices are warmed smallest-first, so the budget buys the
/// largest number of warm indices it can and leaves the rest to warm on demand.
/// How many times a write reopens an index that eviction closed under it before it gives up
/// with [`StoreError::WriterClosed`]. Each attempt needs a fresh eviction of this very index
/// inside the few microseconds between opening it and locking its writer.
const LIVE_WRITER_ATTEMPTS: usize = 3;

/// Lock an index writer, recovering it from a poisoned mutex: a panic applying one write is
/// caught per operation and the writer rebuilt, so poison is not a reason to fail the next.
fn lock_writer<'w>(writer: &'w Mutex<IndexWriter>, index: &str) -> MutexGuard<'w, IndexWriter> {
    writer.lock().unwrap_or_else(|poisoned| {
        tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
        poisoned.into_inner()
    })
}

/// `pending_since` holds this while an index has nothing waiting for a commit.
pub(crate) const NOTHING_PENDING: u64 = 0;

/// With a commit interval configured, how far past the count threshold an index may run before
/// it commits regardless of the clock. The interval is what normally triggers a commit; this
/// bounds the WAL tail a restart replays and the buffer a commit has to flush, when writes
/// arrive faster than an interval can cover.
pub(crate) const COMMIT_BACKSTOP_MULTIPLE: u64 = 20;

pub(crate) const WARMUP_BUDGET: Duration = Duration::from_secs(60);

/// Counting semaphore bounding how many indices replay their WAL tail at once, across every
/// shard in the process.
///
/// Replay needs an `IndexWriter`, and an `IndexWriter` is worker threads plus an indexing
/// arena that reaches hundreds of megabytes on a large index. Recovery is driven per shard
/// and every shard on a node starts at the same moment, so a per-shard limit silently
/// multiplies by the shard count — the 16-shard node that was just killed for using too much
/// memory would answer by allocating 16 × cores arenas to recover from it. The cap is global
/// for that reason.
pub(crate) struct RecoveryGate {
    pub(crate) permits: Mutex<usize>,
    pub(crate) released: Condvar,
}

impl RecoveryGate {
    pub(crate) fn acquire(&self) -> RecoveryPermit<'_> {
        let mut permits = self
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *permits == 0 {
            permits = self
                .released
                .wait(permits)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *permits -= 1;
        RecoveryPermit { gate: self }
    }
}

pub(crate) struct RecoveryPermit<'a> {
    pub(crate) gate: &'a RecoveryGate,
}

impl Drop for RecoveryPermit<'_> {
    fn drop(&mut self) {
        let mut permits = self
            .gate
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *permits += 1;
        self.gate.released.notify_one();
    }
}

/// Four concurrent replays is enough to keep the disk busy without letting the arenas add up
/// to something the node cannot hold. It bounds a path that only runs at boot, and only for
/// the handful of indices that were mid-write when the process stopped.
pub(crate) static RECOVERY_GATE: LazyLock<RecoveryGate> = LazyLock::new(|| RecoveryGate {
    permits: Mutex::new(
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .clamp(1, 4),
    ),
    released: Condvar::new(),
});

/// Opens the Tantivy index at `path` with this engine's tokenizers registered.
pub(crate) fn open_tantivy_index(path: &Path) -> Result<Index, StoreError> {
    let index = Index::open_in_dir(path)?;
    register_tokenizers(&index);
    Ok(index)
}

/// Creates a Tantivy index at `path` with this engine's tokenizers registered.
pub(crate) fn create_tantivy_index(path: &Path, schema: Schema) -> Result<Index, StoreError> {
    let index = Index::create_in_dir(path, schema)?;
    register_tokenizers(&index);
    Ok(index)
}

/// Resolve an index name to its on-disk directory under `indices_base`.
///
/// The index name must be exactly one normal path component. This is a purely
/// lexical check — it needs no filesystem access, so it holds for indexes that
/// do not exist yet (the case where a traversal attempt would otherwise create
/// a directory outside the shard). Rejects `..`, `.`, absolute paths, path
/// separators, Windows prefixes, and the empty string.
pub(crate) fn resolve_index_dir(indices_base: &Path, index: &str) -> Result<PathBuf, StoreError> {
    let mut components = Path::new(index).components();
    let is_single_normal_component = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    );
    if !is_single_normal_component {
        return Err(StoreError::InvalidIndexName(format!(
            "'{}' must be a single path component without separators or '..'",
            index
        )));
    }
    Ok(indices_base.join(index))
}

/// Build the per-field `InvertedIndexReader`s (term dictionaries) a query needs, so the first
/// real query pays for neither opening them nor the page faults behind them.
///
/// Only inverted indexes are warmed, because in tantivy 0.26 they are the only thing a
/// `SegmentReader` keeps: `inverted_index()` memoizes into `inv_idx_reader_cache`, so every
/// later query on this generation reuses the work. The two obvious neighbours do not memoize
/// and were dropped from this function:
///
/// - fast fields — `FastFieldReaders::u64()` and friends go through `read_columns()`, which
///   re-reads the columnar on every call; nothing is cached to hand the next query.
/// - the doc store — `SegmentReader::get_store_reader()` returns a *fresh* `StoreReader` with
///   its own block cache, dropped the moment warming returns, and the searcher builds its own
///   store readers regardless.
///
/// Warming those two bought page-cache residency and nothing else — which a segment this
/// process just wrote already has. What remains here is per *searcher generation*, not per
/// segment: tantivy rebuilds every `SegmentReader` on reload, so the cache dies with the
/// generation that owned it.
pub(crate) fn warm_segment(index: &str, segment_reader: &tantivy::SegmentReader) {
    for (field, field_entry) in segment_reader.schema().fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        // Builds and caches this field's InvertedIndexReader (term dictionary).
        if let Err(e) = segment_reader.inverted_index(field) {
            trace!(
                index = %index,
                field = field_entry.name(),
                error = %e,
                "Warmup: could not open inverted index for field"
            );
        }
    }
}

/// An index this shard is holding open, and the tick at which it was last used.
pub(crate) struct OpenIndex {
    last_used: AtomicU64,
}

/// Multi-tenant hybrid storage engine combining redb and tantivy.
pub struct HybridStore {
    /// Shared redb database across all indices
    pub(crate) kv: Database,
    /// Cache of IndexWriters keyed by index name
    pub(crate) writers: Arc<DashMap<String, Arc<Mutex<IndexWriter>>>>,
    /// Cache of IndexReaders keyed by index name
    pub(crate) readers: Arc<DashMap<String, IndexReader>>,
    /// Atomic counters for WAL sequence IDs per index
    pub(crate) current_seq: Arc<DashMap<String, AtomicU64>>,
    /// Operation counters for smart commits per index
    pub(crate) operations_counter: Arc<DashMap<String, AtomicU64>>,
    /// When each index's oldest uncommitted operation was counted, in milliseconds after
    /// `commit_clock_epoch`, or [`NOTHING_PENDING`]. What `commit_interval_ms` is measured
    /// from; see [`Self::should_commit_writer`].
    pub(crate) pending_since: Arc<DashMap<String, AtomicU64>>,
    /// The origin `pending_since` is measured from.
    pub(crate) commit_clock_epoch: std::time::Instant,
    /// Cache of optimal memory budgets per index to avoid frequent syscalls.
    /// See [`BudgetCacheEntry`] for why it carries a timestamp rather than a bare number.
    pub(crate) budget_cache: Arc<DashMap<String, BudgetCacheEntry>>,
    /// Cache of schemas per index to avoid repeated redb reads
    pub(crate) schema_cache: Arc<DashMap<String, Arc<IndexSchema>>>,
    /// Cache of Tantivy field mappings per index
    pub(crate) fields_cache: Arc<DashMap<String, SchemaFields>>,
    /// Per-index initialization locks, serializing concurrent `get_or_create_index` calls.
    /// Tantivy's `INDEX_WRITER_LOCK` is a non-blocking flock on `.tantivy-writer.lock`, so
    /// two threads opening a writer for the same index race and one fails with `LockBusy`.
    pub(crate) index_init_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    /// Per-index schema locks, serializing every read-modify-write of an index's schema row.
    /// See [`Self::lock_schema`].
    pub(crate) schema_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    /// Searcher generation last warmed, per index. A searcher whose generation is unchanged
    /// holds the same `SegmentReader`s with the same filled caches, so re-warming it is
    /// pointless — this makes repeated warm requests for an idle index free.
    pub(crate) warmed_generations: Arc<DashMap<String, u64>>,
    /// Per-index warmup lifecycle state, for observability.
    pub(crate) warmup_states: Arc<DashMap<String, IndexWarmupState>>,
    /// Unified cache for index sizes (Tantivy + Redb) with expiration to avoid repeated
    /// expensive calculations. Keyed by `(include_data_size, index)` rather than a formatted
    /// string: this cache lives per shard, so the shard path in the old string key was
    /// redundant, and the string form made invalidation a substring match — evicting index
    /// `"a"` also evicted `"ab"` and `"aa"`. The tuple key makes invalidation exact.
    pub(crate) index_size_cache: Arc<Mutex<HashMap<(bool, String), IndexSizeCache>>>,
    /// The indexes this shard currently holds open, and when each was last used.
    ///
    /// The eleventh map, and the one that bounds the other ten. An index earns an entry when
    /// something opens a writer or a reader for it and loses it when [`Self::close_index`]
    /// drops the set, so `len()` is the open-index count and the entry is where LRU reads its
    /// ordering from. Bounded by construction: `max_open_indexes` is what it is a count of.
    pub(crate) open_indexes: Arc<DashMap<String, OpenIndex>>,
    /// Monotonic counter stamped onto an index each time it is used.
    ///
    /// A counter and not a clock. LRU needs an ordering and nothing more, and an atomic
    /// increment is free next to the work it is ordering, while `Instant::now` on the hot read
    /// path is the mistake M0-j already caught once.
    pub(crate) open_tick: Arc<AtomicU64>,
    /// Largest number of indexes *this shard* may hold open; `0` means no cap.
    ///
    /// The node-level `StorageConfig::max_open_indexes` divided by the shard count, so the
    /// figure an operator sets is the one the node obeys.
    pub(crate) max_open_indexes: usize,
    /// Cache expiration duration for index sizes (1 hour)
    pub(crate) index_cache_expiry: Duration,
    /// Storage configuration
    pub(crate) config: StorageConfig,
}

impl HybridStore {
    /// Calculate tiered cache sizes based on database file size, system memory, and shard count.
    /// Returns the normal cache size in bytes.
    ///
    /// Memory is divided by max_shards to ensure we don't exceed system limits
    /// when multiple shards are initialized on the same node.
    pub(crate) fn calculate_cache_size(
        config: &StorageConfig,
        db_file_size_bytes: u64,
        total_shards: usize,
    ) -> usize {
        use sysinfo::{MemoryRefreshKind, System};

        const MIN_CACHE_BYTES: u64 = 32 * 1024 * 1024; // 32MB safety floor per shard

        let shard_count = total_shards.max(1) as u64;

        // Use configured total limit when provided, otherwise fall back to host memory stats
        let (total_memory_bytes, available_memory_bytes) = if config.total_memory_limit_bytes > 0 {
            let pressure = config.memory_pressure_threshold_percent.clamp(1, 100) as u64;
            let total = config.total_memory_limit_bytes;
            let available = total.saturating_mul(pressure) / 100;
            (total, available.max(MIN_CACHE_BYTES * shard_count))
        } else {
            let mut system = System::new();
            system.refresh_memory_specifics(MemoryRefreshKind::everything());
            let total = system.total_memory();
            let available = if system.available_memory() > 0 {
                system.available_memory()
            } else {
                total / 4
            };
            (total, available)
        };

        let cache_pool_bytes = (available_memory_bytes / 4).max(MIN_CACHE_BYTES * shard_count);
        let total_pool_bytes = (total_memory_bytes / 2).max(MIN_CACHE_BYTES * shard_count);

        let per_shard_available = cache_pool_bytes / shard_count;
        let per_shard_total = total_pool_bytes / shard_count;

        // Base standard cache sizes by database tier (before per-shard limits)
        let base_standard_cache = if db_file_size_bytes < 1024 * 1024 {
            32 * 1024 * 1024
        } else if db_file_size_bytes < 100 * 1024 * 1024 {
            64 * 1024 * 1024
        } else if db_file_size_bytes < 1024 * 1024 * 1024 {
            128 * 1024 * 1024
        } else {
            256 * 1024 * 1024
        };

        // Apply per-shard memory caps
        let standard_cache = (base_standard_cache as u64)
            .min(per_shard_available)
            .min(per_shard_total) as usize;

        tracing::info!(
            file_size_mb = db_file_size_bytes / (1024 * 1024),
            available_memory_mb = available_memory_bytes / (1024 * 1024),
            total_memory_mb = total_memory_bytes / (1024 * 1024),
            max_shards = shard_count,
            per_shard_available_mb = per_shard_available / (1024 * 1024),
            standard_cache_mb = standard_cache / (1024 * 1024),
            "HybridStore: calculated cache size (per-shard)"
        );

        standard_cache
    }

    /// Creates a new multi-tenant HybridStore with per-shard cache sizing.
    /// Cache size is divided by total_shards to prevent OOM when multiple
    /// shards are initialized on the same node.
    ///
    /// # Arguments
    /// * `config` - Storage configuration
    /// * `total_shards` - Total number of shards on this node (for per-shard memory budgeting)
    pub fn new(config: StorageConfig, total_shards: usize) -> Result<Self, StoreError> {
        let init_start = Instant::now();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            "HybridStore: initializing shard storage with tiered cache"
        );

        // Create directory structure
        let dir_start = Instant::now();
        fs::create_dir_all(&config.shard_path)?;
        let kv_path = config.shard_path.join("store.redb");
        let indices_path = config.shard_path.join("indices");
        fs::create_dir_all(&indices_path)?;
        let dir_elapsed = dir_start.elapsed();
        tracing::debug!(
            shard_path = %config.shard_path.display(),
            indices_path = %indices_path.display(),
            elapsed_ms = dir_elapsed.as_millis(),
            "HybridStore: ensured directory structure"
        );

        let db_file_exists = kv_path.exists();
        let db_file_size = if db_file_exists {
            fs::metadata(&kv_path)?.len()
        } else {
            0
        };

        // Calculate tiered cache sizes based on file size and shard count
        let normal_cache_size = Self::calculate_cache_size(&config, db_file_size, total_shards);

        let kv = if db_file_exists {
            // EXISTING DATABASE: Open directly with normal cache.
            // The init boost cache was removed — recovery now uses persisted
            // committed seq from TABLE_RECOVERY_META, so no large cache is
            // needed for metadata loading during startup.
            tracing::info!(
                db_path = %kv_path.display(),
                normal_cache_mb = normal_cache_size / (1024 * 1024),
                "HybridStore: Opening existing database"
            );

            let mut builder = redb::Builder::new();
            builder.set_cache_size(normal_cache_size);
            builder.open(&kv_path)?
        } else {
            // NEW DATABASE: Just create with normal cache
            tracing::info!(
                db_path = %kv_path.display(),
                cache_mb = normal_cache_size / (1024 * 1024),
                "HybridStore: Creating new database with standard cache"
            );

            let mut builder = redb::Builder::new();
            builder.set_cache_size(normal_cache_size);
            builder.create(&kv_path)?
        };

        let total_elapsed = init_start.elapsed();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            db_path = %kv_path.display(),
            existed = db_file_exists,
            file_size_mb = db_file_size / (1024 * 1024),
            normal_cache_mb = normal_cache_size / (1024 * 1024),
            elapsed_ms = total_elapsed.as_millis(),
            "HybridStore: initialization complete"
        );

        Ok(HybridStore {
            kv,
            writers: Arc::new(DashMap::new()),
            readers: Arc::new(DashMap::new()),
            current_seq: Arc::new(DashMap::new()),
            operations_counter: Arc::new(DashMap::new()),
            pending_since: Arc::new(DashMap::new()),
            commit_clock_epoch: std::time::Instant::now(),
            budget_cache: Arc::new(DashMap::new()),
            schema_cache: Arc::new(DashMap::new()),
            fields_cache: Arc::new(DashMap::new()),
            index_init_locks: Arc::new(DashMap::new()),
            schema_locks: Arc::new(DashMap::new()),
            warmed_generations: Arc::new(DashMap::new()),
            warmup_states: Arc::new(DashMap::new()),
            index_size_cache: Arc::new(Mutex::new(HashMap::new())),
            open_indexes: Arc::new(DashMap::new()),
            open_tick: Arc::new(AtomicU64::new(0)),
            max_open_indexes: Self::per_shard_open_index_cap(config.max_open_indexes, total_shards),
            index_cache_expiry: Duration::from_secs(3600), // 1 hour
            config: config.clone(),
        })
    }

    /// Gracefully shutdown the HybridStore, releasing all locks and resources
    pub fn shutdown(&self) -> Result<(), StoreError> {
        tracing::info!("HybridStore: Starting graceful shutdown");

        // Check which indices have pending operations
        let indices_with_pending_ops: Vec<String> = self
            .operations_counter
            .iter()
            .filter(|entry| entry.value().load(Ordering::SeqCst) > 0)
            .map(|entry| entry.key().clone())
            .collect();

        if indices_with_pending_ops.is_empty() {
            tracing::info!("No pending operations, skipping commits during shutdown");
        } else {
            tracing::info!(
                indices_count = indices_with_pending_ops.len(),
                indices = ?indices_with_pending_ops,
                "Committing indices with pending operations during shutdown"
            );
        }

        // Commit only writers with pending operations
        for entry in self.writers.iter() {
            let index = entry.key();
            let writer_arc = entry.value();
            if indices_with_pending_ops.contains(index) {
                // Capture the sequence before committing — see commit_index for why the
                // checkpoint must never claim a sequence allocated after the commit started.
                let committed_seq = self
                    .current_seq
                    .get(index)
                    .map(|counter| counter.load(Ordering::SeqCst));

                // Retry with 5s timeout to handle slow writer thread lock release
                let writer = {
                    let start = std::time::Instant::now();
                    let timeout = std::time::Duration::from_secs(5);
                    loop {
                        match writer_arc.try_lock() {
                            Ok(guard) => break Some(guard),
                            Err(_) if start.elapsed() < timeout => {
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                            Err(_) => {
                                tracing::error!(index = %index, "Writer lock timeout during shutdown, skipping commit — data may be lost");
                                break None;
                            }
                        }
                    }
                };
                if let Some(mut w) = writer {
                    tracing::debug!(index = %index, "Committing index during shutdown");
                    let outcome = match committed_seq {
                        Some(seq) => commit_writer_at(&mut w, seq).map(|()| 0),
                        None => w.commit().map_err(StoreError::from),
                    };
                    match outcome {
                        Ok(_) => {
                            // Release the writer lock before touching redb.
                            drop(w);
                            // Checkpoint what we just made durable. Without this the next
                            // startup sees a stale recovery sequence and replays the entire
                            // tail of the WAL even though it is all already in Tantivy.
                            if let Some(seq) = committed_seq
                                && let Err(e) = self.checkpoint_committed(index, seq)
                            {
                                tracing::warn!(
                                    index = %index,
                                    error = %e,
                                    "Failed to checkpoint on shutdown; next startup will replay the WAL tail"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(index = %index, error = %e, "Failed to commit index during shutdown");
                        }
                    }
                }
            } else {
                tracing::debug!(index = %index, "No pending operations, skipping commit during shutdown");
            }
        }

        // Release what holds file handles, rather than leaving it to whenever the last `Arc`
        // to this store happens to drop: `readers` holds every open index's mmaps and
        // `writers` holds tantivy's `.tantivy-writer.lock` flock. Dropping an `IndexWriter`
        // joins its merge threads, so that cost lands here — inside the caller's shutdown
        // timeout — instead of on whichever thread outlives it.
        self.writers.clear();
        self.readers.clear();

        // Clear all caches
        self.schema_cache.clear();
        self.budget_cache.clear();
        self.operations_counter.clear();
        self.pending_since.clear();
        self.current_seq.clear();
        self.index_size_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();

        // Force a final redb fsync/flush to reduce WAL replay on startup
        let redb_start = std::time::Instant::now();
        match self.kv.begin_write() {
            Ok(mut txn) => {
                if let Err(e) = txn.set_durability(Durability::Immediate) {
                    tracing::warn!(error = %e, "Failed to set durability on shutdown flush");
                } else if let Err(e) = txn.commit() {
                    tracing::warn!(error = %e, "Failed to commit shutdown flush transaction");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to open shutdown flush transaction");
            }
        }
        let redb_elapsed = redb_start.elapsed();
        if redb_elapsed > std::time::Duration::from_secs(10) {
            tracing::warn!(elapsed = ?redb_elapsed, "Redb shutdown flush exceeded 10s");
        } else {
            tracing::debug!(elapsed = ?redb_elapsed, "Redb shutdown flush completed");
        }

        tracing::info!("HybridStore: Graceful shutdown completed");
        Ok(())
    }

    /// Whether an `IndexWriter` is currently open for `index`.
    ///
    /// A writer is the expensive resource in this engine — worker threads plus an indexing
    /// arena — so this answers "did that operation have to open one", which is the difference
    /// between a boot that scales with in-flight writes and one that scales with stored data.
    pub fn has_open_writer(&self, index: &str) -> bool {
        self.writers.contains_key(index)
    }

    /// Forcefully remove a writer from cache, even if locked.
    /// Last-resort operation for stuck writers. WARNING: May cause data loss.
    /// Returns true if writer was removed, false if not found.
    pub fn force_remove_writer(&self, index: &str) -> bool {
        if let Some((_, writer_arc)) = self.writers.remove(index) {
            if writer_arc.try_lock().is_ok() {
                tracing::warn!(index = %index, "Force-removing writer (lock available)");
            } else {
                tracing::error!(index = %index, "Force-removing LOCKED writer - data loss possible");
            }
            drop(writer_arc);
            true
        } else {
            tracing::debug!(index = %index, "No writer to force-remove");
            false
        }
    }

    /// Drop every in-memory structure this shard holds for `index`, leaving the disk alone.
    ///
    /// **This is the unit of eviction, and the reason it is a method rather than a line.**
    /// `writers` is the entry worth evicting — it owns the indexing arena and the
    /// `indexer_num_threads + merge_num_threads` OS threads that come with it — but it is one
    /// of ten maps keyed by index name. Dropping it alone leaves the other nine resident, and
    /// `readers` is not a small one: it holds an `IndexReader` with its segment readers and
    /// their fast-field caches. An index is closed when its whole set is, or it is not closed.
    ///
    /// Nine of the ten go here, along with the size cache and this shard's record that the
    /// index is open. `index_init_locks` is the deliberate exception, for the reason stated
    /// inline — it is a lock, not a cache, and dropping it would unserialize the thing it
    /// serializes.
    ///
    /// Callers that are *evicting* must commit first — see `close_index`. Callers that are
    /// *deleting* must not, and call this directly.
    fn drop_index_caches(&self, index: &str) {
        self.writers.remove(index);
        self.readers.remove(index);
        self.current_seq.remove(index);
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        self.budget_cache.remove(index);
        // The operations counter tracks documents buffered in the writer we just dropped.
        // Leaving it non-zero makes the next commit_index for this name believe there is
        // unflushed data.
        self.operations_counter.remove(index);
        self.pending_since.remove(index);
        // Warmup is invalidated by generation equality, and this name's next reader starts
        // its generation counter from zero. Dropping both entries is what makes the next
        // warm actually run, and stops the name reporting warm while it holds no data.
        self.warmed_generations.remove(index);
        self.warmup_states.remove(index);
        // Note: index_init_locks is deliberately not cleared. A concurrent
        // get_or_create_index may be holding the lock, and replacing it here would let a
        // later caller initialize the same index in parallel with that holder.
        self.open_indexes.remove(index);

        self.invalidate_size_cache(index);
    }

    /// This shard's share of the node-wide open-index cap.
    ///
    /// Rounded *up*, and never to zero: a node configured for fewer open indexes than it has
    /// shards would otherwise give each shard a cap of zero, and a cap of zero reads as "no
    /// cap" everywhere else in this file. One per shard is the smallest honest answer.
    fn per_shard_open_index_cap(node_cap: usize, total_shards: usize) -> usize {
        if node_cap == 0 {
            return 0;
        }
        let shards = total_shards.max(1);
        node_cap.div_ceil(shards).max(1)
    }

    /// Record that `index` was used, if this shard is holding it open.
    ///
    /// Takes the map's shared guard rather than an exclusive one: the stamp is an atomic store
    /// through `&OpenIndex`, so concurrent readers of different indexes never queue behind each
    /// other, and two uses of the same index racing to stamp it is a race whose outcome is
    /// "recently used" either way.
    pub(crate) fn touch_open_index(&self, index: &str) {
        if let Some(entry) = self.open_indexes.get(index) {
            let tick = self.open_tick.fetch_add(1, Ordering::Relaxed);
            entry.value().last_used.store(tick, Ordering::Relaxed);
        }
    }

    /// Admit `index` to the open set, closing colder indexes first if it is full.
    ///
    /// Called on the path that is *about to* open a writer or a reader, so the admission and
    /// the work it accounts for cannot drift apart.
    ///
    /// **The cap is enforced, not guaranteed, and the difference is deliberate.** A victim is
    /// skipped when its writer mutex is not free, because taking it here would mean blocking
    /// an opener on somebody else's commit — and, in the one case that matters, blocking it on
    /// a lock the *calling thread* already holds, which is a deadlock rather than a delay.
    /// When no victim can be closed the index is admitted anyway and the overshoot is logged:
    /// exceeding the cap is recoverable and the next admission will try again, while a
    /// deadlocked writer thread is not.
    pub(crate) fn admit_open_index(&self, index: &str) {
        let tick = self.open_tick.fetch_add(1, Ordering::Relaxed);

        if let Some(entry) = self.open_indexes.get(index) {
            entry.value().last_used.store(tick, Ordering::Relaxed);
            return;
        }

        if self.max_open_indexes > 0 {
            // A victim can be taken by a write between being chosen and being closed, and is
            // then skipped; bounded so a shard whose every index keeps getting written cannot
            // hold this caller in the loop.
            let mut skips_left = self.open_indexes.len();
            // `>=` because this call is about to add one.
            while self.open_indexes.len() >= self.max_open_indexes {
                match self.coldest_closable_index(index) {
                    Some(victim) if self.close_index(&victim) => {}
                    Some(_) if skips_left > 0 => skips_left -= 1,
                    _ => {
                        tracing::warn!(
                            index = %index,
                            open = self.open_indexes.len(),
                            cap = self.max_open_indexes,
                            "Open-index cap exceeded: every colder index is busy"
                        );
                        break;
                    }
                }
            }
        }

        self.open_indexes.insert(
            index.to_string(),
            OpenIndex {
                last_used: AtomicU64::new(tick),
            },
        );
    }

    /// The least recently used open index that can be closed without waiting on anyone.
    ///
    /// `exclude` is the index being admitted, which must never be its own victim. An index
    /// whose writer mutex is held is passed over rather than waited for; see
    /// [`Self::admit_open_index`] for why that is the whole point.
    fn coldest_closable_index(&self, exclude: &str) -> Option<String> {
        let mut coldest: Option<(u64, String)> = None;
        for entry in self.open_indexes.iter() {
            let name = entry.key();
            if name == exclude {
                continue;
            }
            if let Some(writer) = self.writers.get(name)
                && writer.value().try_lock().is_err()
            {
                continue;
            }
            // And not one that is being opened right now. `get_or_create_index` admits its
            // index to the open set before it finishes building the writer, so closing a name
            // whose init lock is held would drop caches the holder is about to repopulate and
            // leave it open but unaccounted — a hole in the very count this cap is of.
            if let Some(init) = self.index_init_locks.get(name)
                && init.value().try_lock().is_err()
            {
                continue;
            }
            let used = entry.value().last_used.load(Ordering::Relaxed);
            if coldest.as_ref().is_none_or(|(best, _)| used < *best) {
                coldest = Some((used, name.clone()));
            }
        }
        coldest.map(|(_, name)| name)
    }

    /// How many indexes this shard is holding open — the number `max_open_indexes` bounds.
    pub fn open_index_count(&self) -> usize {
        self.open_indexes.len()
    }

    /// This shard's share of the open-index cap; `0` when uncapped.
    pub fn open_index_cap(&self) -> usize {
        self.max_open_indexes
    }

    /// Whether this shard currently holds `index` open. Says nothing about whether the index
    /// exists: a closed index and an absent one look the same from memory, which is the point.
    pub fn is_index_open(&self, index: &str) -> bool {
        self.open_indexes.contains_key(index)
    }

    /// Commit what `index` holds and drop every structure this shard keeps for it.
    ///
    /// The eviction path, and the inverse of opening one. The data on disk is untouched: the
    /// next reference to this name reopens it and replays whatever the commit did not capture.
    ///
    /// A failed commit evicts anyway, which is the behaviour the admin eviction endpoint
    /// already has and `writer_eviction_test` already pins: the documents are in redb's WAL
    /// until a commit checkpoints past them, so dropping an uncommitted writer costs a replay
    /// on reopen rather than the documents.
    ///
    /// Runs on whichever thread is admitting another index, not on this one's writer thread,
    /// so it takes the writer only if it is free and holds it from the commit through dropping
    /// the caches: a write either finished before (and is in the commit) or starts after (and
    /// finds the writer gone, see `lock_live_writer`). Returns `false`, closing nothing, when a
    /// write holds the writer — the caller picks another victim rather than wait on it.
    pub fn close_index(&self, index: &str) -> bool {
        let writer_arc = self
            .writers
            .get(index)
            .map(|writer| Arc::clone(writer.value()));
        let Some(writer_arc) = writer_arc else {
            // Open for reading only: nothing buffered, nothing to commit.
            self.drop_index_caches(index);
            tracing::debug!(index = %index, "Index closed");
            return true;
        };
        let mut writer = match writer_arc.try_lock() {
            Ok(writer) => writer,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };

        if self.get_operations_count(index) > 0 {
            let committed = self
                .commit_locked_writer(index, &mut writer)
                .and_then(|seq| self.checkpoint_after_commit(index, seq));
            if let Err(e) = committed {
                tracing::warn!(
                    index = %index,
                    error = %e,
                    "Close: commit failed, closing anyway; the WAL still holds what it buffered"
                );
            }
        }
        self.drop_index_caches(index);
        drop(writer);
        tracing::debug!(index = %index, "Index closed");
        true
    }

    /// Highest `_seq` present in the index, found by ordering on the `_seq` fast field.
    ///
    /// Last resort only. Despite returning a single document this reads the `_seq` column of
    /// every segment, so it costs O(segments × docs) — on a multi-terabyte index, minutes.
    /// It is reachable for one index at most once: an index that still has a WAL tail and
    /// whose last commit carries neither a payload stamp nor a `_recovery_meta` row. The
    /// commit that ends that replay stamps a payload, and the path is dead for that index
    /// from then on.
    pub(crate) fn get_highest_indexed_seq(&self, tantivy_index: &Index) -> Result<u64, StoreError> {
        let reader: IndexReader = tantivy_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let schema = searcher.index().schema();
        let doc_count = searcher.num_docs();

        // Get the _seq field from schema
        let seq_field = schema
            .get_field("_seq")
            .map_err(|_| StoreError::FieldNotFound("_seq field missing".to_string()))?;

        // Get one document sorted by _seq descending to find the highest value
        let top_collector = TopDocs::with_limit(1).order_by_u64_field("_seq", Order::Desc);
        let top_docs = searcher.search(&AllQuery, &top_collector)?;

        tracing::debug!(
            doc_count = doc_count,
            top_docs_len = top_docs.len(),
            "get_highest_indexed_seq: Tantivy search completed"
        );

        // CRITICAL: The sort key returned by order_by_u64_field is u64::MAX - actual_value
        // We must retrieve the actual _seq value from the document's stored fields
        if let Some((inverted_sort_key, doc_address)) = top_docs.first() {
            let doc: tantivy::TantivyDocument = searcher.doc(*doc_address)?;
            if let Some(value) = doc.get_first(seq_field)
                && let Some(seq) = value.as_u64()
            {
                tracing::debug!(
                    inverted_sort_key = ?inverted_sort_key,
                    actual_seq = seq,
                    doc_address = ?doc_address,
                    "get_highest_indexed_seq: Retrieved actual _seq from stored fields"
                );
                return Ok(seq);
            } else {
                // The fast field orders this document as the highest `_seq`, and the stored
                // fields cannot read a value on it. Nothing here can reconstruct one the
                // caller could trust, and a wrong checkpoint seeds a replay window — so the
                // scan fails, and with it the index open that asked.
                return Err(StoreError::CorruptIndex(format!(
                    "the top document by _seq ({inverted_sort_key:?} at {doc_address:?}) \
                     carries no stored _seq value to read"
                )));
            }
        } else {
            tracing::debug!("get_highest_indexed_seq: No documents found in index");
        }

        // No documents found, return 0
        Ok(0)
    }

    /// The WAL sequence Tantivy has durably indexed for `index`.
    ///
    /// Three sources, in descending order of trust:
    ///
    /// 1. The Tantivy commit payload, written inside the commit, so it cannot describe a
    ///    segment set that is not on disk.
    /// 2. `_recovery_meta`, written in the transaction that truncates the WAL right after a
    ///    successful commit. It can lag the payload by one commit but never lead it, because
    ///    nothing writes it until the commit it describes has returned.
    /// 3. The `_seq` fast field, scanned. Correct but expensive, available only on an index
    ///    old enough to carry the field, and needed only when the two above have no answer.
    ///
    /// Taking the maximum is safe precisely because none of the three can report a sequence
    /// Tantivy does not have, and it keeps a build transition from replaying a tail twice.
    pub(crate) fn checkpoint_seq(
        &self,
        index: &str,
        tantivy_index: &Index,
    ) -> Result<u64, StoreError> {
        let stamped = tantivy_checkpoint_seq(tantivy_index);
        let persisted = self.get_persisted_committed_seq(index)?;

        if let Some(seq) = stamped.into_iter().chain(persisted).max() {
            tracing::debug!(
                index = %index,
                stamped = ?stamped,
                persisted = ?persisted,
                checkpoint_seq = seq,
                "Resolved Tantivy checkpoint without scanning the index"
            );
            return Ok(seq);
        }

        // No `_seq` column, so there is nothing to scan and nothing to find. An index built
        // without the field is one built after commits started carrying a payload, so the only
        // way to reach here is an index that has never committed — which has nothing indexed,
        // and whose checkpoint is therefore 0.
        if tantivy_index.schema().get_field("_seq").is_err() {
            tracing::debug!(
                index = %index,
                "No checkpoint recorded and no _seq column; index has never been committed"
            );
            return Ok(0);
        }

        // Neither cheap source exists. Either this index was last committed by a build
        // predating both, or it has never been committed at all — a freshly created index
        // looks identical here, and the scan is what distinguishes them.
        let scan_start = Instant::now();
        let scanned = self.get_highest_indexed_seq(tantivy_index)?;

        if scanned == 0 {
            // Nothing indexed, so there is no checkpoint to remember and the scan was free.
            // This is the ordinary path for an index that was just created.
            tracing::debug!(index = %index, "Index has no indexed documents; checkpoint is 0");
            return Ok(0);
        }

        // Record what the scan found, so it is a one-time cost for this index rather than
        // something every open repeats: the stamped payload only appears on the *next*
        // commit, and an index that is read but never written again would otherwise pay the
        // scan forever.
        tracing::warn!(
            index = %index,
            checkpoint_seq = scanned,
            elapsed_ms = scan_start.elapsed().as_millis(),
            "No commit payload and no recovery metadata; scanned the _seq field to locate the \
             checkpoint and backfilled it. This index will not scan again."
        );

        if let Err(e) = self.persist_committed_seq(index, scanned) {
            tracing::warn!(
                index = %index,
                error = %e,
                "Could not backfill the recovery checkpoint; the next open will scan again"
            );
        }

        Ok(scanned)
    }

    /// Replay into Tantivy the writes redb has committed but Tantivy has not.
    ///
    /// redb is the ACID source of truth and the Tantivy index is derived from it, so the only
    /// state that can be stale after an unclean stop is Tantivy's, and only by the writes redb
    /// made durable after Tantivy's last commit. That difference is exactly the `wal_<index>`
    /// entries above [`Self::checkpoint_seq`], and it is all this touches — both engines have
    /// already finished their own recovery by the time it runs, and no part of the corpus is
    /// read to work out where to start.
    ///
    /// The tail is small by construction: [`Self::checkpoint_committed`] deletes every WAL
    /// entry a commit covers, so what remains is one commit interval's worth of writes at
    /// most, whatever the size of the index underneath.
    ///
    /// Idempotent. Every replayed put deletes its `id` term before adding the document, so
    /// replaying a range Tantivy already has changes nothing, which is what lets recovery
    /// resume from a checkpoint that is behind rather than needing one that is exact.
    ///
    /// Returns `(replayed_count, max_wal_seq, checkpoint_seq)` so the caller can seed the
    /// sequence counter without repeating the lookups.
    pub(crate) fn recover_index(
        &self,
        index: &str,
        writer: &mut IndexWriter,
        tantivy_index: &Index,
    ) -> Result<(usize, u64, u64), StoreError> {
        let max_wal_seq = self.get_max_wal_id_for_index(index)?;

        if max_wal_seq == 0 {
            // Nothing is waiting. Read the checkpoint anyway, and fail the open if it cannot
            // be read: it is the durable high-water mark of the sequence counter, and
            // defaulting it to zero would hand out sequence numbers this index has already
            // used — which a later crash reads as a tail already covered and never replays.
            let checkpoint = self.checkpoint_seq(index, tantivy_index)?;
            tracing::debug!(
                index = %index,
                checkpoint_seq = checkpoint,
                "WAL tail is empty; index is in sync with redb"
            );
            return Ok((0, 0, checkpoint));
        }

        let last_committed_seq = self.checkpoint_seq(index, tantivy_index)?;

        // If all sequences are committed, nothing to recover
        if last_committed_seq >= max_wal_seq {
            tracing::info!(
                index = %index,
                checkpoint_seq = last_committed_seq,
                max_wal_seq = max_wal_seq,
                "WAL tail is already indexed; truncating it and skipping replay"
            );

            // Finish the truncation the commit that indexed these entries did not get to.
            // A crash between the Tantivy commit and `checkpoint_committed` leaves entries
            // behind that the checkpoint already covers, and without this they stay: the
            // partition in `recover_indices` reads a non-empty WAL as "needs recovery", so
            // the index would open a writer to discover there is nothing to do on *every*
            // subsequent boot, and would only stop once it happened to take another write.
            // The checkpoint proves these entries are in Tantivy, which is exactly the
            // precondition `checkpoint_committed` asks of its caller.
            if let Err(e) = self.checkpoint_committed(index, last_committed_seq) {
                tracing::warn!(
                    index = %index,
                    error = %e,
                    "Could not truncate the already-indexed WAL tail; recovery will look at it again next boot"
                );
            }

            return Ok((0, max_wal_seq, last_committed_seq));
        }

        // Start recovery from the first missing sequence
        let range_start = last_committed_seq + 1;

        tracing::info!(
            index = %index,
            range_start = range_start,
            max_wal_seq = max_wal_seq,
            pending = max_wal_seq - last_committed_seq,
            "Replaying the WAL tail redb committed after Tantivy's last commit"
        );

        // Start a read transaction on Redb
        let read_txn = self.kv.begin_read()?;
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let wal_table = match read_txn.open_table(wal_table_def) {
            Ok(table) => table,
            Err(_) => {
                tracing::debug!(index = %index, "No WAL table found");
                return Ok((0, max_wal_seq, last_committed_seq));
            }
        };

        // The document bodies, read from the same snapshot as the WAL so the two cannot
        // disagree about what was committed.
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let data_table = read_txn.open_table(data_table_def).ok();

        // Get schema for building documents
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let schema = tantivy_index.schema();
        let id_field = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;
        // Absent on any index built without it. See `SchemaFields::seq`.
        let seq_field = schema.get_field("_seq").ok();

        // Build indexed fields map
        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name != "id" && name != "_seq" {
                indexed_fields.insert(name.to_string(), field);
            }
        }

        // Replay commits are checkpoints, not throughput throttles: size them well above the
        // steady-state write threshold so a large WAL produces a handful of big segments
        // rather than one small segment per batch.
        let recovery_commit_threshold = self.recovery_commit_threshold();

        // Zero-copy WAL replay: iterate range directly, process each entry in-place.
        // AccessGuard::value() returns &[u8] pointing directly into redb's mmap'd pages,
        // avoiding the need to allocate a Vec<u8> for every entry.
        //
        // Each id is applied once. A tail that touched the same document repeatedly — the
        // shape a bursty updater produces, and the one most likely to be long — collapses to
        // one Tantivy operation per distinct id, because every entry for that id resolves to
        // the same committed row. Applying at the first occurrence rather than the last is
        // what keeps the mid-replay checkpoints meaningful: everything below the stamped
        // sequence really has been applied.
        let mut replayed_count = 0;
        let mut replayed_since_commit = 0u64;
        let mut skipped_duplicates = 0usize;
        let mut applied_ids: HashSet<String> = HashSet::new();

        for result in wal_table.range(range_start..=max_wal_seq)? {
            let (seq_guard, wal_data_guard) = result?;
            let seq_id = seq_guard.value();
            let id = decode_wal_entry(wal_data_guard.value())?;

            if !applied_ids.insert(id.clone()) {
                skipped_duplicates += 1;
                replayed_count += 1;
                replayed_since_commit += 1;
                continue;
            }

            // The committed state of this id decides the operation. A row means the document
            // stands as written; no row means it was deleted. Nothing else can be true — a put
            // always writes the row and a delete always removes it, in the same transaction
            // that appended the WAL entry being read here.
            let stored = match data_table.as_ref() {
                Some(table) => table.get(id.as_str())?,
                None => None,
            };

            match stored {
                Some(doc_guard) => {
                    let stored_doc: StoredDocOwned = serde_json::from_slice(doc_guard.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut tantivy_doc = tantivy::TantivyDocument::default();
                    tantivy_doc.add_text(id_field, &id);
                    if let Some(seq_field) = seq_field {
                        tantivy_doc.add_u64(seq_field, seq_id);
                    }

                    if let Some(json_obj) =
                        stored_doc.json_blob.as_ref().and_then(|v| v.as_object())
                    {
                        for (field_name, field_def) in &index_schema.fields {
                            if !field_def.indexed || field_def.is_shadow || field_name == "id" {
                                continue;
                            }

                            if let Some(tantivy_field) = indexed_fields.get(field_name)
                                && let Some(field_value) = json_obj.get(field_name)
                            {
                                add_json_value_to_doc(
                                    &mut tantivy_doc,
                                    *tantivy_field,
                                    field_name,
                                    &field_def.field_type,
                                    field_value,
                                    BadValue::SkipAndWarn,
                                )?;
                            }
                        }
                    }

                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                    writer.add_document(tantivy_doc)?;
                }
                None => {
                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                }
            }

            replayed_count += 1;
            replayed_since_commit += 1;

            // Periodic commit during replay, on a much coarser threshold than steady-state
            // writes: each commit seals a segment and fsyncs, so replaying a large WAL at the
            // normal batch threshold would produce hundreds of tiny segments and the merge
            // storm that follows. Stamping the sequence into each one is what makes them
            // worth taking — a crash mid-recovery resumes from the last stamp instead of
            // replaying the tail from the start. Nothing is written to redb here; the WAL is
            // being iterated inside an open read transaction, and the stamp already records
            // the progress that a redb write would have.
            if replayed_since_commit >= recovery_commit_threshold {
                tracing::info!(
                    index = %index,
                    replayed = replayed_count,
                    replayed_since_commit = replayed_since_commit,
                    checkpoint_seq = seq_id,
                    "Recovery: threshold commit during WAL replay"
                );
                commit_writer_at(writer, seq_id)?;
                replayed_since_commit = 0;
            }

            // Log progress every 1000 documents
            if replayed_count % 1000 == 0 {
                tracing::info!(
                    index = %index,
                    replayed = replayed_count,
                    range_start = range_start,
                    max_wal_seq = max_wal_seq,
                    "Recovery progress"
                );
            }
        }

        // Documents replayed since the last commit are in the writer's buffer but not yet
        // durable. Seed the operations counter with them so the normal commit path (threshold
        // or supervisor idle timeout) flushes them; otherwise the no-op guard in commit_index
        // would leave them buffered until unrelated traffic arrives for this index.
        if replayed_since_commit > 0 {
            self.operations_counter
                .entry(index.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .value()
                .fetch_add(replayed_since_commit, Ordering::SeqCst);
        }

        tracing::info!(
            index = %index,
            replayed_count = replayed_count,
            distinct_documents = applied_ids.len(),
            skipped_duplicates = skipped_duplicates,
            uncommitted = replayed_since_commit,
            "WAL recovery completed - replayed missing operations"
        );

        Ok((replayed_count, max_wal_seq, last_committed_seq))
    }

    /// Build Tantivy schema and field map from index schema definition using native Tantivy types.
    pub(crate) fn create_schema_from_definition(
        index_schema: &IndexSchema,
    ) -> (Schema, SchemaFields) {
        use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

        let mut schema_builder = Schema::builder();

        // ID field is always present - untokenized string for exact matching, stored so the
        // document answers standalone, and a fast column so a hit's id is a term-ordinal
        // read rather than a stored-document decompression per hit. STORED stays because
        // indexes built before the column existed have no `id` fast field and still answer
        // it from the stored document — the reader falls back per index, not per hit.
        let id_field = schema_builder.add_text_field("id", STRING | STORED | FAST);

        // No `_seq` field. It cost 8 stored bytes plus a fast column per document, and its
        // only reader was the checkpoint scan that `order_by_u64_field` needs — which the
        // commit payload replaced. Indices that already have the column keep it and keep
        // being written to; nothing new grows one.

        let mut indexed_fields = HashMap::new();

        for (name, field_def) in &index_schema.fields {
            // Skip reserved fields (id, _seq), non-indexed fields, and shadow fields
            if name == "id" || name == "_seq" || !field_def.indexed || field_def.is_shadow {
                continue;
            }

            let field = match field_def.field_type {
                TantivyFieldType::Text => {
                    let mut options = TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(field_def.tokenizer.as_deref().unwrap_or("default"))
                            .set_index_option(match field_def.index_record_option.as_deref() {
                                Some("Basic") => IndexRecordOption::Basic,
                                Some("WithFreqs") => IndexRecordOption::WithFreqs,
                                _ => IndexRecordOption::WithFreqsAndPositions,
                            }),
                    );
                    if field_def.stored {
                        options = options.set_stored();
                    }
                    // `fast` on a text field builds the string fast column that a sort needs.
                    // `None` rather than a tokenizer name on purpose: the column then holds the
                    // whole untokenized value, which is what an alphabetical sort orders on. A
                    // tokenized fast column would sort by whichever token came first.
                    //
                    // Without this the field has no column, and `search_documents` has to pick
                    // sort candidates by relevance and reorder them afterwards — an ordering that
                    // is only approximately right and cannot be paged through. See the sort
                    // branch there.
                    if field_def.is_fast() {
                        options = options.set_fast(None);
                    }
                    schema_builder.add_text_field(name, options)
                }
                TantivyFieldType::String => {
                    if field_def.is_fast() {
                        schema_builder.add_text_field(name, STRING | FAST)
                    } else {
                        schema_builder.add_text_field(name, STRING)
                    }
                }
                TantivyFieldType::I64 => {
                    if field_def.is_fast() {
                        schema_builder.add_i64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_i64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::U64 => {
                    if field_def.is_fast() {
                        schema_builder.add_u64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_u64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::F64 => {
                    if field_def.is_fast() {
                        schema_builder.add_f64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_f64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Date => {
                    if field_def.is_fast() {
                        schema_builder.add_date_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_date_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Boolean => schema_builder.add_bool_field(name, INDEXED),
                TantivyFieldType::Bytes => schema_builder.add_bytes_field(name, INDEXED),
                TantivyFieldType::Ip => schema_builder.add_ip_addr_field(name, INDEXED),
                TantivyFieldType::Json => schema_builder.add_json_field(name, TEXT),
                TantivyFieldType::Facet => schema_builder.add_facet_field(name, INDEXED),
            };

            indexed_fields.insert(name.clone(), field);
        }

        let schema = schema_builder.build();
        let fields = SchemaFields {
            id: id_field,
            seq: None,
            indexed_fields,
        };

        (schema, fields)
    }

    /// Derive Tantivy field mapping from an existing index schema on disk.
    pub(crate) fn load_fields_from_existing_index(
        tantivy_index: &Index,
    ) -> Result<SchemaFields, StoreError> {
        let schema = tantivy_index.schema();

        let id = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;

        // Absent on any index built without it; that is not an error, it is the new normal.
        // This runs on every open of an existing index, so treating it as required here is
        // what would make an index built either way unopenable by the other.
        let seq = schema.get_field("_seq").ok();

        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" || name == "_seq" {
                continue;
            }
            indexed_fields.insert(name.to_string(), field);
        }

        Ok(SchemaFields {
            id,
            seq,
            indexed_fields,
        })
    }

    /// Derive IndexSchema from a Tantivy index's schema.
    /// This reads back the actual persisted schema from Tantivy and converts it
    /// to our IndexSchema format, ensuring we're in sync with what Tantivy has.
    /// NOTE: Excludes the mandatory 'id' field since it's implicit in Tantivy
    pub(crate) fn derive_index_schema_from_tantivy(tantivy_index: &Index) -> IndexSchema {
        use tantivy::schema::FieldType;

        let schema = tantivy_index.schema();
        let mut fields = HashMap::new();

        for (_field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" {
                continue; // Skip the mandatory id field - it's implicit in Tantivy
            }
            if name == "_seq" {
                // Present only on an index built before the field was retired. Carrying it
                // into a derived schema would put it back into a document nobody declared it
                // in, and every listing filters it out again downstream anyway.
                continue;
            }

            let field_type = match field_entry.field_type() {
                FieldType::Str(_) => {
                    // Check if it's indexed with STRING flag (untokenized) or default TEXT
                    // For simplicity, we'll check if it's stored but not indexed as a heuristic
                    let is_indexed = field_entry.is_indexed();
                    let is_stored = field_entry.is_stored();
                    if is_stored && !is_indexed {
                        TantivyFieldType::String
                    } else {
                        TantivyFieldType::Text
                    }
                }
                FieldType::U64(_) => TantivyFieldType::U64,
                FieldType::I64(_) => TantivyFieldType::I64,
                FieldType::F64(_) => TantivyFieldType::F64,
                FieldType::Bool(_) => TantivyFieldType::Boolean,
                FieldType::Date(_) => TantivyFieldType::Date,
                FieldType::Bytes(_) => TantivyFieldType::Bytes,
                FieldType::JsonObject(_) => TantivyFieldType::Json,
                FieldType::IpAddr(_) => TantivyFieldType::Ip,
                FieldType::Facet(_) => TantivyFieldType::Facet,
            };

            // Determine field options from Tantivy's field entry
            let indexed = field_entry.is_indexed();
            let stored = field_entry.is_stored();
            let fast = field_entry.is_fast();

            // Capture additional options for Text fields
            let (tokenizer, index_record_option) = if let FieldType::Str(text_options) =
                field_entry.field_type()
            {
                // Extract the actual tokenizer and index options from Tantivy
                let tokenizer_name = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let token_name = opts.tokenizer().to_string();
                        tracing::trace!(field_name = %name, tokenizer = %token_name, "Extracted tokenizer from Tantivy");
                        Some(token_name)
                    }
                    None => {
                        tracing::trace!(field_name = %name, "No indexing options found, using default tokenizer");
                        Some("default".to_string())
                    }
                };

                let index_option = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let opt_str = match opts.index_option() {
                            tantivy::schema::IndexRecordOption::Basic => "Basic".to_string(),
                            tantivy::schema::IndexRecordOption::WithFreqs => {
                                "WithFreqs".to_string()
                            }
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions => {
                                "WithFreqsAndPositions".to_string()
                            }
                        };
                        tracing::trace!(field_name = %name, index_option = %opt_str, "Extracted index option from Tantivy");
                        Some(opt_str)
                    }
                    None => {
                        tracing::trace!(field_name = %name, "No indexing options found, using default index option");
                        Some("WithFreqsAndPositions".to_string())
                    }
                };
                (tokenizer_name, index_option)
            } else {
                tracing::trace!(field_name = %name, field_type = ?field_entry.field_type(), "Non-text field, no tokenizer options");
                (None, None)
            };

            fields.insert(
                name.to_string(),
                FieldDef {
                    name: name.to_string(),
                    field_type,
                    indexed,
                    stored,
                    // Derived from the built index, so this is what the index actually has
                    // rather than what a schema asked for: concrete, never unresolved.
                    fast: Some(fast),
                    is_shadow: false, // Fields derived from Tantivy schema are not shadow fields
                    // Tantivy stores no description; one lives only in the schema record.
                    description: None,
                    tokenizer,
                    index_record_option,
                },
            );
        }

        let text_count = fields
            .values()
            .filter(|f| matches!(f.field_type, TantivyFieldType::Text))
            .count();
        let fast_count = fields.values().filter(|f| f.is_fast()).count();
        tracing::debug!(
            total_fields = fields.len(),
            text_fields = text_count,
            fast_fields = fast_count,
            "Derived index schema from Tantivy"
        );

        let now = chrono::Utc::now().timestamp();
        IndexSchema {
            fields,
            // Derived from an index on disk, so it describes a live one.
            state: SchemaState::Active,
            version: 1,
            created_at: now,
            updated_at: now,
            description: None,
            // Tantivy stores fields, not ownership, so there is nothing to recover here. Safe
            // because this value is only ever used for its `fields`: `get_schema_cached` merges
            // them onto the *stored* schema, which is where `tenant` — and `description`, and
            // the timestamps — come from. A caller that used this whole value as a schema would
            // silently unstamp the index.
            tenant: None,
            // Query-time only, and never in tantivy; recovered from the stored schema the same
            // way `tenant` is.
            default_fields: None,
            routing_field_name: "id".to_string(),
            shadow_fields: HashSet::new(),
        }
    }

    /// On-disk Tantivy directory for `index`, validated to stay inside this
    /// shard's `indices/` directory.
    ///
    /// Every path derived from an index name goes through here rather than joining
    /// directly, so that a name containing `..` or a path separator can never escape the
    /// shard. That includes names read back from redb or the filesystem, which are
    /// already trusted — one construction site is what keeps the guarantee checkable.
    pub fn index_dir(&self, index: &str) -> Result<PathBuf, StoreError> {
        resolve_index_dir(&self.config.shard_path.join("indices"), index)
    }

    /// Helper method: get_or_create_index
    /// Made public to allow pre-creating indexes when schema is created
    pub fn get_or_create_index(
        &self,
        index: &str,
    ) -> Result<(Arc<Mutex<IndexWriter>>, SchemaFields), StoreError> {
        // Any return from here marks this index as used, so the LRU ordering reflects every
        // reference and not only the ones that had to open something.
        self.touch_open_index(index);

        // Fast path: Check writers cache first
        if let Some(writer) = self.writers.get(index)
            && let Some(fields) = self.fields_cache.get(index)
        {
            return Ok((Arc::clone(writer.value()), fields.value().clone()));
        }

        // Slow path: serialize initialization per index. `get_or_create_index` is reachable
        // concurrently from the shard writer thread, the read pool (stats) and the startup
        // warmup threads. Without this guard two of them can both miss the fast path and
        // both call `writer_with_options`, where tantivy's non-blocking `.tantivy-writer.lock`
        // makes the loser fail with `LockError::LockBusy`.
        let init_lock = {
            let entry = self
                .index_init_locks
                .entry(index.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(entry.value())
        };
        let _init_guard = init_lock.lock().unwrap_or_else(|poisoned| {
            tracing::error!(index = %index, "Index init mutex was poisoned, recovering");
            poisoned.into_inner()
        });

        // Re-check under the init lock: another thread may have finished initialization
        // while we were waiting for it.
        if let Some(writer) = self.writers.get(index)
            && let Some(fields) = self.fields_cache.get(index)
        {
            return Ok((Arc::clone(writer.value()), fields.value().clone()));
        }

        // A writer is cached but its field handles are not. Persisting a schema evicts the
        // field cache without touching the writer — `store_schema_and_cache` and
        // `invalidate_schema_cache` both do — so a live index can arrive here with half of the
        // pair the fast path needs. Rebuilding the handles from the writer's own index is the
        // only correct response: the slow path below would open a *second* `IndexWriter`
        // against the lockfile this one still holds and fail with `LockBusy`, which is what
        // made every schema write against an open index a 500.
        //
        // Deriving them from the cached writer is also what keeps the two in step by
        // construction — the handles resolve against exactly the index the writer is writing
        // to, rather than against a schema read from somewhere else.
        if let Some(writer) = self.writers.get(index) {
            let writer_arc = Arc::clone(writer.value());
            // Release the map's shard guard before blocking on the writer mutex.
            drop(writer);

            let fields = {
                let guard = writer_arc.lock().unwrap_or_else(|poisoned| {
                    tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                    poisoned.into_inner()
                });
                Self::load_fields_from_existing_index(guard.index())?
            };

            self.fields_cache.insert(index.to_string(), fields.clone());
            tracing::debug!(
                index = %index,
                "Rebuilt field handles for a live index from its cached writer"
            );
            return Ok((writer_arc, fields));
        }

        // Create index directory and Tantivy index if it doesn't exist.
        // `index_dir` rejects any name that is not a single path component, so a
        // traversal attempt cannot reach the `create_dir_all` below.
        let index_path = self.index_dir(index)?;
        let init_start = Instant::now();

        // Determine schema for this index
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let (schema, _) = Self::create_schema_from_definition(&index_schema);

        // Create or open tantivy index, and get the correct field handles
        let open_start = Instant::now();
        let (tantivy_index, fields, sync_schema) = if index_path.join("meta.json").exists() {
            // Opening existing index: must use Field handles from the opened index's schema
            let opened_index = open_tantivy_index(&index_path)?;
            let fields = Self::load_fields_from_existing_index(&opened_index)?;
            (opened_index, fields, false)
        } else {
            // Creating new index: use the schema and fields we just built
            fs::create_dir_all(&index_path)?;
            let new_index = create_tantivy_index(&index_path, schema)?;

            // After creating the index, read back the actual Tantivy schema and sync it.
            // This ensures our cached schema matches exactly what Tantivy persisted.
            let fields = Self::load_fields_from_existing_index(&new_index)?;
            (new_index, fields, true)
        };

        // IMPORTANT: Only sync schema when we actually created a new index
        // This ensures we don't overwrite persisted schema when index was deleted
        if sync_schema {
            // Use the original schema as the source of truth
            // The index_schema contains the complete field definitions including indexed=false fields
            let mut tantivy_schema = (*index_schema).clone();

            // Ensure 'id' field exists with correct Tantivy-derived attributes
            // This handles cases where the original schema didn't specify 'id' field
            tantivy_schema
                .fields
                .entry("id".to_string())
                .or_insert_with(|| {
                    FieldDef {
                        name: "id".to_string(),
                        field_type: TantivyFieldType::Text,
                        indexed: true,
                        stored: true,
                        fast: Some(true),
                        is_shadow: false, // The canonical 'id' field is not a shadow field
                        description: None,
                        tokenizer: Some("raw".to_string()),
                        index_record_option: Some("Basic".to_string()),
                    }
                });

            // IMPORTANT: Cache should reflect merged schema (Tantivy + stored metadata)
            self.schema_cache
                .insert(index.to_string(), Arc::new(tantivy_schema.clone()));

            // Persist the merged schema to redb for future reference
            self.store_schema(index, &tantivy_schema)?;

            // CRITICAL: Clear reader cache to ensure search sees latest commits
            // This prevents searches from using stale readers that don't see newly written documents
            self.readers.remove(index);

            tracing::debug!(index = %index, "Schema synced: Tantivy schema merged with stored metadata, cached and persisted");
        }

        let open_elapsed = open_start.elapsed();

        // Create writer with dynamic memory budget based on index size and field count
        let writer_start = Instant::now();
        let field_count = Some(fields.indexed_fields.len());
        let optimal_budget = self
            .config
            .get_optimal_memory_budget(&index_path, field_count);

        // Cache the budget, stamped: this is the measurement the TTL in
        // `should_commit_writer` ages out.
        self.budget_cache
            .insert(index.to_string(), BudgetCacheEntry::now(optimal_budget));

        let num_worker_threads = self.config.indexer_num_threads.max(1);
        let num_merge_threads = self.config.merge_num_threads.max(1);
        let memory_per_thread = optimal_budget / num_worker_threads;

        let writer_options = tantivy::indexer::IndexWriterOptions::builder()
            .num_worker_threads(num_worker_threads)
            .memory_budget_per_thread(memory_per_thread)
            .num_merge_threads(num_merge_threads)
            .build();
        let mut writer = tantivy_index.writer_with_options(writer_options)?;

        tracing::info!(
            index = %index,
            worker_threads = num_worker_threads,
            merge_threads = num_merge_threads,
            budget_mb = optimal_budget / (1024 * 1024),
            "IndexWriter created with explicit thread configuration"
        );

        let writer_elapsed = writer_start.elapsed();

        // Bring Tantivy up to what redb has already made durable. Normally this reads two
        // numbers and stops. No reader is opened for it: the checkpoint comes from the commit
        // payload in `meta.json`, so nothing here has to touch a searcher, and on a large
        // index building one is real work that the query path would only have to redo.
        let recovery_start = Instant::now();
        let (replayed_count, max_wal_seq, last_committed_seq) =
            self.recover_index(index, &mut writer, &tantivy_index)?;

        if replayed_count > 0 {
            tracing::info!(
                index = %index,
                count = replayed_count,
                "Recovered {} operations from WAL for index {}",
                replayed_count,
                index
            );

            // CRITICAL FIX: Do NOT commit immediately after recovery.
            // The blocking commit() call can take a very long time (segment merging, fsync, etc.)
            // which blocks the writer thread and causes HTTP requests to timeout.
            // Instead, rely on the normal commit flow:
            //   1. Operations are already in Tantivy's in-memory buffer
            //   2. The next normal commit (via maybe_commit_writer) will persist them
            //   3. If the process crashes before that commit, WAL recovery will replay again
            // This is safe because:
            //   - WAL entries are still present until after the next commit
            //   - The sequence counter is set to max(max_wal_seq, last_committed_seq)
            //   - Recovery is idempotent - replaying again is safe
            tracing::info!(
                index = %index,
                "Recovery complete - {} operations in Tantivy buffer, will persist on next commit",
                replayed_count
            );

            // Replay may have made threshold commits, and a query arriving during recovery
            // can already have cached a reader for this directory — the shard answers
            // searches throughout startup. Readers reload only when told to, and the next
            // `commit_index` may be far off on an index that stops taking writes here, so
            // tell them now rather than serving the pre-recovery segment set until then.
            if let Err(e) = self.smart_refresh_reader(index) {
                tracing::warn!(
                    index = %index,
                    error = %e,
                    "Could not refresh reader after recovery; it will refresh on the next commit"
                );
            }
        }

        let recovery_elapsed = recovery_start.elapsed();
        let total_elapsed = init_start.elapsed();

        tracing::info!(
            index = %index,
            open_ms = open_elapsed.as_millis(),
            writer_ms = writer_elapsed.as_millis(),
            recovery_ms = recovery_elapsed.as_millis(),
            total_ms = total_elapsed.as_millis(),
            replayed = replayed_count,
            budget_mb = optimal_budget / (1024 * 1024),
            "Index initialization complete"
        );

        let writer_arc = Arc::new(Mutex::new(writer));

        // Admitted here rather than on the way in: immediately before the insert it accounts
        // for, so a failure anywhere above leaves nothing counted as open that is not.
        self.admit_open_index(index);

        // Store in cache
        self.writers
            .insert(index.to_string(), Arc::clone(&writer_arc));
        self.fields_cache.insert(index.to_string(), fields.clone());

        // Seed the sequence counter from what `recover_index` already read.
        //
        // Both terms matter. The WAL tail is empty on every clean restart, because the last
        // commit truncated it, so `max_wal_seq` alone would restart numbering from zero and
        // hand out sequences this index has already used — and the next crash would then find
        // a checkpoint far *above* the reissued tail and skip replaying it, dropping those
        // documents from the search index while redb still held them. The checkpoint is the
        // durable high-water mark that keeps numbering monotonic across restarts.
        self.current_seq
            .entry(index.to_string())
            .or_insert_with(|| {
                let max_seq = max_wal_seq.max(last_committed_seq);
                // Guard against u64::MAX which indicates corruption or uninitialized state.
                // u64::MAX is not a valid sequence number (would overflow on first write).
                let max_seq = if max_seq == u64::MAX {
                    tracing::warn!(
                        index = %index,
                        max_wal_seq = max_wal_seq,
                        last_committed_seq = last_committed_seq,
                        "Sequence counter detected u64::MAX (corrupted state), resetting to 0"
                    );
                    0
                } else {
                    max_seq
                };
                tracing::debug!(
                    index = %index,
                    max_wal_seq = max_wal_seq,
                    last_committed_seq = last_committed_seq,
                    initialized_seq = max_seq,
                    "Initialized sequence counter"
                );
                AtomicU64::new(max_seq)
            });

        Ok((writer_arc, fields))
    }

    /// ACID-compliant commit threshold with optimized batching to reduce transaction overhead.
    /// Larger batches mean fewer commits and less fsync overhead while maintaining Durability::Immediate.
    pub(crate) fn should_commit_writer(&self, index: &str, operations_since_commit: u64) -> bool {
        // The budget for this index, measured at most once per `BUDGET_CACHE_TTL`. This is
        // the only place the measurement is taken: `commit_index` used to re-take it after
        // every single commit, which is ~300 `stat` calls on a fifty-segment index, on the
        // writer thread, to refresh a five-bucket size class whose boundaries are hundreds of
        // megabytes apart.
        // Read the cached budget and *release the guard* before deciding anything.
        //
        // `DashMap::get` returns a `Ref` holding a read lock on the map's shard, and a match
        // scrutinee's temporary lives to the end of the match — so an arm that calls `insert`
        // asks the same shard for its write lock while this thread still holds the read lock.
        // dashmap's `RwLock` is not reentrant: the writer waits for a reader that is itself,
        // on the shard writer thread, forever. It survived review because the deadlock needs
        // the *stale* arm, which cannot be reached until `BUDGET_CACHE_TTL` has passed since
        // this index's writer was opened — so every short test hit the fresh arm and passed.
        // See ROADMAP OB14 for what it did to a node under load.
        //
        // `and_then` consumes the `Ref` and drops it when the closure returns, so the lock is
        // gone before the `match` below can take the write path.
        let cached = self
            .budget_cache
            .get(index)
            .and_then(|entry| (!entry.value().is_stale()).then(|| entry.value().budget));

        let budget = match cached {
            Some(budget) => budget,
            None => {
                // Measurement only, but the path is still built by `index_dir` so no
                // caller-supplied name is ever joined by hand.
                let b = match self.index_dir(index) {
                    Ok(index_path) => self.config.get_optimal_memory_budget(&index_path, None),
                    Err(_) => self.config.indexer_memory_budget,
                };
                self.budget_cache
                    .insert(index.to_string(), BudgetCacheEntry::now(b));
                b
            }
        };

        // ACID-safe optimization: Scale commit frequency with memory budget
        // More memory = larger batches = fewer commits = less transaction overhead
        let min_budget = self.config.indexer_memory_min_mb * 1024 * 1024;
        let max_budget = self.config.indexer_memory_max_mb * 1024 * 1024;

        // Enhanced adaptive threshold for ACID-compliant commit optimization
        let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
        let default_batch = self.config.default_batch_size as f64;

        // Optimized scaling: 1x default (min) to 20x default (max)
        // e.g., default=1000: 1000 ops (32MB) -> 20000 ops (512MB)
        // This reduces commit frequency while maintaining ACID via Durability::Immediate
        let base_ops = (default_batch * (1.0 + budget_ratio * 19.0)) as u64;

        // Additional optimization: larger thresholds for indices with high operation counts
        // This detects bulk operation patterns and adjusts accordingly
        if self.config.commit_interval_ms > 0 {
            return self.interval_commit_due(index, operations_since_commit, base_ops);
        }

        let threshold = if operations_since_commit > default_batch as u64 * 5 {
            // For very large batches, allow up to 50% more accumulation
            // This reduces fsync overhead during bulk imports
            (base_ops as f64 * 1.5) as u64
        } else {
            base_ops
        };

        operations_since_commit >= threshold
    }

    /// The commit policy when `commit_interval_ms` is set: commit once the oldest uncommitted
    /// operation has waited an interval, or once the count runs [`COMMIT_BACKSTOP_MULTIPLE`]
    /// past `base_ops`.
    ///
    /// Measured from the oldest pending operation rather than from the last commit, so an index
    /// that has been idle for an hour does not commit on its first write and on every write
    /// after it: a quiet index takes a write, waits an interval for company, and commits once.
    /// Called on the writer thread after each drain, which is when a write can have arrived;
    /// an index whose writes stop inside an interval is committed by the node's idle commit.
    fn interval_commit_due(&self, index: &str, pending_ops: u64, base_ops: u64) -> bool {
        if pending_ops == 0 {
            return false;
        }
        if pending_ops >= base_ops.saturating_mul(COMMIT_BACKSTOP_MULTIPLE) {
            return true;
        }

        // Never zero, so a mark set in the first millisecond is not read as "nothing pending".
        let now = (self.commit_clock_epoch.elapsed().as_millis() as u64).max(1);
        // `and_then` drops the shard guard before the insert below can ask for its write lock:
        // the shape OB14 deadlocked on, written the way `should_commit_writer` now is.
        let since = self.pending_since.get(index).map(|entry| {
            // First pending operation since the last commit: start its clock.
            let _ = entry.value().compare_exchange(
                NOTHING_PENDING,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            entry.value().load(Ordering::Relaxed)
        });
        let since = match since {
            Some(since) => since,
            None => {
                self.pending_since
                    .entry(index.to_string())
                    .or_insert_with(|| AtomicU64::new(now));
                return false;
            }
        };
        now.saturating_sub(since) >= self.config.commit_interval_ms
    }

    /// Number of replayed WAL entries between commits during recovery.
    ///
    /// Recovery has different economics from steady-state writes. There is no client waiting
    /// on durability, so the only reasons to commit mid-replay are bounding the writer's
    /// in-memory buffer and checkpointing progress. Each commit costs an fsync and seals a
    /// segment, so committing at the steady-state threshold turns a large WAL into hundreds
    /// of tiny segments and a long merge tail. Scaled off the configured batch size, with a
    /// floor that keeps a small `default_batch_size` from checkpointing constantly.
    pub(crate) fn recovery_commit_threshold(&self) -> u64 {
        const RECOVERY_THRESHOLD_MULTIPLIER: u64 = 10;
        const MIN_RECOVERY_COMMIT_OPS: u64 = 25_000;

        let steady_state = self.config.default_batch_size.max(1) as u64;
        steady_state
            .saturating_mul(RECOVERY_THRESHOLD_MULTIPLIER)
            .max(MIN_RECOVERY_COMMIT_OPS)
    }

    /// Get operation count for an index since last commit
    pub fn get_operations_count(&self, index: &str) -> u64 {
        self.operations_counter
            .get(index)
            .map(|counter| counter.value().load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Increment operation count and return new count
    pub(crate) fn increment_operations(&self, index: &str) -> u64 {
        self.operations_counter
            .entry(index.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .value()
            .fetch_add(1, Ordering::SeqCst)
            + 1
    }

    /// Reset operation counter after commit
    pub fn reset_operations_counter(&self, index: &str) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(0, Ordering::SeqCst);
        }
        // Nothing is waiting any more, so the next operation starts a fresh interval.
        if let Some(since) = self.pending_since.get(index) {
            since.value().store(NOTHING_PENDING, Ordering::Relaxed);
        }
    }

    /// Reset operation counter to a specific value (for intermediate commits)
    /// This allows the supervisor to continue working while resetting the counter
    pub fn reset_operations_counter_to(&self, index: &str, value: u64) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(value, Ordering::SeqCst);
        }
    }

    /// Force a commit for a specific index.
    /// Skips the commit if there are no pending operations (no-op guard).
    /// After commit, checkpoints the durable sequence and truncates the WAL entries
    /// that are now safely persisted in Tantivy.
    pub fn commit_index(&self, index: &str) -> Result<(), StoreError> {
        // No-op guard: skip commit if no operations pending since last commit.
        let ops_pending = self.get_operations_count(index);
        if ops_pending == 0 {
            tracing::debug!(index = %index, "commit_index: skipping, no pending operations");
            return Ok(());
        }

        // Cloned out, and the map's shard guard released, before blocking on the writer. The
        // guard used to live to the end of the commit, so every insert and remove on that shard
        // of `writers` — opening another index, closing one — waited out a Tantivy commit; and a
        // thread holding a map guard while it waits on a mutex is the shape OB14 deadlocked on.
        let Some(writer_arc) = self
            .writers
            .get(index)
            .map(|writer| Arc::clone(writer.value()))
        else {
            // Pending operations with no writer: the buffered documents were dropped
            // together with the writer (admin eviction, forced removal). They are NOT in
            // Tantivy, so the WAL must be kept and the recovery checkpoint must not move —
            // the next open replays them.
            tracing::error!(
                index = %index,
                ops_pending = ops_pending,
                "commit_index: writer missing with pending operations; keeping WAL for replay"
            );
            return Err(StoreError::IndexNotFound(format!(
                "no writer for index {index} with {ops_pending} pending operations"
            )));
        };

        let committed_seq = {
            let mut writer = lock_writer(&writer_arc, index);
            self.commit_locked_writer(index, &mut writer)?
        };

        // All post-commit operations happen WITHOUT holding the writer lock
        tracing::debug!(index = %index, ops_committed = ops_pending, "commit_index: committed");

        // CRITICAL: Smart refresh reader cache after commit to ensure search sees latest data
        self.smart_refresh_reader(index)?;

        // The budget is deliberately *not* re-measured here. It used to be, on the grounds
        // that a commit changes the index size — true, and it changes it by far less than the
        // size class it feeds can notice. `should_commit_writer` re-measures on its own TTL,
        // which bounds the directory walk to twice a minute per index however hard the index
        // is written, instead of once per commit on the writer thread between the Tantivy
        // commit and the checkpoint transaction.

        self.checkpoint_after_commit(index, committed_seq)
    }

    /// Commit a writer its caller has locked, stamping the sequence it covers into the commit.
    ///
    /// The sequence is read under the lock, and that is what makes it exact: a write holds its
    /// writer from before it reserves a sequence until its document is added (see
    /// `lock_live_writer`), so with the lock in hand every reserved sequence is in the writer
    /// and nothing is half-applied. Read before the lock, as it used to be, it could include a
    /// write another thread had reserved and not yet added — stamping as durable a document
    /// this commit did not contain, and letting the checkpoint truncate its WAL entry.
    fn commit_locked_writer(
        &self,
        index: &str,
        writer: &mut IndexWriter,
    ) -> Result<Option<u64>, StoreError> {
        let committed_seq = self
            .current_seq
            .get(index)
            .map(|counter| counter.load(Ordering::SeqCst));
        // Stamp the sequence into the commit itself, so the checkpoint lands atomically with
        // the segments rather than in a second write that a crash can separate them from.
        match committed_seq {
            Some(seq) => commit_writer_at(writer, seq)?,
            None => {
                writer.commit()?;
            }
        }
        Ok(committed_seq)
    }

    /// Record a commit's sequence as durable and drop the WAL entries it covers, then zero
    /// the index's pending count.
    ///
    /// AFTER the Tantivy commit succeeds, and both in one redb transaction so a crash can never
    /// leave the checkpoint ahead of the WAL. The count is reset only once the checkpoint is
    /// durable; otherwise a later failure would make the next commit see zero pending
    /// operations and skip the WAL truncation, leaving the replay tail until the next restart.
    fn checkpoint_after_commit(
        &self,
        index: &str,
        committed_seq: Option<u64>,
    ) -> Result<(), StoreError> {
        if let Some(seq) = committed_seq {
            self.checkpoint_committed(index, seq)?;
        }
        self.reset_operations_counter(index);
        Ok(())
    }

    /// Record `committed_seq` as durable in Tantivy and drop the WAL entries it covers.
    ///
    /// Both writes share a single `Durability::Immediate` transaction: one fsync instead of
    /// two, and no window where the checkpoint has advanced but the WAL entries it claims
    /// are still present (or, worse, the reverse).
    ///
    /// The caller must have completed a successful `IndexWriter::commit()` covering every
    /// sequence up to and including `committed_seq`.
    pub(crate) fn checkpoint_committed(
        &self,
        index: &str,
        committed_seq: u64,
    ) -> Result<(), StoreError> {
        if committed_seq == 0 {
            return Ok(());
        }

        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let mut write_txn = self.kv.begin_write()?;
        {
            write_txn.set_durability(Durability::Immediate)?;

            let mut deleted_count = 0usize;
            {
                let mut wal_table = write_txn.open_table(wal_table_def)?;
                // retain_in deletes in-place over the range — no Vec of keys to materialize.
                wal_table.retain_in(0..=committed_seq, |_, _| {
                    deleted_count += 1;
                    false
                })?;
            }

            let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
            meta_table.insert(index, committed_seq)?;

            if deleted_count > 0 {
                tracing::debug!(
                    index = %index,
                    deleted = deleted_count,
                    up_to_seq = committed_seq,
                    "Checkpointed committed sequence and truncated WAL"
                );
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Force commit writer for an index (for testing)
    pub fn commit_writer(&self, index: &str) -> Result<(), StoreError> {
        self.commit_index(index)
    }

    /// Perform smart commit based on operation count.
    /// Returns Ok(true) if a commit was performed, Ok(false) if threshold not yet reached.
    pub fn maybe_commit_writer(&self, index: &str) -> Result<bool, StoreError> {
        let ops_count = self.get_operations_count(index);

        if self.should_commit_writer(index, ops_count) {
            self.commit_index(index)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Apply a single write and commit if the cumulative operations threshold is met.
    /// Returns (seq_id, committed) where committed indicates if a Tantivy commit was performed.
    pub fn apply_write_and_maybe_commit(
        &self,
        index: &str,
        op: WalOp,
    ) -> Result<(u64, bool), StoreError> {
        let seq_id = self.apply_write(index, op)?;
        let committed = self.maybe_commit_writer(index)?;
        Ok((seq_id, committed))
    }

    /// Apply a batch of writes and commit if the cumulative operations threshold is met.
    /// Returns ((seq_ids, new_docs_count), committed) where committed indicates
    /// if a Tantivy commit was performed.
    pub fn apply_batch_and_maybe_commit(
        &self,
        index: &str,
        ops: Vec<WalOp>,
    ) -> Result<((Vec<u64>, usize), bool), StoreError> {
        let result = self.apply_batch(index, ops)?;
        let committed = self.maybe_commit_writer(index)?;
        Ok((result, committed))
    }

    /// Whether this name's schema records that its index was dropped.
    ///
    /// Answered from the schema cache in the ordinary case, so a write pays a redb read only
    /// for a name this shard has not touched since it opened.
    pub(crate) fn index_was_dropped(&self, index: &str) -> bool {
        if let Some(cached) = self.schema_cache.get(index) {
            return cached.state == SchemaState::Dropped;
        }
        let Ok(read_txn) = self.kv.begin_read() else {
            return false;
        };
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => match schema_table.get(index) {
                Ok(Some(bytes)) => schema_records_a_deletion(bytes.value()),
                _ => false,
            },
            Err(_) => false,
        }
    }

    /// Whether this shard knows `index` at all, without opening or creating anything.
    ///
    /// The schema table is the registry — `get_index_names` enumerates exactly it — so a row
    /// there means the index was created, whether or not a document has ever been written to
    /// it, unless that row records a deletion. The schema cache is checked first because it
    /// answers both questions at once and costs no transaction.
    pub fn index_exists(&self, index: &str) -> bool {
        if let Some(cached) = self.schema_cache.get(index) {
            return cached.state == SchemaState::Active;
        }
        if self.writers.contains_key(index) {
            return true;
        }
        let Ok(read_txn) = self.kv.begin_read() else {
            return false;
        };
        match read_txn.open_table(TABLE_SCHEMA) {
            // A row recording a deletion is not an index.
            Ok(schema_table) => match schema_table.get(index) {
                Ok(Some(bytes)) => serde_json::from_slice::<IndexSchema>(bytes.value())
                    .map(|schema| schema.state == SchemaState::Active)
                    .unwrap_or(true),
                _ => false,
            },
            // No schema table yet, so no index has ever been created here.
            Err(_) => false,
        }
    }

    /// Multi-tenant apply_write method
    pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
        self.apply_write_attempt(index, op, 1)
    }

    /// Lock `writer_arc`, and hand the guard back only if it is still the writer this shard
    /// holds for `index`.
    ///
    /// **A write holds its index's writer from before it reserves a sequence until the
    /// document is in Tantivy and counted**, and this is where that starts. The writer thread
    /// is the only one that writes, but it is not the only one that commits: admitting an index
    /// past the open-index cap closes a colder one from whatever thread is opening — a search on
    /// the read pool, an index creation — and closing commits. It used to find the writer mutex
    /// free for most of a write, which took it only around `add_document`, so a close landing
    /// between the sequence reservation and the add stamped the reserved sequences as durable
    /// in a commit that did not contain them, truncated their WAL entries, and dropped the
    /// writer the documents were then added to. They stayed in redb and never reached the
    /// search index, on the restart that should have replayed them or on any other.
    ///
    /// Held for the whole write, the mutex makes a close either finish before the write starts
    /// or wait for it to end (`close_index` only ever `try_lock`s, so it skips a busy index
    /// rather than waiting). What is left is a close landing between `get_or_create_index`
    /// handing this writer out and the lock here, which leaves it locked but detached; `None`
    /// says so, and the caller reopens before it has touched anything.
    fn lock_live_writer<'w>(
        &self,
        index: &str,
        writer_arc: &'w Arc<Mutex<IndexWriter>>,
    ) -> Option<MutexGuard<'w, IndexWriter>> {
        let guard = lock_writer(writer_arc, index);
        let live = self
            .writers
            .get(index)
            .is_some_and(|held| Arc::ptr_eq(held.value(), writer_arc));
        live.then_some(guard)
    }

    /// This index's schema lock, which every read-modify-write of its schema row holds from
    /// the read to the cache update.
    ///
    /// The row has more than one writer: the writer thread evolves it when a document brings a
    /// field it has not seen, and the admin paths edit default fields and indexing flags from
    /// the blocking pool. Each read the schema, changed a copy and wrote it back with nothing
    /// between them, so one could write over what the other had just added — a field, or the
    /// operator's `default_fields` — in redb and in the cache alike.
    ///
    /// Ordering: a writer thread takes this while holding its index writer, and takes the redb
    /// write slot while holding this. Nothing that holds this waits on an index writer, which
    /// is what keeps the three from forming a cycle. Never removed from the map, like
    /// `index_init_locks` and for the same reason.
    fn lock_schema(&self, index: &str) -> Arc<Mutex<()>> {
        self.schema_locks
            .entry(index.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    fn apply_write_attempt(
        &self,
        index: &str,
        op: WalOp,
        attempt: usize,
    ) -> Result<u64, StoreError> {
        // A delete must not bring an index into existence. `get_or_create_index` below creates
        // one when it is absent, which is what a put wants and the opposite of what removing a
        // document that cannot be there wants — an empty index and a Tantivy directory would be
        // the trace left by deleting nothing. A put is unaffected: it is the caller that
        // legitimately creates.
        if matches!(op, WalOp::Delete { .. }) && !self.index_exists(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // A put creates the index it names, but not one whose schema has been dropped. The
        // caller settles a schema before dispatching, replacing the record of the deletion, so
        // a write that still finds that record raced the drop and has no settled schema to be
        // indexed under.
        if self.index_was_dropped(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // Get or create the index, and hold its writer for the rest of the write.
        let (writer_arc, fields) = self.get_or_create_index(index)?;
        let Some(writer) = self.lock_live_writer(index, &writer_arc) else {
            if attempt >= LIVE_WRITER_ATTEMPTS {
                return Err(StoreError::WriterClosed(index.to_string()));
            }
            return self.apply_write_attempt(index, op, attempt + 1);
        };

        // Get sequence ID for this index
        let seq_id = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Create dynamic table definitions
        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        // The WAL records which document changed, not what it changed to: the `data_<index>`
        // row written in this same transaction is the document, and recovery reads it there.
        let wal_data = encode_wal_entry(match &op {
            WalOp::Put { id, .. } => id,
            WalOp::Delete { id } => id,
        });

        match op {
            WalOp::Put { id, json_blob } => {
                // Get the schema before the transaction: evolution, shadow filtering and the
                // Tantivy document all read it, and none of them needs the txn.
                let schema = if let Some(schema) = self.get_schema_cached(index)? {
                    schema
                } else {
                    tracing::debug!(index = %index, "Loading schema from metadata store");
                    self.get_schema(index)?
                        .map(Arc::new)
                        .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                };

                // Evolve only when the document carries a field the schema has not seen. Cloning
                // the whole schema on every write was the hot-path cost this removes; an indexed
                // field's type is never changed here (see `evolve_field`).
                let mut evolved_schema = None;
                // Held from re-reading the schema below until the evolved one is in the cache,
                // so an admin edit cannot land between this write's read and its write-back.
                let schema_lock;
                let mut _schema_guard = None;
                if let Some(blob) = &json_blob {
                    let has_new_field = blob.as_object().is_some_and(|obj| {
                        obj.keys().any(|name| !schema.fields.contains_key(name))
                    });
                    if has_new_field {
                        schema_lock = self.lock_schema(index);
                        _schema_guard = Some(schema_lock.lock().unwrap_or_else(|p| p.into_inner()));
                        // Evolve the schema as it stands now, not the snapshot read before the
                        // lock: an edit that committed in between is part of what gets written.
                        let current = self
                            .get_schema_cached(index)?
                            .unwrap_or(Arc::clone(&schema));
                        let mut schema_mut = (*current).clone();
                        let evolved_fields = schema_mut.evolve_from_document(blob);
                        if !evolved_fields.is_empty() {
                            tracing::debug!(
                                index = %index,
                                evolved_fields = ?evolved_fields,
                                "Evolved schema with new non-indexed fields (persists in the data transaction)"
                            );
                            evolved_schema = Some(schema_mut);
                        }
                    }
                }

                // Build the Tantivy document outside the redb transaction: it needs the schema
                // and the field handles, not the txn, and doing it here keeps the transaction —
                // and therefore the time the writer lock and the data row are held — short.
                let mut tantivy_doc = doc!(fields.id => id.as_str());
                if let Some(seq_field) = fields.seq {
                    tantivy_doc.add_u64(seq_field, seq_id);
                }
                if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                    for (field_name, field_value) in json_obj {
                        // O(1) shadow field skip via pre-computed HashSet
                        if schema.shadow_fields.contains(field_name) {
                            continue;
                        }

                        let field_def = match schema.fields.get(field_name) {
                            Some(fd) if fd.indexed => fd,
                            _ => continue,
                        };
                        let tantivy_field = match fields.indexed_fields.get(field_name) {
                            Some(tf) => tf,
                            None => continue,
                        };

                        add_json_value_to_doc(
                            &mut tantivy_doc,
                            *tantivy_field,
                            field_name,
                            &field_def.field_type,
                            field_value,
                            BadValue::Refuse,
                        )?;
                    }
                }

                // The stored body, shadow fields stripped. Moved rather than cloned — the clone
                // on the no-shadow case was the other hot-path cost this removes.
                let filtered_json_blob = if schema.has_shadow_fields() {
                    json_blob.map(|blob| filter_shadow_fields_owned(blob, &schema))
                } else {
                    json_blob
                };
                let doc_bytes = serde_json::to_vec(&StoredDoc {
                    json_blob: filtered_json_blob.as_ref(),
                })
                .map_err(|e| StoreError::Serialization(e.to_string()))?;

                // Serialised out here for the same reason `doc_bytes` is: it walks the field map
                // and touches no table, so it does not belong inside the transaction.
                let evolved_schema_bytes = evolved_schema
                    .as_ref()
                    .map(serde_json::to_vec)
                    .transpose()
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                // redb commits the WAL entry, the document row, and any schema the document
                // evolved, before Tantivy sees any of them. Two invariants come out of that.
                //
                // The search index cannot run ahead of the document store: a failed commit can
                // never leave a document buffered in the writer that redb does not have.
                //
                // And a document that introduces a field cannot become durable without the
                // schema that names it. This used to be two transactions — the data commit, then
                // `persist_schema_evolution` — with a window between them the code could only
                // acknowledge, logging `CRITICAL: Schema evolution failed after data commit` and
                // returning an error that said the data was already saved. A stream teaching an
                // index its own shape is the workload this engine exists for, so that window sat
                // on the hot path of the differentiating feature and cost it a second fsync
                // besides. One transaction removes both: redb makes the pair atomic, so there is
                // nothing left to reconcile, and a failure now writes nothing at all.
                let is_new_document = {
                    let mut write_txn = self.kv.begin_write()?;
                    let is_new = {
                        // A schema row is metadata and was always written with `Immediate`;
                        // folding it in must not quietly downgrade that. An evolving write
                        // therefore commits durably whatever `wal_sync` says — which is the one
                        // fsync the separate schema transaction was already paying, now covering
                        // the document as well rather than in addition to it.
                        let durability = if self.config.wal_sync || evolved_schema_bytes.is_some() {
                            Durability::Immediate
                        } else {
                            Durability::None
                        };
                        write_txn.set_durability(durability)?;
                        tracing::trace!(index = %index, durability = ?durability, "Data transaction durability set (user data)");

                        let mut wal_table = write_txn.open_table(wal_table_def)?;
                        wal_table.insert(seq_id, wal_data.as_slice())?;

                        let mut data_table = write_txn.open_table(data_table_def)?;
                        let is_new = data_table
                            .insert(id.as_str(), doc_bytes.as_slice())?
                            .is_none();

                        if let Some(schema_bytes) = &evolved_schema_bytes {
                            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
                            schema_table.insert(index, schema_bytes.as_slice())?;
                        }

                        is_new
                    };
                    write_txn.commit()?;
                    is_new
                };

                // The schema cache moves only once the row it describes is durable. It used to
                // be written optimistically before the transaction and again after it, so a
                // failure anywhere in between left the cache ahead of the store.
                if let Some(evolved) = evolved_schema {
                    tracing::debug!(index = %index, "Schema evolution committed with the document");
                    self.schema_cache
                        .insert(index.to_string(), Arc::new(evolved));
                }

                // Tantivy, after redb is durable.
                if !is_new_document {
                    let term = tantivy::Term::from_field_text(fields.id, &id);
                    writer.delete_term(term);
                }
                writer.add_document(tantivy_doc)?;

                // Counted before the writer is released, so a commit never sees the document
                // without the count that makes it commit it.
                self.increment_operations(index);
                drop(writer);
                Ok(seq_id)
            }
            WalOp::Delete { id } => {
                let mut write_txn = self.kv.begin_write()?;
                {
                    let durability = if self.config.wal_sync {
                        Durability::Immediate
                    } else {
                        Durability::None
                    };
                    write_txn.set_durability(durability)?;
                    tracing::trace!(index = %index, durability = ?durability, "Data transaction durability set (user data)");

                    let mut wal_table = write_txn.open_table(wal_table_def)?;
                    wal_table.insert(seq_id, wal_data.as_slice())?;

                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.remove(id.as_str())?;
                }
                write_txn.commit()?;

                // Tantivy delete, after redb committed the removal.
                let term = tantivy::Term::from_field_text(fields.id, &id);
                writer.delete_term(term);

                self.increment_operations(index);
                drop(writer);
                Ok(seq_id)
            }
        }
    }

    /// Delete all data for an index using redb's efficient delete_table() function
    /// If delete_schema is true, also removes schema metadata from TABLE_SCHEMA
    pub fn delete_index_data(&self, index: &str, delete_schema: bool) -> Result<(), StoreError> {
        // Resolve (and validate) the directory before mutating any state, so an
        // invalid name cannot drop caches or redb tables on its way to failing.
        let index_path = self.index_dir(index)?;

        // Remove from caches first — the same ten maps an eviction drops, and deliberately
        // without the commit that precedes one: this data is about to stop existing.
        self.drop_index_caches(index);

        // Held past the transaction so the cache can take it, which is what lets
        // `index_was_dropped` answer from memory.
        let mut tombstone: Option<IndexSchema> = None;

        // Delete redb tables completely using delete_table() for efficiency
        let mut write_txn = self.kv.begin_write()?;
        {
            // Index deletion always uses Immediate durability for critical metadata operations
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index, durability = "Immediate", "Index deletion durability set");

            let data_table_name = format!("data_{}", index);
            let wal_table_name = format!("wal_{}", index);
            let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
            let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

            // Delete tables using redb's delete_table function (more efficient than manual clearing)
            // Note: delete_table returns bool indicating if table existed, we ignore the result
            let _ = write_txn.delete_table(data_table_def)?;
            let _ = write_txn.delete_table(wal_table_def)?;

            // Drop the recovery checkpoint together with the data it describes. A stale
            // checkpoint outlives the deleted index and, once the name is recreated, reads
            // as "already synced" far beyond the new WAL — which would skip recovery for an
            // index that genuinely needs it.
            {
                let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
                let _ = meta_table.remove(index)?;
            }

            // Dropping the schema records the deletion rather than removing the row. The
            // fields go, so the next write samples afresh; the row stays, so a write already
            // in flight reads that the index is gone.
            if delete_schema {
                let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
                let previous = schema_table
                    .get(index)?
                    .and_then(|bytes| serde_json::from_slice::<IndexSchema>(bytes.value()).ok());
                if let Some(previous) = previous {
                    let mut new_tombstone = IndexSchema {
                        state: SchemaState::Dropped,
                        // Above the dropped schema's, so a write still carrying it cannot
                        // install it over this row.
                        version: previous.version.saturating_add(1),
                        created_at: previous.created_at,
                        updated_at: chrono::Utc::now().timestamp(),
                        ..IndexSchema::default()
                    };
                    new_tombstone.fields.clear();
                    let encoded = serde_json::to_vec(&new_tombstone)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    schema_table.insert(index, encoded.as_slice())?;
                    tracing::debug!(
                        index = %index,
                        version = new_tombstone.version,
                        "Schema dropped; recorded the deletion in TABLE_SCHEMA"
                    );
                    tombstone = Some(new_tombstone);
                }
            } else {
                tracing::debug!(index = %index, "Keeping schema metadata in TABLE_SCHEMA");
            }
        }
        write_txn.commit()?;

        if let Some(tombstone) = tombstone {
            self.schema_cache
                .insert(index.to_string(), Arc::new(tombstone));
        }

        // Remove tantivy directory
        if index_path.exists() {
            fs::remove_dir_all(&index_path)?;
        }

        Ok(())
    }

    /// Store schema for an index
    pub fn store_schema(&self, index_name: &str, schema: &IndexSchema) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema changes always use Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Persist a schema on its own, with `Immediate` durability (critical metadata).
    ///
    /// For metadata-only changes, where there is no document for the schema row to ride along
    /// with — `update_field_indexing` is the one caller. The write path does *not* use this: a
    /// schema a document evolved is written into that document's own transaction by
    /// [`HybridStore::apply_write`], so the pair is atomic and costs one fsync rather than two.
    pub(crate) fn persist_schema_evolution(
        &self,
        index_name: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema evolution always uses Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema evolution persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::debug!(index = %index_name, "Schema evolution persisted with Immediate durability");
        Ok(())
    }

    /// Get schema for an index
    /// The query-cost policy this store applies — the node's `[security.limits]` query bounds.
    pub fn query_policy(&self) -> &QueryPolicy {
        &self.config.query
    }

    pub fn get_schema(&self, index_name: &str) -> Result<Option<IndexSchema>, StoreError> {
        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => match schema_table.get(index_name)? {
                Some(value) => {
                    let mut schema: IndexSchema = serde_json::from_slice(value.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    schema.rebuild_shadow_fields_cache();
                    Ok(Some(schema))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    /// Get schema from cache, or load from Tantivy and cache it
    /// IMPORTANT: Always prefers Tantivy schema (source of truth) over stored schema
    pub fn get_schema_cached(&self, index: &str) -> Result<Option<Arc<IndexSchema>>, StoreError> {
        // Fast path: check cache first
        if let Some(schema) = self.schema_cache.get(index) {
            return Ok(Some(Arc::clone(schema.value())));
        }

        // Slow path: load from Tantivy (source of truth), not from stored schema
        // Get the index path and open the Tantivy index directly
        let index_path = self.index_dir(index)?;

        // Always load stored schema first (may contain non-indexed fields)
        let stored_schema = self.get_schema(index)?;

        if index_path.exists() {
            let tantivy_index = open_tantivy_index(&index_path)?;

            // Derive schema from Tantivy (indexed fields only, excludes 'id')
            let tantivy_schema = Self::derive_index_schema_from_tantivy(&tantivy_index);

            // If Tantivy has no indexed fields (empty or only 'id'), prefer stored schema
            if tantivy_schema.fields.is_empty()
                && let Some(stored) = stored_schema
            {
                tracing::debug!(index = %index, "Using stored schema (Tantivy has no indexed fields yet)");
                return Ok(Some(self.cache_loaded_schema(index, stored)));
            }

            // Use stored schema as base, then add indexed fields from Tantivy
            // This preserves all field definitions including non-indexed ones
            let mut merged_schema = stored_schema.unwrap_or_default();

            // Add indexed fields from Tantivy that might be missing
            for (name, field_def) in tantivy_schema.fields {
                merged_schema.fields.entry(name).or_insert(field_def);
            }

            tracing::debug!(index = %index, "Loaded and cached merged schema (Tantivy + stored metadata)");
            Ok(Some(self.cache_loaded_schema(index, merged_schema)))
        } else {
            // Fallback: try to load from stored schema (metadata only)
            if let Some(stored) = stored_schema {
                tracing::debug!(index = %index, "Using stored schema as fallback (Tantivy not available)");
                Ok(Some(self.cache_loaded_schema(index, stored)))
            } else {
                Ok(None)
            }
        }
    }

    /// Cache a schema this thread *loaded* on a miss, unless another one arrived meanwhile, and
    /// return whichever is cached.
    ///
    /// Loading reads the stored row and opens the Tantivy index, which is slow, and a write
    /// that evolves or edits the schema can commit and cache its result inside that window.
    /// Inserting over it, as this used to, put the older schema back — and the next evolving
    /// write, starting from the cache, wrote a row without the field the newer one had added.
    /// Anything that reached the cache while this was loading came from a committed write and
    /// is at least as new as what this read, so it wins.
    pub(crate) fn cache_loaded_schema(&self, index: &str, loaded: IndexSchema) -> Arc<IndexSchema> {
        self.schema_cache
            .entry(index.to_string())
            .or_insert_with(|| Arc::new(loaded))
            .value()
            .clone()
    }

    /// Invalidate cache entry when schema is updated
    pub fn invalidate_schema_cache(&self, index: &str) {
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        tracing::debug!(index = %index, "Invalidated schema and fields cache");
    }

    /// Evolve schema from a JSON document and invalidate caches if changed
    pub fn evolve_schema_from_document(
        &self,
        index: &str,
        json_blob: &JsonValue,
    ) -> Result<Vec<String>, StoreError> {
        // Get current schema
        let mut schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Make it mutable for evolution
        let evolved_fields = Arc::make_mut(&mut schema).evolve_from_document(json_blob);

        if !evolved_fields.is_empty() {
            tracing::info!(
                index = %index,
                evolved_fields = ?evolved_fields,
                "Schema evolved with new fields"
            );

            // Store the evolved schema
            self.store_schema_and_cache(index, &schema)?;

            // Invalidate caches to force rebuild with new schema
            self.invalidate_schema_cache(index);
        }

        Ok(evolved_fields)
    }

    /// Update both redb and cache atomically
    pub fn store_schema_and_cache(
        &self,
        index: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        let schema_lock = self.lock_schema(index);
        let _schema_guard = schema_lock.lock().unwrap_or_else(|p| p.into_inner());

        // Persist to redb first
        self.store_schema(index, schema)?;

        // Update cache
        let schema_arc = Arc::new(schema.clone());
        self.schema_cache.insert(index.to_string(), schema_arc);

        // Invalidate fields cache so it rebuilds on next access
        self.fields_cache.remove(index);

        Ok(())
    }

    /// Set the `indexed` flag on named fields of a stored schema.
    ///
    /// This is the engine half of `PATCH /api/{index}/_schema`, and it exists because the two
    /// things the operation has to get right are both only knowable here.
    ///
    /// The first is that the schema must be edited in place rather than round-tripped through
    /// a response shape. Reading the schema out as JSON, mutating it and writing it back
    /// erases every property that shape does not carry — `routing_field_name` among them,
    /// which silently changes which shard a document routes to.
    ///
    /// The second is that the stored schema is a *declaration*, and the Tantivy index is built
    /// from it — at creation, and again whenever the index data is rebuilt. So marking a field
    /// indexed is meaningful even when the current Tantivy index has no column for it: it is the
    /// first step of declare-then-reingest, which is how a discovered field is made searchable
    /// today (`delete_index_data` with `delete_schema = false`, then write again).
    ///
    /// Such a field is reported in `pending_reindex` rather than refused. Until the rebuild it
    /// simply does not match, and that is not silent — the query path reports the clause as
    /// discarded and the MCP layer refuses the search outright, so nothing reads a narrower
    /// answer as a complete one.
    pub fn update_field_indexing(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<SchemaFieldUpdate, StoreError> {
        // From the read inside the plan to the cache update: see `lock_schema`.
        let schema_lock = self.lock_schema(index);
        let _schema_guard = schema_lock.lock().unwrap_or_else(|p| p.into_inner());
        let (mut schema, outcome) = self.plan_field_indexing_inner(index, updates)?;

        // Applies what this shard knows and reports what it does not, rather than refusing the
        // whole request. Shards usually hold the same schema, but semi-structured input written a
        // document at a time can leave a field on only the shards that received it — so "unknown
        // here" is not by itself a bad request. Whether a name is unknown *everywhere*, and so
        // worth refusing, is a question only the caller spanning the shards can answer.
        if outcome.applied.is_empty() {
            return Ok(outcome);
        }

        for field_name in &outcome.applied {
            if let Some(field_def) = schema.fields.get_mut(field_name) {
                field_def.indexed = updates[field_name];
            }
        }
        schema.mark_modified();

        // Persist without re-creating the index. A field in `pending_reindex` deliberately does
        // not get a Tantivy column here: building one would mean recreating the index and
        // discarding every document in it, which is the caller's decision to make, not this
        // function's.
        self.persist_schema_evolution(index, &schema)?;
        self.schema_cache
            .insert(index.to_string(), Arc::new(schema));

        tracing::info!(
            index = %index,
            applied = ?outcome.applied,
            "Field indexing flags updated"
        );

        Ok(outcome)
    }

    /// Declare which fields an unqualified term searches, or clear the declaration with `None`.
    ///
    /// Query-time only, like everything about default fields: no column changes and nothing is
    /// rebuilt, so it takes effect on the next search. The list is checked against this shard's
    /// schema before it is stored, and the version advances as for any schema edit, so a peer
    /// holding the older list loses to this one.
    pub fn set_default_fields(
        &self,
        index: &str,
        default_fields: Option<Vec<String>>,
    ) -> Result<(), StoreError> {
        // From the read to the cache update: see `lock_schema`.
        let schema_lock = self.lock_schema(index);
        let _schema_guard = schema_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mut schema = self
            .get_schema_cached(index)?
            .map(|arc| (*arc).clone())
            .ok_or_else(|| StoreError::IndexNotFound(index.to_string()))?;
        if schema.default_fields == default_fields {
            return Ok(());
        }
        schema.default_fields = default_fields;
        schema
            .validate_default_fields()
            .map_err(StoreError::Serialization)?;
        schema.mark_modified();
        self.persist_schema_evolution(index, &schema)?;
        self.schema_cache
            .insert(index.to_string(), Arc::new(schema));
        Ok(())
    }

    /// What [`HybridStore::update_field_indexing`] would do, without doing it.
    ///
    /// A schema spans every shard that holds the index, so a caller that wants the edit to be
    /// all-or-nothing across them has to learn whether each one would accept it before any of
    /// them writes.
    pub fn plan_field_indexing(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<SchemaFieldUpdate, StoreError> {
        self.plan_field_indexing_inner(index, updates)
            .map(|(_, outcome)| outcome)
    }

    /// The classification both the plan and the apply path share, with the schema it read.
    pub(crate) fn plan_field_indexing_inner(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<(IndexSchema, SchemaFieldUpdate), StoreError> {
        let schema = self
            .get_schema_cached(index)?
            .map(|arc| (*arc).clone())
            .ok_or_else(|| StoreError::IndexNotFound(index.to_string()))?;

        // Whether the built index has a column for a field decides only whether the edit takes
        // effect *now* or at the next rebuild — not whether it is allowed. Opening the index
        // reads `meta.json`; it does not touch the writer lockfile, so this is safe against a
        // live writer.
        let index_path = self.index_dir(index)?;
        let tantivy_schema = if index_path.join("meta.json").exists() {
            Some(open_tantivy_index(&index_path)?.schema())
        } else {
            None
        };

        let mut outcome = SchemaFieldUpdate::default();

        for (field_name, want_indexed) in updates {
            let Some(field_def) = schema.fields.get(field_name) else {
                outcome.unknown.push(field_name.clone());
                continue;
            };

            if field_def.indexed == *want_indexed {
                outcome.unchanged.push(field_name.clone());
                continue;
            }

            let is_promotion = *want_indexed;
            let missing_from_tantivy = tantivy_schema
                .as_ref()
                .is_some_and(|schema| schema.get_field(field_name).is_err());

            // Applied either way. The flag is a declaration, and the index is built from the
            // declaration — so this takes effect at the next rebuild rather than immediately.
            outcome.applied.push(field_name.clone());
            if is_promotion && missing_from_tantivy {
                outcome.pending_reindex.push(field_name.clone());
            }
        }

        Ok((schema, outcome))
    }

    /// How many WAL entries are waiting for Tantivy to catch up on this index.
    ///
    /// This is the quantity that decides recovery cost: it is what a replay would have to
    /// read, and zero is what lets startup skip the index without opening Tantivy at all. In
    /// steady state it returns to zero at every commit, so a value that keeps climbing means
    /// commits are not keeping up with writes.
    pub fn pending_wal_entries(&self, index: &str) -> Result<u64, StoreError> {
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let read_txn = self.kv.begin_read()?;
        match read_txn.open_table(wal_table_def) {
            Ok(table) => Ok(table.len()?),
            // No table at all: the index has never been written to.
            Err(_) => Ok(0),
        }
    }

    /// Get max WAL ID for a specific index
    /// Uses B-tree last() for O(log n) access instead of O(n) full table scan
    pub(crate) fn get_max_wal_id_for_index(&self, index: &str) -> Result<u64, StoreError> {
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(wal_table_def) {
            Ok(wal_table) => {
                // redb B-tree stores keys in sorted order; last() is O(log n)
                if let Some(result) = wal_table.last()? {
                    let max_id = result.0.value();
                    tracing::debug!(
                        index = %index,
                        max_wal_id = max_id,
                        "Retrieved max WAL ID from redb (B-tree last)"
                    );
                    Ok(max_id)
                } else {
                    tracing::debug!(index = %index, "WAL table is empty, returning 0");
                    Ok(0)
                }
            }
            Err(_) => {
                tracing::debug!(index = %index, "WAL table does not exist, returning 0");
                Ok(0) // Table doesn't exist yet
            }
        }
    }

    /// Record `seq` in the recovery metadata table on its own.
    ///
    /// Only the backfill in [`Self::checkpoint_seq`] uses this. The steady-state path writes
    /// the same table through [`Self::checkpoint_committed`], together with the WAL truncation
    /// the checkpoint authorises, so that the two can never be separated by a crash.
    pub(crate) fn persist_committed_seq(&self, index: &str, seq: u64) -> Result<(), StoreError> {
        let mut write_txn = self.kv.begin_write()?;
        {
            write_txn.set_durability(Durability::Immediate)?;
            let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
            meta_table.insert(index, seq)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Read the persisted last committed sequence for an index from the recovery metadata table.
    ///
    /// The checkpoint now travels inside Tantivy's commit payload, so this is the fallback
    /// [`Self::checkpoint_seq`] consults for an index whose last commit predates the stamp.
    /// Returns `None` when no row exists.
    pub(crate) fn get_persisted_committed_seq(
        &self,
        index: &str,
    ) -> Result<Option<u64>, StoreError> {
        let read_txn = self.kv.begin_read()?;
        match read_txn.open_table(TABLE_RECOVERY_META) {
            Ok(meta_table) => match meta_table.get(index)? {
                Some(guard) => Ok(Some(guard.value())),
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    /// Warm one index so the first query does not pay cold-start costs.
    ///
    /// Opens and caches the `IndexReader` (which is what the search path uses — the *writer*
    /// cache is irrelevant to queries), populates the schema and field caches, and builds each
    /// segment's term dictionaries. See [`warm_segment`] for why that is the whole list.
    ///
    /// Best-effort by design. Warming is driven explicitly — at startup and after commits —
    /// rather than by a registered tantivy `Warmer`, which would run on the writer thread
    /// inside `reload()` and cost a background thread per open index. The gap that buys:
    /// segments published by a background merge are not warmed until the next commit on that
    /// index. That is a cheap gap, because a merge writes its output segment through this
    /// process, so the merged data is already in page cache — the part warming cannot
    /// reconstruct for free — and merges are themselves triggered by commits, so an active
    /// index warms its merged segments on the following commit.
    ///
    /// Safe to call concurrently and repeatedly: an already-warm searcher generation is a
    /// no-op. Returns `None` when the index has no Tantivy directory yet (nothing to warm).
    pub fn warm_index(&self, index: &str) -> Result<Option<IndexWarmupStats>, StoreError> {
        let start = Instant::now();
        self.warmup_states
            .insert(index.to_string(), IndexWarmupState::Warming);

        // Loading the schema here keeps the first query off the redb metadata path.
        if let Err(e) = self.get_schema_cached(index) {
            self.warmup_states
                .insert(index.to_string(), IndexWarmupState::Failed);
            return Err(e);
        }

        let reader = match self.get_reader(index) {
            Ok(Some((reader, _fields))) => reader,
            Ok(None) => {
                // No Tantivy directory: an index that has a schema but has never been
                // written to. Nothing to warm, and nothing wrong.
                self.warmup_states
                    .insert(index.to_string(), IndexWarmupState::Warm);
                return Ok(None);
            }
            Err(e) => {
                self.warmup_states
                    .insert(index.to_string(), IndexWarmupState::Failed);
                return Err(e);
            }
        };

        let searcher = reader.searcher();
        let generation = searcher.generation().generation_id();
        let segments = searcher.segment_readers().len();

        // Skip if this exact generation was already warmed. `reader.searcher()` hands out the
        // same searcher until a reload replaces it, so an unchanged generation means the same
        // SegmentReaders with the same filled caches.
        let already_warm = self
            .warmed_generations
            .get(index)
            .is_some_and(|warmed| *warmed.value() == generation);

        let segments_warmed = if already_warm {
            0
        } else {
            for segment_reader in searcher.segment_readers() {
                warm_segment(index, segment_reader);
            }
            self.warmed_generations
                .insert(index.to_string(), generation);
            segments
        };

        let stats = IndexWarmupStats {
            index: index.to_string(),
            segments,
            segments_warmed,
            generation,
            num_docs: searcher.num_docs(),
            elapsed_ms: start.elapsed().as_millis(),
        };

        self.warmup_states
            .insert(index.to_string(), IndexWarmupState::Warm);

        if segments_warmed > 0 {
            tracing::debug!(
                index = %index,
                segments = stats.segments,
                generation = generation,
                num_docs = stats.num_docs,
                elapsed_ms = stats.elapsed_ms,
                "Index warmed"
            );
        }

        Ok(Some(stats))
    }

    /// Current warmup state for every index this store knows about.
    pub fn warmup_states(&self) -> HashMap<String, IndexWarmupState> {
        self.warmup_states
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Whether an index has completed warmup and will answer from warm buffers.
    pub fn is_index_warm(&self, index: &str) -> bool {
        self.warmup_states
            .get(index)
            .is_some_and(|state| *state.value() == IndexWarmupState::Warm)
    }

    /// Apply multiple write operations atomically to a specific index
    ///
    /// This function provides guaranteed batch write with supervised smart commits:
    /// 1. Single atomic redb transaction for all data operations
    /// 2. Single atomic tantivy writer commit for all index operations  
    /// 3. Predictable smart commit logic based on operation thresholds
    /// 4. Guaranteed document searchability after successful commit
    ///
    /// Returns (sequence_ids, new_documents_count)
    pub fn apply_batch(
        &self,
        index: &str,
        ops: Vec<WalOp>,
    ) -> Result<(Vec<u64>, usize), StoreError> {
        self.apply_batch_attempt(index, ops, 1)
    }

    fn apply_batch_attempt(
        &self,
        index: &str,
        ops: Vec<WalOp>,
        attempt: usize,
    ) -> Result<(Vec<u64>, usize), StoreError> {
        let ops_len = ops.len();
        if ops.is_empty() {
            return Ok((Vec::new(), 0));
        }

        tracing::debug!(
            index = %index,
            ops_count = ops.len(),
            "HybridStore: Starting apply_batch"
        );

        // See the guard in `apply_write`. A batch carrying even one put may create the index,
        // because that put is a caller asking for it; a batch of nothing but deletes may not.
        if ops.iter().all(|op| matches!(op, WalOp::Delete { .. })) && !self.index_exists(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // See the second guard in `apply_write`.
        if self.index_was_dropped(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // Get or create the index, and hold its writer for the rest of the batch — see
        // `lock_live_writer` for what a close landing mid-batch used to do.
        let (writer_arc, fields) = self.get_or_create_index(index)?;
        let Some(writer) = self.lock_live_writer(index, &writer_arc) else {
            if attempt >= LIVE_WRITER_ATTEMPTS {
                return Err(StoreError::WriterClosed(index.to_string()));
            }
            return self.apply_batch_attempt(index, ops, attempt + 1);
        };

        // Get schema for shadow field filtering
        let schema = if let Some(schema) = self.get_schema_cached(index)? {
            schema
        } else {
            self.get_schema(index)?
                .map(Arc::new)
                .unwrap_or_else(|| Arc::new(IndexSchema::default()))
        };

        // Reserve a contiguous block of sequence IDs atomically.
        // fetch_add returns the previous value; +1 gives the first usable seq.
        let start_seq = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(ops_len as u64, Ordering::SeqCst) + 1
        };
        let seq_ids_iter = (0..ops_len).map(move |i| start_seq + i as u64);

        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        enum PreparedKind {
            /// The Tantivy document this put will add, built before the transaction opens.
            Put {
                tantivy_doc: tantivy::TantivyDocument,
            },
            Delete,
        }

        struct PreparedOp {
            wal_bytes: Vec<u8>,
            doc_bytes: Option<Vec<u8>>,
            id: String,
            seq_id: u64,
            kind: PreparedKind,
        }

        let has_shadow_fields = schema.has_shadow_fields();

        // Step 1: everything that is CPU and not redb, done before the write transaction opens.
        //
        // Shadow filtering, the stored-body serialisation *and* the Tantivy document build all
        // read the batch and the schema and touch no table, so none of them needs to be inside
        // the transaction — and while they were, every document's field traversal held the redb
        // write lock against every other writer of this store. The transaction below is now the
        // two `insert`s and the bookkeeping that depends on what they displaced, which is the
        // only part that genuinely needs to be there.
        //
        // A refused value still aborts the whole batch and writes nothing; it now does so
        // before any table is opened rather than by dropping a transaction that had already
        // staged three hundred rows.
        let mut prepared_ops = Vec::with_capacity(ops_len);
        for (op, seq_id) in ops.into_iter().zip(seq_ids_iter) {
            match op {
                WalOp::Put { id, json_blob } => {
                    let filtered_json_blob = if has_shadow_fields {
                        json_blob.map(|blob| filter_shadow_fields_owned(blob, &schema))
                    } else {
                        json_blob
                    };

                    // Id only: the document goes to `data_<index>` in the same transaction,
                    // which is where recovery reads it from.
                    let wal_bytes = encode_wal_entry(&id);

                    let doc_bytes = serde_json::to_vec(&StoredDoc {
                        json_blob: filtered_json_blob.as_ref(),
                    })
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    // Build the Tantivy document with ONLY indexed fields: a single-pass JSON
                    // traversal that skips shadows and extracts the fields the schema indexes.
                    let mut tantivy_doc = doc!(fields.id => id.as_str());
                    if let Some(seq_field) = fields.seq {
                        tantivy_doc.add_u64(seq_field, seq_id);
                    }
                    if let Some(json_obj) = filtered_json_blob.as_ref().and_then(|v| v.as_object())
                    {
                        for (field_name, field_value) in json_obj {
                            // O(1) shadow field skip via pre-computed HashSet
                            if has_shadow_fields && schema.shadow_fields.contains(field_name) {
                                continue;
                            }

                            // Look up schema field def + Tantivy field in one go
                            let field_def = match schema.fields.get(field_name) {
                                Some(fd) if fd.indexed => fd,
                                _ => continue,
                            };
                            let tantivy_field = match fields.indexed_fields.get(field_name) {
                                Some(tf) => tf,
                                None => continue,
                            };

                            add_json_value_to_doc(
                                &mut tantivy_doc,
                                *tantivy_field,
                                field_name,
                                &field_def.field_type,
                                field_value,
                                BadValue::Refuse,
                            )?;
                        }
                    }

                    prepared_ops.push(PreparedOp {
                        wal_bytes,
                        doc_bytes: Some(doc_bytes),
                        id,
                        seq_id,
                        kind: PreparedKind::Put { tantivy_doc },
                    });
                }
                WalOp::Delete { id } => {
                    let wal_bytes = encode_wal_entry(&id);

                    prepared_ops.push(PreparedOp {
                        wal_bytes,
                        doc_bytes: None,
                        id,
                        seq_id,
                        kind: PreparedKind::Delete,
                    });
                }
            }
        }

        // Single transaction for all operations
        let mut write_txn = self.kv.begin_write()?;
        let batch_size = ops_len as u64;

        /// The last operation for an id in this batch. A batch may name the same id several
        /// times, and Tantivy has to end with the document the last one left behind, not one per
        /// put. redb already keeps the last row (last write wins); this list makes the index
        /// agree, keeping first-occurrence order so the add order — and therefore the doc ids
        /// Tantivy assigns, which break score ties — stays deterministic.
        enum FinalOp {
            Add(tantivy::TantivyDocument),
            Delete,
        }

        /// One distinct id, and everything the Tantivy pass needs to know about it.
        struct FinalEntry {
            id: String,
            /// Whether redb held a row for this id *before this batch began*.
            ///
            /// Decided on the id's first appearance and never revised, which is the whole point:
            /// a delete earlier in the same batch removes the row, so a later put's `insert`
            /// reports nothing displaced for a document the index has had all along. Reading it
            /// per-operation left that document in Tantivy with no `delete_term` to remove it,
            /// and the re-put added a second one beside it.
            existed_before: bool,
            op: FinalOp,
        }

        /// Record `op` as this id's latest, keeping the `existed_before` its first appearance
        /// decided. `touched_existing_row` is what redb just reported, and is consulted only
        /// when the id is new to this batch.
        fn record_final(
            final_ops: &mut Vec<FinalEntry>,
            final_index: &mut HashMap<String, usize>,
            id: String,
            touched_existing_row: bool,
            op: FinalOp,
        ) {
            match final_index.get(&id).copied() {
                Some(idx) => final_ops[idx].op = op,
                None => {
                    final_index.insert(id.clone(), final_ops.len());
                    final_ops.push(FinalEntry {
                        id,
                        existed_before: touched_existing_row,
                        op,
                    });
                }
            }
        }

        let mut final_ops: Vec<FinalEntry> = Vec::with_capacity(ops_len);
        // Where in `final_ops` an id's entry lives, so a repeat put or delete overwrites in place
        // rather than appending a second entry.
        let mut final_index: HashMap<String, usize> = HashMap::with_capacity(ops_len);

        // Collect sequence IDs during processing
        let mut seq_ids = Vec::with_capacity(ops_len);

        {
            // Set durability based on config for bulk operations
            let durability = if self.config.wal_sync {
                Durability::Immediate
            } else {
                Durability::None
            };
            write_txn.set_durability(durability)?;
            tracing::trace!(index = %index, batch_size = batch_size, durability = ?durability, "Bulk data transaction durability set (user data)");

            let mut wal_table = write_txn.open_table(wal_table_def)?;
            let mut data_table = write_txn.open_table(data_table_def)?;

            // Step 2: the transaction itself — the two table writes, and the bookkeeping that
            // depends on what they displaced. Every document was built above.
            for prepared in prepared_ops {
                let PreparedOp {
                    wal_bytes,
                    doc_bytes,
                    id,
                    seq_id,
                    kind,
                } = prepared;

                // Write to WAL
                wal_table.insert(seq_id, wal_bytes.as_slice())?;

                // Collect sequence ID for final result
                seq_ids.push(seq_id);

                match kind {
                    PreparedKind::Put { tantivy_doc } => {
                        // What `insert` displaced, which says whether the id already had a row —
                        // but only on this id's first appearance in the batch. `record_final`
                        // below is what enforces that.
                        let displaced_a_row = match &doc_bytes {
                            Some(bytes) => {
                                data_table.insert(id.as_str(), bytes.as_slice())?.is_some()
                            }
                            None => false,
                        };

                        record_final(
                            &mut final_ops,
                            &mut final_index,
                            id,
                            displaced_a_row,
                            FinalOp::Add(tantivy_doc),
                        );
                    }
                    PreparedKind::Delete => {
                        // `remove` answers the same question `insert` does, for the arm that
                        // would otherwise hide it from a later put of the same id.
                        let removed_a_row = data_table.remove(id.as_str())?.is_some();
                        record_final(
                            &mut final_ops,
                            &mut final_index,
                            id,
                            removed_a_row,
                            FinalOp::Delete,
                        );
                    }
                }
            }
        }

        write_txn.commit()?;

        // Apply the final Tantivy operation per id, in the order redb committed: the prior
        // version removed where there was one, then the batch's last put or delete for that id.
        //
        // Interleaved per id rather than done as a delete pass and an add pass. Tantivy resolves
        // a `delete_term` against the documents added before it, so `delete_term` immediately
        // followed by `add_document` under the same id is correct and reads as the one
        // replacement it is. The two-pass shape said the same thing less clearly, and its delete
        // list was built from what each `insert` displaced — which a delete of the same id
        // earlier in the batch had already emptied.
        let mut new_documents_count = 0usize;
        let mut replaced_documents = 0usize;
        {
            for FinalEntry {
                id,
                existed_before,
                op,
            } in final_ops
            {
                // A version to remove: one this batch is replacing, or one it is deleting. A
                // delete always issues the term even where redb held no row, which is what keeps
                // the batch a repair for an index holding a document the store does not.
                if existed_before || matches!(op, FinalOp::Delete) {
                    let term = tantivy::Term::from_field_text(fields.id, &id);
                    writer.delete_term(term);
                }

                if let FinalOp::Add(tantivy_doc) = op {
                    writer.add_document(tantivy_doc)?;
                    if existed_before {
                        replaced_documents += 1;
                    } else {
                        new_documents_count += 1;
                    }
                }
            }

            // Increment operations counter by batch size for threshold tracking.
            // The actual commit decision is made by apply_batch_and_maybe_commit()
            // which calls maybe_commit_writer() after this function returns.
            self.operations_counter
                .entry(index.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .value()
                .fetch_add(batch_size, Ordering::SeqCst);

            tracing::debug!(
                index = %index,
                batch_size = batch_size,
                new_docs = new_documents_count,
                updated_docs = replaced_documents,
                "Bulk write completed"
            );

            // Released only once the documents are in and counted, which is what makes a close
            // on another thread either precede this batch or include all of it.
            drop(writer);
        }

        // Invalidate size cache for this index to ensure fresh stats on next query.
        //
        // Unconditional, where this used to ask for a new or updated document first: a batch of
        // pure deletes satisfies neither test and still changes every figure in there. An empty
        // batch cannot reach this point — `ops.is_empty()` returned at the top.
        self.invalidate_size_cache(index);

        tracing::debug!(
            index = %index,
            seq_count = seq_ids.len(),
            new_docs = new_documents_count,
            "HybridStore: apply_batch completed successfully"
        );

        Ok((seq_ids, new_documents_count))
    }

    /// Phase 1 of startup: replay the WAL tail of every index that has one.
    ///
    /// redb and Tantivy have each finished their own recovery before this runs — redb by
    /// pointing back at its last commit root, Tantivy by opening the segments its last commit
    /// published. Neither costs time proportional to the data it holds. All that is left is
    /// the gap between them, and this closes it.
    ///
    /// Finding the indices in that gap costs one redb read transaction for the whole shard.
    /// A commit deletes the WAL entries it covers, so a non-empty `wal_<index>` is exactly
    /// the set of writes Tantivy may be missing, and an empty one means the index is in sync:
    /// no Tantivy open, no metadata lookup, no searcher. That is what keeps boot bounded by
    /// how much was in flight when the process stopped rather than by how much data the node
    /// holds — an idle 30 TB index is one B-tree descent, the same as an empty one.
    ///
    /// Returns the plan for phase 2. Recovery and warmup are deliberately split: recovery
    /// is a correctness requirement and blocks, warmup is a latency optimization and does
    /// not. See [`HybridStore::warm_index`] for phase 2.
    pub fn recover_indices(&self) -> Result<WarmupPlan, StoreError> {
        let start = Instant::now();
        let index_names = self.get_index_names()?;

        if index_names.is_empty() {
            tracing::debug!("No indices to recover");
            return Ok(WarmupPlan::default());
        }

        // One read transaction for the whole partition. Per-index transactions were a long
        // tail of small redb operations at high index counts, and there is nothing to gain
        // from them: this is a point-in-time question, and a single snapshot answers it for
        // every index at once.
        let mut needs_recovery = Vec::new();
        {
            let read_txn = self.kv.begin_read()?;
            for index_name in &index_names {
                let wal_table_name = format!("wal_{}", index_name);
                let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);
                let has_tail = match read_txn.open_table(wal_table_def) {
                    // `last()` descends to the rightmost leaf; it does not scan the table.
                    Ok(table) => table.last()?.is_some(),
                    // No table at all: the index has never been written to.
                    Err(_) => false,
                };

                if has_tail {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Recovering);
                    needs_recovery.push(index_name.clone());
                } else {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Cold);
                }
            }
        }

        tracing::info!(
            total = index_names.len(),
            needs_recovery = needs_recovery.len(),
            partition_ms = start.elapsed().as_millis(),
            "Phase 1: replaying the WAL tail of indices redb committed past Tantivy"
        );

        let mut recovered = Vec::new();
        let mut failed = Vec::new();

        if !needs_recovery.is_empty() {
            let results = std::sync::Mutex::new(Vec::new());

            // Every index that needs replay gets a thread, and `RECOVERY_GATE` decides how
            // many of them hold an `IndexWriter` at once. The threads are the cheap part; the
            // arenas are what has to be rationed, and rationing them here rather than by
            // chunking means a slow replay does not hold back the rest of its chunk.
            std::thread::scope(|scope| {
                let handles: Vec<_> = needs_recovery
                    .iter()
                    .map(|index_name| {
                        let results = &results;
                        scope.spawn(move || {
                            let _permit = RECOVERY_GATE.acquire();
                            // get_or_create_index runs recover_index as a side effect.
                            let outcome = self.get_or_create_index(index_name);
                            results
                                .lock()
                                .unwrap()
                                .push((index_name.clone(), outcome.is_ok()));
                            if let Err(e) = outcome {
                                tracing::warn!(
                                    index = %index_name,
                                    error = %e,
                                    "Recovery failed, index will retry on first access"
                                );
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    let _ = handle.join();
                }
            });

            for (index_name, ok) in results.into_inner().unwrap() {
                if ok {
                    // Recovered, but the reader is still cold — phase 2 warms it.
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Cold);
                    recovered.push(index_name);
                } else {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Failed);
                    failed.push(index_name);
                }
            }
        }

        // Phase 2 covers every index, recovered or not: recovery populates the *writer*
        // cache, which queries never touch. Order smallest-first so the greatest number of
        // indices become warm soonest — a large index left for last still answers queries,
        // it just pays its own cold cost once.
        let mut pending_warmup: Vec<String> = index_names
            .iter()
            .filter(|name| !failed.contains(name))
            .cloned()
            .collect();
        pending_warmup.sort_by_key(|name| {
            self.index_dir(name)
                .ok()
                .and_then(|path| index_size_bytes(&path))
                .unwrap_or(0)
        });

        let plan = WarmupPlan {
            recovered,
            failed,
            pending_warmup,
        };

        tracing::info!(
            total = index_names.len(),
            recovered = plan.recovered.len(),
            failed = plan.failed.len(),
            pending_warmup = plan.pending_warmup.len(),
            elapsed_ms = start.elapsed().as_millis(),
            "Phase 1 complete: all indices are queryable"
        );

        Ok(plan)
    }

    /// Phase 2 of startup: warm the readers for `indices`, in the order given.
    ///
    /// Runs on the calling thread — callers put it on a background thread so it never delays
    /// serving. Individual failures are logged and skipped: a failed warmup costs latency on
    /// the first query, not correctness.
    ///
    /// Returns the number of indices warmed.
    pub fn warm_indices(&self, indices: &[String]) -> usize {
        if indices.is_empty() {
            return 0;
        }

        let start = Instant::now();
        let mut warmed = 0usize;
        let mut total_segments = 0usize;
        let mut total_docs = 0u64;
        let mut skipped = 0usize;

        for (position, index) in indices.iter().enumerate() {
            // Warming faults term dictionaries in from mmap, so on a multi-terabyte shard it
            // is sustained random IO — and every shard on the node is doing it at the same
            // time. Past this budget the storm costs the queries that are already arriving
            // more than it saves the ones that have not. Whatever is left warms on first
            // access through the same path, which is where an index nobody queries should
            // have been paying anyway.
            if start.elapsed() >= WARMUP_BUDGET {
                skipped = indices.len() - position;
                break;
            }

            match self.warm_index(index) {
                Ok(Some(stats)) => {
                    warmed += 1;
                    total_segments += stats.segments;
                    total_docs += stats.num_docs;
                }
                Ok(None) => {
                    // Schema exists but nothing was ever written; nothing to warm.
                    warmed += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        index = %index,
                        error = %e,
                        "Warmup failed; first query for this index will pay the cold cost"
                    );
                }
            }
        }

        if skipped > 0 {
            tracing::warn!(
                requested = indices.len(),
                warmed = warmed,
                skipped = skipped,
                budget_secs = WARMUP_BUDGET.as_secs(),
                "Phase 2 hit its time budget; the remaining indices will warm on first query"
            );
        }

        tracing::info!(
            requested = indices.len(),
            warmed = warmed,
            skipped = skipped,
            segments = total_segments,
            documents = total_docs,
            elapsed_ms = start.elapsed().as_millis(),
            "Phase 2 complete: index readers warmed"
        );

        warmed
    }
}
