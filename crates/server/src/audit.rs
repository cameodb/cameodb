//! The audit trail: who touched what, and whether they were allowed to.
//!
//! Phase 14 C2. Authorization already decides every request at one chokepoint
//! ([`crate::authz::decide`]) and already knows the `key_id`, the label, the role and the
//! index. What it did not do is *keep* any of it: refusals wrote a `warn!` and successes
//! wrote a `debug!`, so a node could tell you it had turned somebody away but never who had
//! legitimately read what. This module is the sink that closes that, and deliberately
//! nothing else — the decision of *which* events deserve a record lives with the code that
//! classifies routes, not here.
//!
//! Three properties shape the whole design:
//!
//! **Detail where it is affordable, statistics where it is not.** CameoDB's usual workload
//! is a knowledge base: ingestion outnumbers retrieval by orders of magnitude, and at the
//! measured ~6 900 writes/s a record per write is a firehose that buries the handful of
//! reads worth looking at. So writes are folded into periodic per-key counts and reads keep
//! a record each. That is the opposite of what a general-purpose access log would do, and it
//! follows from the shape of the traffic rather than from taste.
//!
//! **Never on the request's critical path.** Emitting is a stamp and a non-blocking
//! `try_send`. Everything after that — serialization, file writes, rotation — happens on a
//! dedicated OS thread. A slow disk must not become a slow node; audit is evidence, not a
//! dependency of serving traffic.
//!
//! **Loss is admitted, never silent.** A full queue drops the record and counts it, and the
//! writer emits a `gap` record naming the number lost. An audit log that quietly skips
//! entries under load is worse than one that says it skipped them, because only the first
//! one lies about what it contains.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// `[security.audit]` — what the node keeps about who called it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuditConfig {
    /// Off by default. An audit trail nobody asked for is a disk-usage surprise, and the
    /// posture matrix is where "you probably want this" belongs — not this field.
    pub enabled: bool,

    /// Optional JSON Lines file. Without it the trail is the in-memory ring only, which is
    /// queryable but dies with the process; with it the ring is a live view of a file that
    /// outlives restarts.
    pub file: Option<PathBuf>,

    /// How many records `/_admin/audit` can show. Bounded because it is memory.
    pub buffer_capacity: usize,

    /// Depth of the hand-off queue to the writer thread. Sized for a burst, not a backlog:
    /// if the writer cannot keep up over a sustained period, dropping and saying so is the
    /// intended behaviour.
    pub queue_capacity: usize,

    /// Rotate once the active file passes this size.
    pub max_file_bytes: u64,

    /// How many rotated files to keep, `audit.jsonl.1` … `.N`, oldest deleted.
    pub max_files: usize,

    /// Whether to record search query text.
    ///
    /// Off by default because a query is itself data: searching for a person's name records
    /// that name, so an audit log written to answer "who read the customer index" would
    /// quietly accumulate the customer names that were looked up. Turn it on where the
    /// forensic detail is worth that, and treat the audit file as sensitive when you do.
    pub record_query_text: bool,

    /// How often rolled-up counts are flushed, in seconds. Also the interval at which the
    /// file is flushed and any dropped-record gap is reported.
    pub rollup_secs: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            file: None,
            buffer_capacity: 2048,
            queue_capacity: 8192,
            max_file_bytes: 100 * 1024 * 1024,
            max_files: 5,
            record_query_text: false,
            rollup_secs: 10,
        }
    }
}

impl AuditConfig {
    /// Reject settings that would make the trail useless rather than merely unusual.
    ///
    /// A zero here is not a tuning choice, it is a silent disabling: a ring of zero holds
    /// nothing, a queue of zero drops everything, a rollup interval of zero spins. Caught at
    /// load time so the failure is a refusal to start rather than an empty audit log
    /// discovered during an incident.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.buffer_capacity == 0 {
            return Err("[security.audit] buffer_capacity must be at least 1".to_string());
        }
        if self.queue_capacity == 0 {
            return Err("[security.audit] queue_capacity must be at least 1".to_string());
        }
        if self.rollup_secs == 0 {
            return Err("[security.audit] rollup_secs must be at least 1".to_string());
        }
        if self.max_files == 0 {
            return Err(
                "[security.audit] max_files must be at least 1 when a file is configured"
                    .to_string(),
            );
        }
        if self.file.is_some() && self.max_file_bytes == 0 {
            return Err("[security.audit] max_file_bytes must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// Whether the action happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Allowed,
    Denied,
}

/// One line of the trail.
///
/// Flat rather than an enum of shapes, because the consumer is `grep`, `jq` or a log
/// collector: every record answers the same questions in the same field names, and the ones
/// that do not apply are absent rather than null. `event` says which kind it is.
///
/// There is no field for the key itself and no constructor that could put one here. The
/// `key_id` is a digest prefix minted for exactly this purpose — it ties a line to a
/// credential an operator issued without the credential ever being written down.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// RFC 3339, UTC, millisecond precision. Stamped when the record is handed over, not
    /// when it is written, so queue delay cannot backdate an event.
    pub ts: String,

    /// `http`, `mcp_tool`, `write_stats`, `public_stats`, `auth_denied_stats` or `gap`.
    ///
    /// A `Cow` so the constructors cost no allocation for the fixed set of kinds, while the
    /// record still round-trips through JSON — the trail is read back by tests and by
    /// anything else that consumes the file.
    pub event: Cow<'static, str>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,

    /// Why it was refused. Absent when it was not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// The socket's remote address. Not `X-Forwarded-For`: that header is written by the
    /// client, so trusting it would let a caller choose what the audit log says about them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,

    /// Path without its query string — a query string is caller-supplied data and belongs
    /// under `query`, where `record_query_text` governs whether it is kept at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,

    /// Rolled-up records only: how many operations this line stands for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<u64>,

    /// Rolled-up records only: how many of them failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<u64>,

    /// Rolled-up records only: when the first operation in this window happened.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start: Option<String>,

    /// `gap` records only: how many records were lost to a full queue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped: Option<u64>,
}

impl AuditRecord {
    /// An HTTP request, identified by what the router saw.
    pub fn http(method: &str, path: &str) -> Self {
        Self {
            event: Cow::Borrowed("http"),
            method: Some(method.to_string()),
            // Split rather than trusting the caller: a handler could hand back a raw URI.
            path: Some(path.split('?').next().unwrap_or(path).to_string()),
            ..Self::default()
        }
    }

    /// One MCP tool call, which the HTTP layer cannot see inside.
    pub fn mcp_tool(tool: &str) -> Self {
        Self {
            event: Cow::Borrowed("mcp_tool"),
            tool: Some(tool.to_string()),
            ..Self::default()
        }
    }

    pub fn with_identity(
        mut self,
        key_id: Option<String>,
        label: Option<String>,
        role: Option<String>,
    ) -> Self {
        self.key_id = key_id;
        self.label = label;
        self.role = role;
        self
    }

    /// Rename this record's kind.
    ///
    /// Used when handing a record to [`AuditSink::record_rolled`]: a total is a different
    /// kind of statement from an event, and calling both `http` would let a consumer read
    /// one line as one request. The emitting side names it, not the writer — only the
    /// emitter knows *why* this class is counted rather than listed.
    pub fn with_event(mut self, event: &'static str) -> Self {
        self.event = Cow::Borrowed(event);
        self
    }

    pub fn with_peer(mut self, peer: Option<String>) -> Self {
        self.peer = peer;
        self
    }

    pub fn with_index(mut self, index: Option<String>) -> Self {
        self.index = index;
        self
    }

    pub fn with_query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    /// It ran. No status, because not every surface has one — an MCP tool answers inside a
    /// successful HTTP response, so recording `200` against a refusal would be a lie the
    /// `outcome` field then contradicts.
    pub fn succeeded(mut self) -> Self {
        self.outcome = Some(Outcome::Allowed);
        self
    }

    /// It did not run, or it failed. See [`AuditRecord::succeeded`] on the missing status.
    pub fn refused(mut self, reason: impl Into<String>) -> Self {
        self.outcome = Some(Outcome::Denied);
        self.reason = Some(reason.into());
        self
    }

    pub fn allowed(mut self, status: u16) -> Self {
        self.outcome = Some(Outcome::Allowed);
        self.status = Some(status);
        self
    }

    pub fn denied(mut self, status: u16, reason: impl Into<String>) -> Self {
        self.outcome = Some(Outcome::Denied);
        self.status = Some(status);
        self.reason = Some(reason.into());
        self
    }
}

/// What identifies a group of operations that are counted rather than listed.
///
/// Deliberately *not* keyed on the path: a hundred thousand writes to `docs` are one fact
/// about one key and one index, and splitting them by `/api/docs/document` versus
/// `/api/docs/_bulk` would trade the readability this exists for against a distinction
/// nobody investigating an incident is asking about.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RollupKey {
    event: Cow<'static, str>,
    key_id: Option<String>,
    index: Option<String>,
    outcome: Option<Outcome>,
}

#[derive(Debug, Clone)]
struct Rollup {
    ops: u64,
    errors: u64,
    window_start: String,
    label: Option<String>,
    role: Option<String>,
}

/// Counts held between flushes.
#[derive(Debug, Default)]
struct Rollups {
    buckets: HashMap<RollupKey, Rollup>,
}

impl Rollups {
    /// Fold one operation into its bucket.
    ///
    /// `status >= 400` counts as an error, so a rolled-up line still distinguishes "fifty
    /// thousand writes" from "fifty thousand writes, nine hundred of which failed" — the
    /// second is a story, and losing it would make rollup a worse trade than it is.
    fn fold(&mut self, record: AuditRecord) {
        let key = RollupKey {
            event: record.event.clone(),
            key_id: record.key_id.clone(),
            index: record.index.clone(),
            outcome: record.outcome,
        };
        let failed = record.status.is_some_and(|s| s >= 400);
        let entry = self.buckets.entry(key).or_insert_with(|| Rollup {
            ops: 0,
            errors: 0,
            window_start: record.ts.clone(),
            label: record.label.clone(),
            role: record.role.clone(),
        });
        entry.ops += 1;
        if failed {
            entry.errors += 1;
        }
    }

    fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Turn every bucket into a record and start a fresh window.
    fn drain(&mut self, now: &str) -> Vec<AuditRecord> {
        let mut out: Vec<AuditRecord> = self
            .buckets
            .drain()
            .map(|(key, value)| AuditRecord {
                ts: now.to_string(),
                event: key.event.clone(),
                outcome: key.outcome,
                key_id: key.key_id,
                label: value.label,
                role: value.role,
                index: key.index,
                ops: Some(value.ops),
                errors: Some(value.errors),
                window_start: Some(value.window_start),
                ..AuditRecord::default()
            })
            .collect();
        // Stable output: a HashMap drain is arbitrary, and a file whose line order changes
        // run to run is a file nobody can diff.
        out.sort_by(|a, b| (&a.key_id, &a.index, &a.event).cmp(&(&b.key_id, &b.index, &b.event)));
        out
    }
}

/// The last N records, newest last.
#[derive(Debug)]
struct Ring {
    capacity: usize,
    records: VecDeque<AuditRecord>,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            records: VecDeque::new(),
        }
    }

    fn push(&mut self, record: AuditRecord) {
        if self.records.len() == self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    /// Newest first, which is the order an operator reads an audit log in.
    fn recent(&self, limit: usize) -> Vec<AuditRecord> {
        self.records.iter().rev().take(limit).cloned().collect()
    }
}

/// What the emitting side hands to the writer.
enum Message {
    /// Keep this one verbatim.
    Detail(Box<AuditRecord>),
    /// Count it; the writer will emit a total.
    Rolled(Box<AuditRecord>),
    /// Flush and exit.
    Shutdown,
}

/// The handle every emit site holds.
///
/// Cheap to clone-by-`Arc` and safe to call when auditing is off: a disabled sink has no
/// channel and no thread, and every method short-circuits. Call sites do not branch on
/// whether auditing is configured.
pub struct AuditSink {
    tx: Option<SyncSender<Message>>,
    dropped: AtomicU64,
    ring: Arc<Mutex<Ring>>,
    record_query_text: bool,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for AuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditSink")
            .field("enabled", &self.tx.is_some())
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .finish()
    }
}

impl AuditSink {
    /// A sink that keeps nothing, for the default configuration and for tests.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            tx: None,
            dropped: AtomicU64::new(0),
            ring: Arc::new(Mutex::new(Ring::new(1))),
            record_query_text: false,
            worker: Mutex::new(None),
        })
    }

    /// Start the writer thread and return the handle emit sites share.
    ///
    /// A plain OS thread rather than a tokio task: the work is blocking file I/O on a timer,
    /// which is exactly what an async runtime is bad at hosting. It also means the trail
    /// keeps draining while the runtime is saturated — the moment it matters most.
    pub fn start(config: &AuditConfig) -> Arc<Self> {
        if !config.enabled {
            return Self::disabled();
        }

        let ring = Arc::new(Mutex::new(Ring::new(config.buffer_capacity)));
        let (tx, rx) = sync_channel(config.queue_capacity);
        let sink = Arc::new(Self {
            tx: Some(tx),
            dropped: AtomicU64::new(0),
            ring: Arc::clone(&ring),
            record_query_text: config.record_query_text,
            worker: Mutex::new(None),
        });

        let worker_config = config.clone();
        let worker_sink = Arc::clone(&sink);
        let handle = std::thread::Builder::new()
            .name("cameodb-audit".to_string())
            .spawn(move || writer_loop(rx, worker_config, ring, worker_sink))
            .expect("spawn audit writer thread");
        *sink.worker.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);

        info!(
            file = ?config.file,
            buffer_capacity = config.buffer_capacity,
            record_query_text = config.record_query_text,
            "Audit trail enabled"
        );
        sink
    }

    pub fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// Whether call sites should attach search query text.
    pub fn records_query_text(&self) -> bool {
        self.record_query_text
    }

    /// Keep this event verbatim.
    pub fn record(&self, record: AuditRecord) {
        self.send(|stamped| Message::Detail(Box::new(stamped)), record);
    }

    /// Count this event towards a periodic total instead of writing a line for it.
    pub fn record_rolled(&self, record: AuditRecord) {
        self.send(|stamped| Message::Rolled(Box::new(stamped)), record);
    }

    fn send(&self, wrap: impl FnOnce(AuditRecord) -> Message, mut record: AuditRecord) {
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        record.ts = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        match tx.try_send(wrap(record)) {
            Ok(()) => {}
            // Deliberately not `send`: blocking here would put audit I/O on the request
            // path, which is the one thing this module promises never to do.
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// How many records have been lost to a full queue since start.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The most recent records, newest first.
    pub fn recent(&self, limit: usize) -> Vec<AuditRecord> {
        self.ring
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .recent(limit)
    }

    /// Flush pending counts and stop the writer.
    ///
    /// Called on shutdown so a node that is asked to stop does not take the last rollup
    /// window to the grave with it — which, on a busy ingest node, is every write of the
    /// final ten seconds.
    pub fn shutdown(&self) {
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        // Blocking, unlike every other send: this one runs once, off the request path, and
        // its whole purpose is to not be dropped.
        let _ = tx.send(Message::Shutdown);
        if let Some(handle) = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = handle.join();
        }
    }
}

/// Drain records, fold the rolled ones, and flush on a timer until told to stop.
fn writer_loop(
    rx: Receiver<Message>,
    config: AuditConfig,
    ring: Arc<Mutex<Ring>>,
    sink: Arc<AuditSink>,
) {
    let mut file = config.file.as_ref().and_then(|path| {
        FileSink::open(path, config.max_file_bytes, config.max_files)
            .map_err(|e| warn!(path = %path.display(), error = %e, "Audit file unavailable; keeping the in-memory trail only"))
            .ok()
    });
    let mut rollups = Rollups::default();
    let mut reported_drops = 0u64;
    let interval = Duration::from_secs(config.rollup_secs.max(1));
    let mut next_flush = std::time::Instant::now() + interval;

    loop {
        let timeout = next_flush.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(timeout) {
            Ok(Message::Detail(record)) => emit(*record, &ring, &mut file),
            Ok(Message::Rolled(record)) => rollups.fold(*record),
            Ok(Message::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                flush(&mut rollups, &ring, &mut file, &sink, &mut reported_drops);
                if let Some(file) = file.as_mut() {
                    file.flush();
                }
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                flush(&mut rollups, &ring, &mut file, &sink, &mut reported_drops);
                if let Some(file) = file.as_mut() {
                    file.flush();
                }
                next_flush = std::time::Instant::now() + interval;
            }
        }
    }
}

/// Emit accumulated counts, plus a gap record if anything was lost since the last flush.
fn flush(
    rollups: &mut Rollups,
    ring: &Arc<Mutex<Ring>>,
    file: &mut Option<FileSink>,
    sink: &AuditSink,
    reported_drops: &mut u64,
) {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    if !rollups.is_empty() {
        for record in rollups.drain(&now) {
            emit(record, ring, file);
        }
    }

    let dropped = sink.dropped();
    if dropped > *reported_drops {
        let lost = dropped - *reported_drops;
        *reported_drops = dropped;
        // A warn! as well as a record: a gap means the node was busy enough to outrun its
        // own audit queue, which is an operational fact and not only a forensic one.
        warn!(
            dropped = lost,
            total = dropped,
            "Audit records dropped: the writer could not keep up"
        );
        emit(
            AuditRecord {
                ts: now,
                event: Cow::Borrowed("gap"),
                dropped: Some(lost),
                ..AuditRecord::default()
            },
            ring,
            file,
        );
    }
}

/// One record into both sinks.
fn emit(record: AuditRecord, ring: &Arc<Mutex<Ring>>, file: &mut Option<FileSink>) {
    if let Some(file) = file.as_mut() {
        file.write(&record);
    }
    // Also a `tracing` event, so a deployment already shipping logs somewhere gets the trail
    // without configuring a second path. Its own target, so it can be routed — or silenced —
    // independently of everything else the node says.
    tracing::info!(target: "cameodb::audit", record = %render(&record));
    ring.lock().unwrap_or_else(|p| p.into_inner()).push(record);
}

fn render(record: &AuditRecord) -> String {
    serde_json::to_string(record).unwrap_or_else(|e| format!(r#"{{"error":"{e}"}}"#))
}

/// The rotating JSON Lines file.
struct FileSink {
    path: PathBuf,
    handle: std::io::BufWriter<std::fs::File>,
    written: u64,
    max_bytes: u64,
    max_files: usize,
}

impl FileSink {
    fn open(path: &Path, max_bytes: u64, max_files: usize) -> std::io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let written = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path: path.to_path_buf(),
            handle: std::io::BufWriter::new(file),
            written,
            max_bytes,
            max_files: max_files.max(1),
        })
    }

    fn write(&mut self, record: &AuditRecord) {
        let line = render(record);
        if writeln!(self.handle, "{line}").is_err() {
            return;
        }
        self.written += line.len() as u64 + 1;
        if self.written >= self.max_bytes {
            self.rotate();
        }
    }

    fn flush(&mut self) {
        let _ = self.handle.flush();
    }

    /// `audit.jsonl` → `.1`, `.1` → `.2`, oldest discarded.
    ///
    /// A rename rather than a copy, so a reader holding the old inode keeps reading a
    /// complete file instead of one being truncated underneath it.
    fn rotate(&mut self) {
        self.flush();
        for index in (1..self.max_files).rev() {
            let from = self.numbered(index);
            let to = self.numbered(index + 1);
            if from.exists() {
                let _ = std::fs::rename(&from, &to);
            }
        }
        if std::fs::rename(&self.path, self.numbered(1)).is_err() {
            return;
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(file) => {
                self.handle = std::io::BufWriter::new(file);
                self.written = 0;
            }
            // The old handle still works — it now points at the rotated file. Writing to a
            // renamed file beats losing the trail because a directory turned read-only.
            Err(e) => warn!(error = %e, "Audit file could not be reopened after rotation"),
        }
    }

    fn numbered(&self, index: usize) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(format!(".{index}"));
        PathBuf::from(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_record(key: &str, index: &str, status: u16) -> AuditRecord {
        AuditRecord::http("PUT", &format!("/api/{index}/document"))
            .with_identity(
                Some(key.to_string()),
                Some("ingest".into()),
                Some("writer".into()),
            )
            .with_index(Some(index.to_string()))
            .allowed(status)
            .with_event("write_stats")
    }

    /// The premise of the whole design: writes outnumber reads by orders of magnitude, so a
    /// hundred thousand of them must cost a handful of lines rather than a hundred thousand.
    #[test]
    fn many_writes_collapse_into_one_line_per_key_and_index() {
        let mut rollups = Rollups::default();
        for _ in 0..100_000 {
            rollups.fold(http_record("k_a", "docs", 200));
        }
        for _ in 0..5 {
            rollups.fold(http_record("k_b", "docs", 200));
        }

        let drained = rollups.drain("2026-08-09T00:00:00.000Z");
        assert_eq!(
            drained.len(),
            2,
            "two keys writing one index should produce two lines, got {drained:#?}"
        );
        let a = drained
            .iter()
            .find(|r| r.key_id.as_deref() == Some("k_a"))
            .unwrap();
        assert_eq!(a.ops, Some(100_000));
        assert_eq!(a.event, "write_stats");
    }

    /// A rolled-up line still has to distinguish "fifty thousand writes" from "fifty
    /// thousand writes, nine hundred of which failed" — that difference is the reason
    /// somebody would read the line at all.
    #[test]
    fn a_rollup_counts_failures_separately() {
        let mut rollups = Rollups::default();
        for _ in 0..97 {
            rollups.fold(http_record("k_a", "docs", 200));
        }
        for _ in 0..3 {
            rollups.fold(http_record("k_a", "docs", 500));
        }

        let drained = rollups.drain("2026-08-09T00:00:00.000Z");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].ops, Some(100));
        assert_eq!(drained[0].errors, Some(3));
    }

    /// Draining must start a fresh window, or every flush would re-report the previous one
    /// and the totals would compound.
    #[test]
    fn draining_starts_a_new_window() {
        let mut rollups = Rollups::default();
        rollups.fold(http_record("k_a", "docs", 200));
        assert_eq!(rollups.drain("t1").len(), 1);
        assert!(rollups.is_empty());
        assert!(
            rollups.drain("t2").is_empty(),
            "a drained window must not re-report"
        );
    }

    /// Separate indexes are separate facts. Collapsing them would answer "this key wrote a
    /// lot" when the question is "this key wrote *to payroll*".
    #[test]
    fn rollups_do_not_merge_across_indexes() {
        let mut rollups = Rollups::default();
        rollups.fold(http_record("k_a", "docs", 200));
        rollups.fold(http_record("k_a", "payroll", 200));
        let drained = rollups.drain("t");
        assert_eq!(drained.len(), 2);
        assert!(
            drained
                .iter()
                .any(|r| r.index.as_deref() == Some("payroll"))
        );
    }

    #[test]
    fn the_ring_keeps_the_newest_and_reports_newest_first() {
        let mut ring = Ring::new(3);
        for i in 0..5u16 {
            ring.push(AuditRecord::http("GET", "/_indexes").allowed(i));
        }
        let recent = ring.recent(10);
        assert_eq!(recent.len(), 3, "capacity 3 must hold 3");
        assert_eq!(recent[0].status, Some(4), "newest first");
        assert_eq!(
            recent[2].status,
            Some(2),
            "oldest kept is the third from the end"
        );
    }

    #[test]
    fn the_ring_honours_a_smaller_limit() {
        let mut ring = Ring::new(10);
        for i in 0..5u16 {
            ring.push(AuditRecord::http("GET", "/_indexes").allowed(i));
        }
        assert_eq!(ring.recent(2).len(), 2);
    }

    /// A disabled sink is the default, so every call site runs through this path on a node
    /// that never configured auditing. It must be inert rather than merely quiet.
    #[test]
    fn a_disabled_sink_keeps_nothing_and_never_panics() {
        let sink = AuditSink::disabled();
        assert!(!sink.is_enabled());
        sink.record(AuditRecord::http("GET", "/_indexes").allowed(200));
        sink.record_rolled(http_record("k_a", "docs", 200));
        sink.shutdown();
        assert!(sink.recent(10).is_empty());
        assert_eq!(sink.dropped(), 0);
    }

    /// The query string belongs under `query`, where `record_query_text` governs it — not
    /// smuggled into `path`, which is recorded unconditionally.
    #[test]
    fn a_path_is_recorded_without_its_query_string() {
        let record = AuditRecord::http("GET", "/api/docs/search?q=alice%20smith");
        assert_eq!(record.path.as_deref(), Some("/api/docs/search"));
    }

    /// Absent rather than null: a `jq` filter over the trail should not have to distinguish
    /// "no index" from `"index": null`, and the file is a great deal smaller for it.
    #[test]
    fn fields_that_do_not_apply_are_absent_from_the_json() {
        let json = render(&AuditRecord::http("GET", "/_indexes").allowed(200));
        assert!(
            !json.contains(r#""index":"#),
            "unset fields must not be serialized: {json}"
        );
        assert!(
            !json.contains(r#""key_id":"#),
            "unset fields must not be serialized: {json}"
        );
        assert!(!json.contains("null"), "no nulls in the trail: {json}");
        assert!(json.contains(r#""outcome":"allowed""#), "{json}");
    }

    /// End to end through the real thread and file, because everything above tests a part.
    #[test]
    fn an_enabled_sink_writes_details_and_totals_to_its_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.jsonl");
        let sink = AuditSink::start(&AuditConfig {
            enabled: true,
            file: Some(path.clone()),
            rollup_secs: 1,
            ..AuditConfig::default()
        });

        sink.record(
            AuditRecord::http("GET", "/api/docs/search")
                .with_identity(Some("k_r".into()), None, Some("reader".into()))
                .with_index(Some("docs".into()))
                .allowed(200),
        );
        for _ in 0..1_000 {
            sink.record_rolled(http_record("k_w", "docs", 200));
        }
        // Shutdown flushes; without it the rolled-up window would still be in the writer.
        sink.shutdown();

        let contents = std::fs::read_to_string(&path).expect("audit file");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "one read and a thousand rolled-up writes are two lines, got:\n{contents}"
        );
        let parsed: Vec<AuditRecord> = lines
            .iter()
            .map(|l| serde_json::from_str(l).expect("each line is a JSON record"))
            .collect();
        let stats = parsed
            .iter()
            .find(|r| r.event == "write_stats")
            .expect("a totals line");
        assert_eq!(stats.ops, Some(1_000));
        assert!(
            parsed
                .iter()
                .any(|r| r.event == "http" && r.role.as_deref() == Some("reader"))
        );
    }

    /// The ring is what `/_admin/audit` serves, so it has to see the same records the file
    /// does — including the ones that were folded.
    #[test]
    fn the_ring_sees_what_the_file_sees() {
        let sink = AuditSink::start(&AuditConfig {
            enabled: true,
            rollup_secs: 1,
            ..AuditConfig::default()
        });
        sink.record(AuditRecord::http("GET", "/_indexes").allowed(200));
        sink.record_rolled(http_record("k_w", "docs", 200));
        sink.shutdown();

        let recent = sink.recent(10);
        assert_eq!(recent.len(), 2, "got {recent:#?}");
        assert!(recent.iter().any(|r| r.event == "write_stats"));
    }

    /// Loss must be visible in the trail itself, not only in a counter somebody has to know
    /// to look at.
    #[test]
    fn a_full_queue_drops_and_says_so() {
        let sink = AuditSink::start(&AuditConfig {
            enabled: true,
            // One slot, so a burst cannot fit and the drop path is taken deterministically
            // rather than only under a load the test would have to gamble on producing.
            queue_capacity: 1,
            rollup_secs: 1,
            ..AuditConfig::default()
        });
        for _ in 0..5_000 {
            sink.record(AuditRecord::http("GET", "/_indexes").allowed(200));
        }
        sink.shutdown();

        assert!(
            sink.dropped() > 0,
            "5 000 records through a 1-slot queue must drop some"
        );
        let recent = sink.recent(5_000);
        let gap = recent.iter().find(|r| r.event == "gap");
        assert!(gap.is_some(), "a drop must leave a gap record in the trail");
        assert!(gap.unwrap().dropped.unwrap_or(0) > 0);
    }

    /// Rotation is the difference between a bounded audit trail and a full disk.
    #[test]
    fn the_file_rotates_and_keeps_a_bounded_number_of_generations() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.jsonl");
        let mut file = FileSink::open(&path, 200, 2).expect("open");
        for i in 0..50u16 {
            file.write(&AuditRecord::http("GET", "/_indexes").allowed(i));
        }
        file.flush();

        assert!(path.exists(), "the active file must exist after rotation");
        assert!(
            path.with_extension("jsonl.1").exists(),
            "one generation back"
        );
        assert!(
            !path.with_extension("jsonl.3").exists(),
            "max_files = 2 must not keep a third generation"
        );
    }

    /// A misconfiguration that silently keeps nothing is worse than one that refuses to
    /// start, because it is discovered during the incident it was meant to explain.
    #[test]
    fn zeroed_settings_are_rejected_rather_than_silently_disabling_the_trail() {
        let base = AuditConfig {
            enabled: true,
            ..AuditConfig::default()
        };
        assert!(base.validate().is_ok());
        for (label, config) in [
            (
                "buffer_capacity",
                AuditConfig {
                    buffer_capacity: 0,
                    ..base.clone()
                },
            ),
            (
                "queue_capacity",
                AuditConfig {
                    queue_capacity: 0,
                    ..base.clone()
                },
            ),
            (
                "rollup_secs",
                AuditConfig {
                    rollup_secs: 0,
                    ..base.clone()
                },
            ),
            (
                "max_files",
                AuditConfig {
                    max_files: 0,
                    ..base.clone()
                },
            ),
        ] {
            let err = config.validate().expect_err("{label} = 0 must be rejected");
            assert!(
                err.contains(label),
                "the error must name {label}, got: {err}"
            );
        }
        // Disabled: nothing is enforced, because nothing is used.
        assert!(
            AuditConfig {
                enabled: false,
                buffer_capacity: 0,
                ..base
            }
            .validate()
            .is_ok(),
            "a disabled trail has no settings worth rejecting"
        );
    }
}
