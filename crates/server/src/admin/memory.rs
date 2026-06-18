//! Memory observability and admin operations.
//!
//! Contains:
//! - Process/Jemalloc memory stats types and platform-specific readers
//! - Admin message types (`GetAdminMemory`, `PurgeAdminMemory`, etc.)
//! - Report types (`AdminMemoryReport`, `AdminIndexCommitReport`, etc.)
//! - `Message` implementations on `NodeOrchestrator`
//! - Orchestrator helper methods for admin operations

#[cfg(target_os = "linux")]
use std::fs;

use kameo::message::{Context, Message};
use serde::{Deserialize, Serialize};

use crate::node_orchestrator::{NodeOrchestrator, OrchestratorError};

// ── Memory stat structs ──

/// Memory snapshot read from /proc/self/status (Linux-only; fields are None on other OSes).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessMemoryStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_size_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_rss_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_anon_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_file_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_shmem_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_data_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_swap_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u64>,
}

/// Jemalloc-native allocator statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JemallocStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resident: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained: Option<u64>,
}

/// Report returned by GET /_admin/memory and POST /_admin/memory/purge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMemoryReport {
    pub process: ProcessMemoryStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_after_purge: Option<ProcessMemoryStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jemalloc: Option<JemallocStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_result: Option<i32>,
}

// ── Index admin report structs ──

/// Per-shard error detail for index admin operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardError {
    pub shard_id: String,
    pub error: String,
}

/// Report returned by POST /_admin/index/{index}/commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexCommitReport {
    pub index: String,
    pub shards_total: usize,
    pub shards_committed: usize,
    pub errors: Vec<ShardError>,
}

/// Report returned by POST /_admin/index/{index}/evict_writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexEvictWriterReport {
    pub index: String,
    pub shards_total: usize,
    pub writers_evicted: usize,
    pub writers_missing: usize,
    pub errors: Vec<ShardError>,
}

// ── Admin message types ──

#[derive(Debug, Clone)]
pub struct GetAdminMemory;

#[derive(Debug, Clone)]
pub struct PurgeAdminMemory {
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct CommitAdminIndex {
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct EvictAdminIndexWriter {
    pub index: String,
}

// ── Platform-specific memory readers ──

#[cfg(target_os = "linux")]
fn read_process_memory_stats() -> ProcessMemoryStats {
    let mut stats = ProcessMemoryStats::default();
    let Ok(contents) = fs::read_to_string("/proc/self/status") else {
        return stats;
    };
    for line in contents.lines() {
        let parse = |l: &str| -> Option<u64> { l.split_whitespace().nth(1)?.parse().ok() };
        if line.starts_with("VmSize:") {
            stats.vm_size_kb = parse(line);
        } else if line.starts_with("VmRSS:") {
            stats.vm_rss_kb = parse(line);
        } else if line.starts_with("RssAnon:") {
            stats.rss_anon_kb = parse(line);
        } else if line.starts_with("RssFile:") {
            stats.rss_file_kb = parse(line);
        } else if line.starts_with("RssShmem:") {
            stats.rss_shmem_kb = parse(line);
        } else if line.starts_with("VmData:") {
            stats.vm_data_kb = parse(line);
        } else if line.starts_with("VmSwap:") {
            stats.vm_swap_kb = parse(line);
        } else if line.starts_with("Threads:") {
            stats.threads = parse(line);
        }
    }
    stats
}

#[cfg(target_os = "macos")]
fn read_process_memory_stats() -> ProcessMemoryStats {
    let mut stats = ProcessMemoryStats::default();
    unsafe {
        let pid = std::process::id() as i32;
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let size = libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            std::mem::size_of::<libc::proc_taskinfo>() as i32,
        );
        if size == std::mem::size_of::<libc::proc_taskinfo>() as i32 {
            stats.vm_rss_kb = Some(info.pti_resident_size / 1024);
            stats.vm_size_kb = Some(info.pti_virtual_size / 1024);
            stats.threads = Some(info.pti_threadnum as u64);
        } else {
            tracing::warn!("proc_pidinfo(PROC_PIDTASKINFO) failed, size={}", size);
        }
    }
    stats
}

#[cfg(target_os = "windows")]
fn read_process_memory_stats() -> ProcessMemoryStats {
    let mut stats = ProcessMemoryStats::default();
    let pid = std::process::id();
    let output = match std::process::Command::new("wmic")
        .args([
            "process",
            "where",
            &format!("ProcessId={}", pid),
            "get",
            "VirtualSize,WorkingSetSize,ThreadCount",
            "/format:list",
        ])
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!("wmic command failed: {}", e);
            return stats;
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let Ok(num) = value.trim().parse::<u64>() else {
            continue;
        };
        match key.trim() {
            "VirtualSize" => stats.vm_size_kb = Some(num / 1024),
            "WorkingSetSize" => stats.vm_rss_kb = Some(num / 1024),
            "ThreadCount" => stats.threads = Some(num),
            _ => {}
        }
    }
    stats
}

#[cfg(target_os = "linux")]
fn call_memory_purge(force: bool) -> i32 {
    let name: &[u8] = if force {
        b"arena.4294967295.purge\0"
    } else {
        b"arena.4294967295_decay.purge\0"
    };
    unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr().cast(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    }
}

#[cfg(target_os = "linux")]
fn read_jemalloc_stats() -> JemallocStats {
    let mut stats = JemallocStats::default();

    // Advance epoch to refresh cached statistics before reading.
    let mut epoch: u64 = 1;
    let mut epoch_sz = std::mem::size_of::<u64>();
    let epoch_ret = unsafe {
        tikv_jemalloc_sys::mallctl(
            b"epoch\0".as_ptr().cast(),
            (&mut epoch as *mut u64).cast(),
            &mut epoch_sz,
            std::ptr::null_mut(),
            0,
        )
    };
    if epoch_ret != 0 {
        tracing::warn!("jemalloc epoch refresh failed with code {}", epoch_ret);
    }

    let read_u64 = |name: &[u8]| -> Option<u64> {
        let mut value: u64 = 0;
        let mut sz = std::mem::size_of::<u64>();
        let ret = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr().cast(),
                (&mut value as *mut u64).cast(),
                &mut sz,
                std::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 {
            Some(value)
        } else {
            tracing::warn!(
                "jemalloc mallctl '{}' failed with code {} (errno {})",
                String::from_utf8_lossy(&name[..name.len().saturating_sub(1)]),
                ret,
                ret
            );
            None
        }
    };

    stats.allocated = read_u64(b"stats.allocated\0");
    stats.active = read_u64(b"stats.active\0");
    stats.resident = read_u64(b"stats.resident\0");
    stats.metadata = read_u64(b"stats.metadata\0");
    stats.retained = read_u64(b"stats.retained\0");

    stats
}

// ── Message implementations on NodeOrchestrator ──

impl Message<GetAdminMemory> for NodeOrchestrator {
    type Reply = Result<AdminMemoryReport, OrchestratorError>;

    async fn handle(
        &mut self,
        _msg: GetAdminMemory,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let report = tokio::task::spawn_blocking(|| {
            #[cfg(target_os = "linux")]
            let jemalloc = Some(read_jemalloc_stats());
            #[cfg(not(target_os = "linux"))]
            let jemalloc = None;

            AdminMemoryReport {
                process: read_process_memory_stats(),
                process_after_purge: None,
                jemalloc,
                purge_result: None,
            }
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(report)
    }
}

impl Message<PurgeAdminMemory> for NodeOrchestrator {
    type Reply = Result<AdminMemoryReport, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: PurgeAdminMemory,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        #[allow(unused_variables)]
        let force = msg.force;
        let report = tokio::task::spawn_blocking(move || {
            let process = read_process_memory_stats();
            #[cfg(target_os = "linux")]
            let purge_result = Some(call_memory_purge(force));
            #[cfg(not(target_os = "linux"))]
            let purge_result = None;
            let process_after_purge = read_process_memory_stats();

            #[cfg(target_os = "linux")]
            let jemalloc = Some(read_jemalloc_stats());
            #[cfg(not(target_os = "linux"))]
            let jemalloc = None;

            AdminMemoryReport {
                process,
                process_after_purge: Some(process_after_purge),
                jemalloc,
                purge_result,
            }
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(report)
    }
}

impl Message<CommitAdminIndex> for NodeOrchestrator {
    type Reply = Result<AdminIndexCommitReport, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: CommitAdminIndex,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.orch_admin_commit_index(msg.index).await
    }
}

impl Message<EvictAdminIndexWriter> for NodeOrchestrator {
    type Reply = Result<AdminIndexEvictWriterReport, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: EvictAdminIndexWriter,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.orch_admin_evict_index_writer(msg.index).await
    }
}

// ── NodeOrchestrator helper methods for admin operations ──

impl NodeOrchestrator {
    pub(crate) async fn orch_admin_commit_index(
        &self,
        index: String,
    ) -> Result<AdminIndexCommitReport, OrchestratorError> {
        let mut committed = 0usize;
        let mut errors: Vec<ShardError> = Vec::new();
        for (shard_id, shard) in &self.shards {
            match shard.admin_commit_via_channel(index.clone()).await {
                Ok(()) => committed += 1,
                Err(e) => errors.push(ShardError {
                    shard_id: shard_id.to_string(),
                    error: e.to_string(),
                }),
            }
        }
        Ok(AdminIndexCommitReport {
            index,
            shards_total: self.shards.len(),
            shards_committed: committed,
            errors,
        })
    }

    pub(crate) async fn orch_admin_evict_index_writer(
        &self,
        index: String,
    ) -> Result<AdminIndexEvictWriterReport, OrchestratorError> {
        let mut evicted = 0usize;
        let mut missing = 0usize;
        let mut errors: Vec<ShardError> = Vec::new();
        for (shard_id, shard) in &self.shards {
            match shard.admin_evict_writer_via_channel(index.clone()).await {
                Ok(true) => evicted += 1,
                Ok(false) => missing += 1,
                Err(e) => errors.push(ShardError {
                    shard_id: shard_id.to_string(),
                    error: e.to_string(),
                }),
            }
        }
        Ok(AdminIndexEvictWriterReport {
            index,
            shards_total: self.shards.len(),
            writers_evicted: evicted,
            writers_missing: missing,
            errors,
        })
    }
}
