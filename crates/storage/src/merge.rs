//! Merging an index's segments on request, which is what returns the disk its deletes hold.
//!
//! A Tantivy delete only marks a document dead in its segment; the bytes stay until a merge
//! rewrites the segment without it. The default merge policy merges segments of similar size
//! once enough of them exist, and never one past ten million live documents, so after a large
//! delete the space can stay indefinitely. Measured on 160 MB indexes with 62% of their
//! documents deleted: 176 MB → 65 MB in about 1.3 s each.
//!
//! The merge is started on the shard's writer thread, after a commit, and runs on Tantivy's
//! merge threads: the writer thread goes back to writes at once, so a merge of large segments
//! does not stall the shard. It needs disk for the merged segment while it runs — at most the
//! size of the live documents — and the old segments go when it ends.

use std::sync::Arc;

use tantivy::FutureResult;
use tantivy::index::{SegmentId, SegmentMeta};

use crate::{HybridStore, StoreError};

/// A merge that has been started, and what it started from.
pub struct MergeStarted {
    /// Segments being merged into one.
    pub segments: usize,
    /// Documents the merged segment will hold.
    pub live_docs: u64,
    /// Deleted documents the merge drops.
    pub deleted_docs: u64,
    /// The index's Tantivy files before the merge.
    pub bytes_before: u64,
    /// Resolves when the merge ends. Await it, then call [`HybridStore::finish_merge`].
    pub merge: FutureResult<Option<SegmentMeta>>,
}

impl HybridStore {
    /// Commit `index`, then start merging all of its segments into one.
    ///
    /// The commit comes first so the deletes still in the writer's buffer are applied and the
    /// merge drops them too. `None` when there is nothing to gain: the index is not open on
    /// this shard, or it is already one segment with no deletes.
    ///
    /// Tantivy refuses a merge that includes a segment its own merge policy is already merging;
    /// that comes back as the merge's error, and asking again once that merge is done works.
    pub fn start_full_merge(&self, index: &str) -> Result<Option<MergeStarted>, StoreError> {
        self.commit_index(index)?;

        let Some(writer_arc) = self
            .writers
            .get(index)
            .map(|writer| Arc::clone(writer.value()))
        else {
            return Ok(None);
        };
        let mut writer = writer_arc.lock().unwrap_or_else(|poisoned| {
            tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
            poisoned.into_inner()
        });

        let metas = writer.index().searchable_segment_metas()?;
        if metas.is_empty() || (metas.len() == 1 && !metas[0].has_deletes()) {
            return Ok(None);
        }
        let ids: Vec<SegmentId> = metas.iter().map(SegmentMeta::id).collect();
        let started = MergeStarted {
            segments: metas.len(),
            live_docs: metas.iter().map(|m| u64::from(m.num_docs())).sum(),
            deleted_docs: metas.iter().map(|m| u64::from(m.num_deleted_docs())).sum(),
            bytes_before: self.measure_tantivy_bytes(index)?,
            merge: writer.merge(&ids),
        };
        Ok(Some(started))
    }

    /// After a merge has ended: let go of the segments it replaced, and return their disk.
    ///
    /// Tantivy deletes the replaced files when the merge ends, but the reader still maps them
    /// (it reloads by hand, on commits), and a mapped file's disk is not freed until it is
    /// unmapped — so the reader is reloaded first, and the deletion run again for whatever the
    /// first pass could not remove. Returns the index's Tantivy bytes afterwards.
    pub fn finish_merge(&self, index: &str) -> Result<u64, StoreError> {
        self.smart_refresh_reader(index)?;
        if let Some(writer_arc) = self
            .writers
            .get(index)
            .map(|writer| Arc::clone(writer.value()))
        {
            let writer = writer_arc.lock().unwrap_or_else(|poisoned| {
                tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                poisoned.into_inner()
            });
            writer.garbage_collect_files().wait()?;
        }
        self.measure_tantivy_bytes(index)
    }
}
