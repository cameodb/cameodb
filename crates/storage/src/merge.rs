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

//!
//! [`ReclaimDeletesMergePolicy`] does the same without being asked, behind
//! `reclaim_deleted_ratio`: one segment at a time, once its deleted share passes the setting.

use std::collections::HashSet;
use std::sync::Arc;

use tantivy::FutureResult;
use tantivy::index::{SegmentId, SegmentMeta};
use tantivy::merge_policy::{LogMergePolicy, MergeCandidate, MergePolicy};

use crate::{HybridStore, StoreError};

/// Tantivy's default policy, plus a merge of its own for every segment whose deleted documents
/// exceed `ratio` of it.
///
/// The default ([`LogMergePolicy`]) merges segments of similar size once eight of them exist,
/// never because of deletes alone, and never a segment past ten million live documents — so
/// the largest segments of a large index, the ones whose deletes hold the most disk, keep them
/// for good. Its own `del_docs_ratio_before_merge` does not help: it pulls the whole level
/// around such a segment into the merge, and still skips the largest.
///
/// This keeps every candidate the default proposes, and adds a single-segment candidate for each
/// remaining segment over the ratio. Rewriting one segment costs in proportion to that segment;
/// it needs free disk for the segment's live documents while it runs, and gives back the whole
/// segment when it ends. A rewritten segment has no deletes, so it is not proposed again until
/// new deletes pass the ratio.
#[derive(Debug)]
pub(crate) struct ReclaimDeletesMergePolicy {
    index: String,
    ratio: f32,
    default_policy: LogMergePolicy,
}

impl ReclaimDeletesMergePolicy {
    /// The policy for `index`; `ratio` in `(0, 1)`, which the node's configuration checks.
    pub(crate) fn new(index: &str, ratio: f32) -> Self {
        Self {
            index: index.to_string(),
            ratio,
            default_policy: LogMergePolicy::default(),
        }
    }

    /// The deleted share of `segment`, past which it is rewritten.
    fn over_ratio(&self, segment: &SegmentMeta) -> bool {
        let max_doc = segment.max_doc();
        max_doc > 0
            && f64::from(segment.num_deleted_docs()) / f64::from(max_doc) > f64::from(self.ratio)
    }
}

impl MergePolicy for ReclaimDeletesMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let mut candidates = self.default_policy.compute_merge_candidates(segments);
        // A segment in two candidates would have the second refused by Tantivy; the default's
        // merge rewrites it anyway, deletes and all.
        let taken: HashSet<SegmentId> = candidates
            .iter()
            .flat_map(|candidate| candidate.0.iter().copied())
            .collect();
        for segment in segments {
            if taken.contains(&segment.id()) || !self.over_ratio(segment) {
                continue;
            }
            tracing::info!(
                index = %self.index,
                segment = %segment.id().short_uuid_string(),
                deleted = segment.num_deleted_docs(),
                documents = segment.max_doc(),
                "Reclaiming deleted documents: rewriting a segment past reclaim_deleted_ratio"
            );
            candidates.push(MergeCandidate(vec![segment.id()]));
        }
        candidates
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::Index;
    use tantivy::schema::Schema;

    fn index() -> Index {
        Index::create_in_ram(Schema::builder().build())
    }

    /// A segment of `max_doc` documents, `deleted` of them deleted.
    fn segment(index: &Index, max_doc: u32, deleted: u32) -> SegmentMeta {
        let meta = index.new_segment_meta(SegmentId::generate_random(), max_doc);
        if deleted == 0 {
            meta
        } else {
            meta.with_delete_meta(deleted, 0)
        }
    }

    fn proposed(candidates: &[MergeCandidate]) -> Vec<Vec<SegmentId>> {
        candidates
            .iter()
            .map(|candidate| candidate.0.clone())
            .collect()
    }

    #[test]
    fn a_segment_past_the_ratio_is_rewritten_alone_and_one_below_it_is_left() {
        let index = index();
        let heavy = segment(&index, 1_000, 400);
        let light = segment(&index, 1_000, 100);
        let clean = segment(&index, 1_000, 0);
        let policy = ReclaimDeletesMergePolicy::new("idx", 0.3);

        let candidates = policy.compute_merge_candidates(&[heavy.clone(), light, clean]);
        assert_eq!(proposed(&candidates), vec![vec![heavy.id()]]);
    }

    #[test]
    fn the_ratio_itself_is_not_past_it() {
        let index = index();
        let policy = ReclaimDeletesMergePolicy::new("idx", 0.3);
        assert!(
            policy
                .compute_merge_candidates(&[segment(&index, 1_000, 300)])
                .is_empty()
        );
        assert!(
            policy
                .compute_merge_candidates(&[segment(&index, 0, 0)])
                .is_empty()
        );
    }

    #[test]
    fn a_segment_too_large_for_the_default_policy_is_still_reclaimed() {
        let index = index();
        // Past `LogMergePolicy`'s ten million live documents, which it never merges.
        let large = segment(&index, 30_000_000, 12_000_000);
        let policy = ReclaimDeletesMergePolicy::new("idx", 0.3);
        assert!(
            LogMergePolicy::default()
                .compute_merge_candidates(std::slice::from_ref(&large))
                .is_empty()
        );
        assert_eq!(
            proposed(&policy.compute_merge_candidates(std::slice::from_ref(&large))),
            vec![vec![large.id()]]
        );
    }

    #[test]
    fn the_default_policys_candidates_are_kept_and_no_segment_is_proposed_twice() {
        let index = index();
        // Eight segments of a size is what the default policy merges; one of them is past the
        // ratio, and a ninth, much larger, sits alone on its level.
        let mut level: Vec<SegmentMeta> = (0..7).map(|_| segment(&index, 1_000, 0)).collect();
        let heavy_in_level = segment(&index, 1_000, 400);
        level.push(heavy_in_level.clone());
        let heavy_alone = segment(&index, 5_000_000, 3_000_000);
        let mut segments = level.clone();
        segments.push(heavy_alone.clone());
        let policy = ReclaimDeletesMergePolicy::new("idx", 0.3);

        let default_candidates = LogMergePolicy::default().compute_merge_candidates(&segments);
        assert_eq!(
            default_candidates.len(),
            1,
            "the eight of one size merge together"
        );
        let candidates = policy.compute_merge_candidates(&segments);
        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].0.len(),
            8,
            "the default's merge is kept as it was"
        );
        assert!(candidates[0].0.contains(&heavy_in_level.id()));
        assert_eq!(candidates[1].0, vec![heavy_alone.id()]);

        let mut seen = HashSet::new();
        for id in candidates.iter().flat_map(|candidate| candidate.0.iter()) {
            assert!(seen.insert(*id), "segment {id:?} proposed twice");
        }
    }
}
