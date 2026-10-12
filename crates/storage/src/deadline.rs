//! The deadline a search runs under, enforced where the work actually is.
//!
//! Nothing above this module can stop a search once it is running: the read is a
//! `spawn_blocking` closure, which tokio never cancels — the HTTP timeout and the worker
//! liveness cap only abandon the *answer*, while the segment walk behind it runs to
//! completion holding a read thread. Under sustained load that is how the pool ends up
//! working through searches whose clients have all gone.
//!
//! So the budget travels into the walk itself. [`DeadlineQuery`] wraps the parsed query and
//! its weight checks the deadline at intervals inside each of tantivy's three iteration
//! shapes — the scored walk, the buffered unscored walk, and the pruning walk the top-docs
//! collector uses. The interval is a counter, not a clock read per document: an
//! `Instant::now()` every [`CHECK_EVERY`] hits is below the noise floor of the walk it
//! samples, while a check per document measurably costs.
//!
//! `tripped` is what reports the outcome back up. Tantivy's iteration carries only a
//! `TantivyError`, so the deadline cannot ride out as its own variant; it leaves as an
//! internal error and the flag says whether it is ours — the caller checks the flag rather
//! than matching message text, which nothing should ever have to parse.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tantivy::query::{EnableScoring, Explanation, Query, Weight};
use tantivy::{DocId, Score, SegmentReader, TantivyError};

use crate::StoreError;

/// Documents walked between two clock reads. `Instant::now()` at this cadence is noise
/// beside the segment walk it samples.
const CHECK_EVERY: u64 = 4096;

/// The text the deadline error rides out of a tantivy walk under. Never matched on — the
/// `tripped` flag is the signal; the string is only what the error says if it is ever seen.
const DEADLINE_MARKER: &str = "cameodb search deadline exceeded";

/// A wall-clock budget for one search, shared by the loops that spend it.
///
/// `Clone`d freely: every handle stamps the same `started` instant and reads the same
/// `tripped` flag, so a deadline observed inside one segment's walk is known to every
/// other handle the search holds.
#[derive(Clone, Debug)]
pub struct SearchDeadline {
    started: Instant,
    budget: Duration,
    /// Set the first time `expired` observes the budget spent. Written by whichever check
    /// fires first and read by the caller to tell the deadline's error from a real
    /// tantivy failure — elapsed time alone cannot answer "did *this* run hit its
    /// deadline", because the flag is the record of the check that fired.
    tripped: Arc<AtomicBool>,
}

impl SearchDeadline {
    /// A budget measured from `started`, so the deadline runs on the same clock the
    /// request does — including whatever the caller already spent queueing.
    pub fn new(started: Instant, budget: Duration) -> Self {
        Self {
            started,
            budget,
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether the budget is spent. The check *is* the enforcement — a `true` answer is
    /// latched into `tripped` so the caller can distinguish a deadline error from a
    /// tantivy one afterwards, when only the flag survives the error channel.
    pub fn expired(&self) -> bool {
        let expired = self.started.elapsed() > self.budget;
        if expired {
            self.tripped.store(true, Ordering::Relaxed);
        }
        expired
    }

    /// Whether any `expired` check has ever answered `true`. Monotonic: once a deadline
    /// has fired it stays fired, which is what lets the flag stand in for the check at
    /// points where only the error made it out.
    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }

    /// The error form of [`expired`](Self::expired): fails the operation in the store's
    /// own variant rather than passing a flag back for the caller to translate.
    pub fn check(&self) -> Result<(), StoreError> {
        if self.expired() {
            return Err(self.exceeded_error());
        }
        Ok(())
    }

    /// What [`check`](Self::check) fails with: the budget as it was given and the time
    /// actually spent, in the units an error message can act on.
    fn exceeded_error(&self) -> StoreError {
        StoreError::SearchDeadlineExceeded {
            elapsed_ms: self.started.elapsed().as_millis() as u64,
            budget_ms: self.budget.as_millis() as u64,
        }
    }
}

/// A [`Query`] that fails with the deadline's marker once its [`SearchDeadline`] is spent.
///
/// The check lives in the weight, not the query: the query is the plan and the weight is
/// the walk, and only the walk knows how far it has gone. `Clone` is implemented by hand
/// because `Box<dyn Query>` is not `Clone` — `box_clone` is what it has instead.
#[derive(Debug)]
pub struct DeadlineQuery {
    inner: Box<dyn Query>,
    deadline: SearchDeadline,
}

impl DeadlineQuery {
    pub fn new(inner: Box<dyn Query>, deadline: SearchDeadline) -> Self {
        Self { inner, deadline }
    }
}

impl Clone for DeadlineQuery {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.box_clone(),
            deadline: self.deadline.clone(),
        }
    }
}

impl Query for DeadlineQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(DeadlineWeight {
            inner: self.inner.weight(enable_scoring)?,
            deadline: self.deadline.clone(),
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.inner.query_terms(visitor);
    }
}

/// The [`DeadlineQuery`]'s walk: the inner weight's, with the budget sampled on the way.
///
/// `scorer`, `explain` and `count` are the inner weight's unchanged — the enforcement
/// lives in the three iteration shapes, which is every way a collector can drive a walk.
/// `for_each` and `for_each_no_score` run their own loops rather than delegating with a
/// checked callback because the callback cannot stop the walk — it returns nothing — so a
/// deadline observed through it would still walk every remaining document before the
/// error could be raised. Driving the scorer directly is the same walk tantivy's own
/// default implementations run; it just gets to leave early.
struct DeadlineWeight {
    inner: Box<dyn Weight>,
    deadline: SearchDeadline,
}

impl DeadlineWeight {
    /// The error a tripped deadline leaves the walk on. `InternalError` because the
    /// iteration channel has no other variant; the `tripped` flag, not this text, is what
    /// the caller reads.
    fn marker() -> TantivyError {
        TantivyError::InternalError(DEADLINE_MARKER.to_string())
    }
}

impl Weight for DeadlineWeight {
    fn scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        self.inner.scorer(reader, boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.inner.explain(reader, doc)
    }

    fn count(&self, reader: &SegmentReader) -> tantivy::Result<u32> {
        self.inner.count(reader)
    }

    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> tantivy::Result<()> {
        // Checked up front as well as on the interval: a deadline already spent must not
        // be sampled at all before answering, and one that expires under CHECK_EVERY docs
        // of work would otherwise walk the whole segment unobserved.
        if self.deadline.expired() {
            return Err(Self::marker());
        }
        let mut scorer = self.inner.scorer(reader, 1.0)?;
        let mut seen = 0u64;
        let mut doc = scorer.doc();
        while doc != tantivy::TERMINATED {
            callback(doc, scorer.score());
            seen += 1;
            if seen.is_multiple_of(CHECK_EVERY) && self.deadline.expired() {
                return Err(Self::marker());
            }
            doc = scorer.advance();
        }
        Ok(())
    }

    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> tantivy::Result<()> {
        if self.deadline.expired() {
            return Err(Self::marker());
        }
        let mut docset = self.inner.scorer(reader, 1.0)?;
        let mut buffer = [0u32; tantivy::COLLECT_BLOCK_BUFFER_LEN];
        // Buffered the way tantivy's own unscored walk is: the scorer fills a block and
        // the clock is read per block, 64 documents at a time.
        let mut buffers = 0u64;
        loop {
            let num_items = docset.fill_buffer(&mut buffer);
            callback(&buffer[..num_items]);
            buffers += 1;
            if buffers.is_multiple_of(64) && self.deadline.expired() {
                return Err(Self::marker());
            }
            if num_items != buffer.len() {
                break;
            }
        }
        Ok(())
    }

    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> tantivy::Result<()> {
        if self.deadline.expired() {
            return Err(Self::marker());
        }
        // Delegated, unlike the two above: the pruning walk is where the inner weight's
        // own optimizations live — Block-WAND skips whole blocks by comparing the score a
        // block can reach against the current threshold — and reimplementing that loop
        // would lose the skipping the walk exists for. The callback still gets a vote on
        // the threshold, though, and `Score::MAX` is a threshold no block can reach: the
        // walk ends almost immediately, with the pruning machinery intact.
        let deadline = self.deadline.clone();
        let mut seen = 0u64;
        self.inner
            .for_each_pruning(threshold, reader, &mut |doc, score| {
                seen += 1;
                if seen.is_multiple_of(CHECK_EVERY) && deadline.expired() {
                    return Score::MAX;
                }
                callback(doc, score)
            })?;
        // The callback could not return an error, only the threshold — so the walk ending
        // is what the check reports, and the flag says whether the deadline was the cause.
        if self.deadline.tripped() {
            return Err(Self::marker());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pruning walk is the one the top-docs collector takes, and the deadline must
    /// end it: the callback cannot raise an error, so the `Score::MAX` threshold has to
    /// empty the walk and the flag has to be what reports it.
    #[test]
    fn an_expired_deadline_fails_a_pruning_search_and_marks_itself() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        let title = schema_builder.add_text_field("title", tantivy::schema::TEXT);
        let index = tantivy::Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer(50_000_000).unwrap();
        for i in 0..5000 {
            writer
                .add_document(tantivy::doc!(title => format!("doc {i}")))
                .unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();

        let deadline = SearchDeadline::new(
            Instant::now() - Duration::from_secs(5),
            Duration::from_millis(1),
        );
        let query = DeadlineQuery::new(Box::new(tantivy::query::AllQuery), deadline.clone());
        let collector = tantivy::collector::TopDocs::with_limit(5).order_by_score();

        assert!(
            searcher.search(&query, &collector).is_err(),
            "an expired deadline must fail the walk, not answer it"
        );
        assert!(
            deadline.tripped(),
            "the flag is how the caller tells it apart"
        );
    }

    /// A deadline with budget left is transparent: the walk answers exactly what the
    /// inner query would have.
    #[test]
    fn a_deadline_with_budget_answers_what_the_inner_query_would() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        let title = schema_builder.add_text_field("title", tantivy::schema::TEXT);
        let index = tantivy::Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer(50_000_000).unwrap();
        for i in 0..100 {
            writer
                .add_document(tantivy::doc!(title => format!("doc {i}")))
                .unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();

        let deadline = SearchDeadline::new(Instant::now(), Duration::from_secs(60));
        let query = DeadlineQuery::new(Box::new(tantivy::query::AllQuery), deadline);
        let collector = tantivy::collector::TopDocs::with_limit(10).order_by_score();
        let hits = searcher.search(&query, &collector).unwrap();
        assert_eq!(hits.len(), 10);
    }
}
