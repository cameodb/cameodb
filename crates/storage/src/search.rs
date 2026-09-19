//! The read side of `HybridStore`: reader pool, query validation, `search_documents`, key
//! lookups and index statistics.
//!
//! There is no cache of document bodies here. There used to be — 1024 per index, mirroring rows
//! of `data_<index>` — and it was removed on 2026-09-19 because it was a third caching layer
//! over two that already work: redb's page cache (32 MB per shard at the floor, tiered by
//! database size) and the operating system's. Measured, a point read costs 0.62 µs without it,
//! and what it removed from that was one B-tree descent — the body is copied out either way.
//! What it cost was a generation protocol whose only purpose was to stop it serving a body a
//! write had superseded. `get_by_key` reads redb and is right by construction.
use crate::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde_json::Value as JsonValue;
use tantivy::schema::{Document, Value as TantivyValue};
use tantivy::{Index, IndexReader};
use tracing::{debug, trace, warn};
use walkdir::WalkDir;

/// Wrapper to handle both sorted (u64) and unsorted (f32) search results
pub(crate) enum SearchResult {
    Unsorted(Vec<(f32, tantivy::DocAddress)>),
    /// The key slot is `Some` only when the sort was on the document key itself — then the
    /// collector's sort key *is* the document id, already read off the column, and no hit
    /// needs a second read to answer it. Every other sort drops its key here: the ordering
    /// lives in the sequence.
    Sorted(Vec<(Option<String>, tantivy::DocAddress)>),
}

pub(crate) const TANTIVY_DATA_FILE_EXTENSIONS: &[&str] =
    &["store", "fast", "idx", "doc", "pos", "term"];

/// Measure the on-disk size of a Tantivy index.
///
/// Callers pass the index directory (`<shard>/indices/<name>`). `fs::metadata(dir).len()`
/// reports the size of the directory entry itself — a couple of KB regardless of contents —
/// so summing the files inside is the only way to size an index. Tantivy lays its segment
/// files out flat, so one non-recursive `read_dir` suffices: a `getdents` plus a `stat`
/// per file.
///
/// A plain file path is measured directly, so the function is meaningful for any path a
/// caller might reasonably hand it.
///
/// Returns `None` when the path does not exist yet (a brand-new index).
pub(crate) fn index_size_bytes(index_path: &Path) -> Option<u64> {
    let metadata = fs::metadata(index_path).ok()?;
    if metadata.is_file() {
        return Some(metadata.len());
    }

    let mut total = 0u64;
    for entry in fs::read_dir(index_path).ok()?.flatten() {
        if let Ok(entry_meta) = entry.metadata()
            && entry_meta.is_file()
        {
            total = total.saturating_add(entry_meta.len());
        }
    }
    Some(total)
}

/// A commit-cadence memory budget, and when it was measured.
///
/// The budget comes from [`index_size_bytes`], which is a `read_dir` plus a `stat` per file —
/// some three hundred syscalls on a fifty-segment index. What it buys is a five-bucket size
/// class (100MB / 500MB / 2GB / 8GB) that scales how many operations accumulate before the next
/// commit. `commit_index` used to re-measure it after *every* commit, on the writer thread, in
/// the window between the Tantivy commit and the checkpoint transaction — precision nobody
/// could use, since an index crosses one of those boundaries after hundreds of megabytes of
/// writing. The timestamp is what lets one measurement stand for many commits.
#[derive(Clone, Copy)]
pub(crate) struct BudgetCacheEntry {
    pub(crate) budget: usize,
    pub(crate) measured_at: Instant,
}

/// How long a measured budget stands before the index directory is walked again.
///
/// Bounds the walk at two per index per minute however hard the index is being written, against
/// one per commit before. A budget this stale is still right: crossing a size class takes orders
/// of magnitude longer than 30s of writing at any rate this engine sustains, and being one
/// bucket behind for a few seconds costs a commit cadence slightly off its optimum, not
/// correctness.
pub(crate) const BUDGET_CACHE_TTL: Duration = Duration::from_secs(30);

impl BudgetCacheEntry {
    pub(crate) fn now(budget: usize) -> Self {
        Self {
            budget,
            measured_at: Instant::now(),
        }
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.measured_at.elapsed() >= BUDGET_CACHE_TTL
    }
}

/// Unified cache entry for index sizes (both Tantivy directory and Redb table) with timestamp
#[derive(Clone)]
pub(crate) struct IndexSizeCache {
    pub(crate) tantivy_bytes: u64,
    pub(crate) redb_bytes: u64,
    pub(crate) document_count: u64,
    pub(crate) timestamp: Instant,
}

/// Result of batch index size measurement
pub(crate) struct IndexSizes {
    pub(crate) tantivy_bytes: u64,
    pub(crate) redb_bytes: u64,
    pub(crate) document_count: u64,
}

impl HybridStore {
    /// Smart refresh strategy for reader cache
    /// Tries fast reload first, falls back to remove + recreate if reload fails
    /// This preserves cache when possible while ensuring data freshness
    pub(crate) fn smart_refresh_reader(&self, index: &str) -> Result<(), StoreError> {
        // Fast path: Try to reload existing reader
        if let Some(reader_ref) = self.readers.get(index) {
            match reader_ref.value().reload() {
                Ok(_) => {
                    tracing::debug!(index = %index, "Reader reloaded successfully (fast path)");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(index = %index, error = %e, "Reader reload failed, falling back to recreation");
                }
            }
        }

        // Fallback: Remove and recreate (reliable path)
        self.readers.remove(index);
        tracing::debug!(index = %index, "Reader cache cleared, will recreate on next search (reliable path)");
        Ok(())
    }

    /// Get document by key from specific index
    pub fn get_by_key(&self, index: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => match data_table.get(key)? {
                Some(value) => Ok(Some(value.value().to_vec())),
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist (index was deleted)
        }
    }

    /// Batch retrieve documents by keys from specific index
    /// More efficient than multiple get_by_key calls - uses single transaction
    pub fn get_batch_by_keys(
        &self,
        index: &str,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        // Single read transaction for all keys.
        let read_txn = self.kv.begin_read()?;
        let data_table = match read_txn.open_table(data_table_def) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()), // Table doesn't exist
        };

        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(value) = data_table.get(key.as_str())? {
                results.push((key.clone(), value.value().to_vec()));
            }
            // Skip keys that don't exist (document may have been deleted)
        }

        Ok(results)
    }

    /// Helper: Get fields cache (Lock-Free Read)
    pub(crate) fn get_fields_for_index(
        &self,
        index: &str,
        tantivy_index: &Index,
    ) -> Result<SchemaFields, StoreError> {
        // Fast path: fields already cached
        if let Some(fields) = self.fields_cache.get(index) {
            return Ok(fields.value().clone());
        }

        // Derive fields from the opened Tantivy index (Field handles must match the index)
        let fields = Self::load_fields_from_existing_index(tantivy_index)?;
        self.fields_cache.insert(index.to_string(), fields.clone());
        Ok(fields)
    }

    /// Get or create a cached IndexReader for the given index.
    ///
    /// Lock-free fast path via DashMap. Readers reload only when `commit_index` tells them to.
    pub(crate) fn get_reader(
        &self,
        index: &str,
    ) -> Result<Option<(IndexReader, SchemaFields)>, StoreError> {
        // Fast path: Zero-lock retrieval from cache
        if let Some(reader_ref) = self.readers.get(index) {
            let reader = reader_ref.value();
            // No reload() here: `commit_index` reloads through `smart_refresh_reader` as part
            // of the commit, so a cached reader is already current.

            // Get fields (fast lookup)
            let tantivy_index = reader.searcher().index().clone();
            let fields = self.get_fields_for_index(index, &tantivy_index)?;

            return Ok(Some((reader.clone(), fields)));
        }

        // Slow path: Index not cached, need to open and cache it
        let index_path = self.index_dir(index)?;
        if !index_path.exists() || !index_path.join("meta.json").exists() {
            return Ok(None);
        }

        // Use DashMap entry API for concurrent-safe creation
        let reader = self
            .readers
            .entry(index.to_string())
            .or_try_insert_with(|| {
                let tantivy_index = open_tantivy_index(&index_path)?;

                // `ReloadPolicy::Manual`, deliberately. Every commit in this process goes
                // through `commit_index`, which reloads the reader itself, so the alternative
                // (`OnCommitWithDelay`) would only ever reload a *second* time for a commit
                // already reflected here. That redundant reload is not free:
                //
                // - it makes tantivy spawn a `thread-tantivy-meta-file-watcher` per open
                //   index, each waking every 500ms forever to read and checksum meta.json,
                // - and because reload() rebuilds every SegmentReader, it discards the caches
                //   `warm_index` had just filled for the generation the first reload made.
                //
                // What manual reloading gives up: segments published by a background merge
                // become visible at the next commit rather than within 500ms. Nothing reads
                // stale data — the live searcher keeps its own (pre-merge) segments
                // referenced, and merges are triggered by commits anyway.
                //
                // Also deliberately no tantivy `Warmer`: warmers run synchronously inside
                // reload(), which happens on the shard writer thread, so one would put
                // warming on the write hot path — and cost another thread per open index.
                // Warming is driven explicitly by `warm_index` instead.
                let reader = tantivy_index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?;

                Ok::<IndexReader, StoreError>(reader)
            })?;

        // Warm up fields cache
        let tantivy_index = reader.value().searcher().index().clone();
        let fields = self.get_fields_for_index(index, &tantivy_index)?;

        Ok(Some((reader.value().clone(), fields)))
    }

    /// Parse a query against an index without running it, and report what the parser found.
    ///
    /// This exists because the only way to learn whether a query parses used to be to run it and
    /// read what a search discarded. Checking that quotes and parentheses balance is not the same
    /// question: the interesting failure is a query that balances fine and still does not parse,
    /// and resolving a field name needs the index, so nothing above the engine can answer it.
    ///
    /// Parses through exactly the path a search takes — same normalization, same default field
    /// set, same lenient parser — so a query this accepts is one a search will run, and the
    /// clauses it reports as discarded are the clauses that search would drop.
    ///
    /// `Ok(None)` when the index has no Tantivy directory yet: an index that has a schema but has
    /// never been written to has nothing to resolve field names against. That is not an error,
    /// and it is the same distinction [`HybridStore::warm_index`] draws.
    pub fn validate_query(
        &self,
        index: &str,
        query: &str,
    ) -> Result<Option<QueryValidation>, StoreError> {
        let Some((reader, fields)) = self.get_reader(index)? else {
            return Ok(None);
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // An exact id or shadow-field lookup never reaches the parser on the search path —
        // `parse_exact_id_query` answers it from the key-value store — so validating it must
        // not report the identifier's field as a discarded clause.
        if parse_exact_id_query(query, &schema).is_some() {
            return Ok(Some(QueryValidation {
                normalized_query: query.to_string(),
                syntax_errors: Vec::new(),
                discarded: Vec::new(),
            }));
        }

        let (normalized_query, prefix_notes, query_parser) =
            prepare_query_parser(tantivy_index, &fields, &schema, query);

        // The query itself is discarded: what is wanted is the error list, which is the half a
        // search throws away after deciding it can still run.
        let (_parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);

        let syntax_errors: Vec<String> = parse_errors
            .iter()
            .filter(|err| !is_recovered_ambiguity(err))
            .filter_map(|err| match err {
                tantivy::query::QueryParserError::SyntaxError(detail) => Some(detail.clone()),
                _ => None,
            })
            .collect();

        // Syntax errors are reported above with the parser's own wording and position, so they
        // are kept out of the discarded list rather than appearing twice in weaker words.
        let semantic_errors: Vec<tantivy::query::QueryParserError> = parse_errors
            .into_iter()
            .filter(|err| !matches!(err, tantivy::query::QueryParserError::SyntaxError(_)))
            .collect();

        let mut discarded = describe_discarded_all(&semantic_errors, query, &schema);
        discarded.extend(prefix_notes);

        Ok(Some(QueryValidation {
            normalized_query,
            syntax_errors,
            discarded,
        }))
    }

    /// Search documents in a specific index
    /// Uses tantivy for search, then batch-retrieves complete documents from redb
    /// Returns (results, total_hits) where total_hits is the total number of matching documents
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        limit: usize,
        _sort: Option<&SortSpec>,
    ) -> Result<SearchOutcome, StoreError> {
        // Get reader and field mapping from cache or disk
        let (reader, fields) = match self.get_reader(index)? {
            Some(r) => r,
            None => {
                // Normal for an index with no commits yet, and emitted once per shard per
                // search — four lines for one query against an empty index at `warn`.
                debug!(index = %index, "No tantivy reader found for index");
                return Ok(SearchOutcome::empty());
            }
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();

        // Get cached schema to determine which fields are indexed
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Count-only mode: limit=0 means return just total_hits without document data.
        // Runs only the Count collector (cheaper than TopDocs) and skips all redb lookups.
        if limit == 0 {
            // For exact ID queries, we can short-circuit: total_hits is 0 or 1
            if let Some((id_value, _)) = parse_exact_id_query(query, &schema) {
                let exists = self.get_batch_by_keys(index, &[id_value])?.len();
                let total_hits = if exists > 0 { 1 } else { 0 };
                // No parse happens on this path, so nothing can be discarded.
                return Ok(SearchOutcome::counted(total_hits, Vec::new(), false));
            }

            let (normalized_query, prefix_notes, query_parser) =
                prepare_query_parser(tantivy_index, &fields, &schema, query);
            let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);
            let mut discarded = describe_discarded_all(&parse_errors, query, &schema);
            discarded.extend(prefix_notes);

            let emptied = !discarded.is_empty() && nothing_survived(parsed_query.as_ref());

            if !discarded.is_empty() {
                if emptied {
                    warn!(
                        index = %index,
                        query = %normalized_query,
                        discarded = ?discarded,
                        "Count-only: every clause was discarded; nothing was left to run and the count is zero"
                    );
                } else {
                    warn!(
                        index = %index,
                        query = %normalized_query,
                        discarded = ?discarded,
                        "Count-only: query parser discarded clauses; the count does not answer the query as written"
                    );
                }
            }

            let count_collector = tantivy::collector::Count;
            let total_hits = searcher.search(&parsed_query, &count_collector)?;

            debug!(
                index = %index,
                total_hits = total_hits,
                "Count-only search completed (limit=0)"
            );

            return Ok(SearchOutcome::counted(total_hits, discarded, emptied));
        }

        // Check if this is an exact ID lookup (id:field or shadow field) that can bypass Tantivy
        if let Some((id_value, _is_exact_id_query)) = parse_exact_id_query(query, &schema) {
            debug!(
                index = %index,
                id_value = %id_value,
                "Exact ID query detected, bypassing Tantivy search"
            );

            // Simulate Tantivy result with score 1.0
            let doc_ids_with_scores = vec![(1.0, id_value)];

            // Skip to Step 2: Batch retrieve from redb (reuse existing logic)
            let doc_ids: Vec<String> = doc_ids_with_scores
                .iter()
                .map(|(_, id)| id.clone())
                .collect();

            let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

            debug!(
                index = %index,
                requested_ids = doc_ids.len(),
                retrieved_docs = redb_docs.len(),
                "Retrieved documents from redb (direct ID lookup)"
            );

            // Create lookup map for O(1) access
            let doc_map: std::collections::HashMap<String, Vec<u8>> =
                redb_docs.into_iter().collect();

            // Step 3: Combine scores with complete documents (reuse existing logic)
            let mut results = Vec::new();
            for (score, doc_id) in doc_ids_with_scores {
                if let Some(doc_bytes) = doc_map.get(&doc_id) {
                    // Deserialize complete document from redb
                    let stored_doc: StoredDocOwned = serde_json::from_slice(doc_bytes)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    // Get schema for shadow field reconstruction
                    let schema = if let Some(schema) = self.get_schema_cached(index)? {
                        schema
                    } else {
                        self.get_schema(index)?
                            .map(Arc::new)
                            .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                    };

                    // OPTIMIZATION: Pass ownership to avoid cloning all fields
                    let final_doc = if let Some(json_blob) = stored_doc.json_blob {
                        reconstruct_shadow_fields_owned(json_blob, &schema, &doc_id)
                    } else {
                        // Fallback if blob was empty
                        serde_json::json!({ "id": doc_id })
                    };

                    results.push((score, final_doc));
                } else {
                    trace!(index = %index, doc_id = %doc_id, "Document not found in redb lookup map");
                }
            }

            let total_hits = if results.is_empty() { 0 } else { 1 };
            // No parse happens on this path, and no sort: an id lookup returns the one document
            // it names. So nothing can be discarded and no order can be approximate.
            return Ok(SearchOutcome {
                hits: results,
                total_hits,
                discarded: Vec::new(),
                approximate_sort: None,
                emptied: false,
            });
        }

        let (normalized_query, prefix_notes, query_parser) =
            prepare_query_parser(tantivy_index, &fields, &schema, query);

        // Lenient, so one bad clause does not fail the whole query; what it drops is reported
        // through `SearchOutcome::discarded` rather than swallowed.
        let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);
        let mut discarded = describe_discarded_all(&parse_errors, query, &schema);
        discarded.extend(prefix_notes);

        let emptied = !discarded.is_empty() && nothing_survived(parsed_query.as_ref());

        if !discarded.is_empty() {
            // A dropped clause does not move the result set one way: it widens a conjunction,
            // narrows a disjunction, and empties a query that had nothing else to run. Only the
            // last is knowable from here, and it is the one worth separating — the zero an
            // emptied query reports is not a negative answer, it is no answer at all.
            if emptied {
                warn!(
                    index = %index,
                    query = %normalized_query,
                    discarded = ?discarded,
                    "Every clause was discarded; nothing was left to run and the result set is empty"
                );
            } else {
                warn!(
                    index = %index,
                    query = %normalized_query,
                    discarded = ?discarded,
                    "Query parser discarded clauses; the results do not answer the query as written"
                );
            }
        }

        // Flag set when sorting by a string field (post-fetch alphabetic sort).
        // The field name and order are captured here and used after redb retrieval.
        let mut string_sort: Option<(String, SortOrder)> = None;

        let (top_docs, total_hits) = if let Some(sort_spec) = _sort {
            // A sort names one value under two names, and on an index with a shadow field they
            // are not the same name.
            //
            // A shadow field *is* the document key under the source's own name: the query path
            // maps it to `id` (`rewrite_shadow_fields`), so a sort maps the same way or the two
            // disagree about what the caller's name means. That gives the column to order on.
            // But the value the caller reads back is not under that name — shadow
            // reconstruction *replaces* `id` with the shadow field on the way out (see
            // `reconstruct_shadow_fields_owned`), so the post-fetch sort below, and every merge
            // above this one, has to look for it under the name the document actually carries.
            let sorts_by_document_key =
                sort_spec.field == "id" || schema.is_shadow_field(&sort_spec.field);
            let column_name: &str = if sorts_by_document_key {
                "id"
            } else {
                &sort_spec.field
            };
            let document_name: String = if sorts_by_document_key {
                document_key_field(&schema)
            } else {
                sort_spec.field.clone()
            };

            // Get field from schema to check type and FAST flag
            let schema = tantivy_index.schema();

            // `_seq` is FAST, so it would otherwise satisfy every check below and sort — but
            // only within one shard. Document bodies are served from redb, which has no `_seq`
            // key, so nothing is stamped for a scatter-gather merge to order by: the caller
            // gets a partial ordering and no error. Every field listing already hides `_seq`,
            // so refusing it here is what makes the engine agree with what `sortable_fields`
            // advertises.
            if sort_spec.field == "_seq" {
                return Err(StoreError::FieldNotFound(sort_spec.field.clone()));
            }

            let field = schema
                .get_field(column_name)
                .map_err(|_| StoreError::FieldNotFound(sort_spec.field.clone()))?;

            let field_entry = schema.get_field_entry(field);

            // Text/String fields don't require the FAST flag — they use a post-fetch sort.
            let is_str_field =
                matches!(field_entry.field_type(), tantivy::schema::FieldType::Str(_));

            if !field_entry.is_fast() && !is_str_field {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Field '{}' is not marked as FAST. Only FAST fields support sorting.",
                        sort_spec.field
                    ),
                )));
            }

            let order = match sort_spec.order {
                SortOrder::Asc => tantivy::Order::Asc,
                SortOrder::Desc => tantivy::Order::Desc,
            };

            // Run a TopDocs sort ordered by a FAST field of the given type. The generic
            // type MUST match the field's actual type — `order_by_fast_field::<u64>` on an
            // i64/f64/date field returns a Tantivy SchemaError at collection time.
            // The sort key itself is discarded downstream (ordering is encoded in the Vec
            // sequence), so all branches normalize to `(None, doc_address)`.
            macro_rules! collect_sorted {
                ($t:ty) => {{
                    let top_docs_collector = tantivy::collector::TopDocs::with_limit(limit)
                        .order_by_fast_field::<$t>(column_name, order);
                    let count_collector = tantivy::collector::Count;

                    // Use MultiCollector to get both results and count in single query execution
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let sorted: Vec<(Option<$t>, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);

                    let docs: Vec<(Option<String>, tantivy::DocAddress)> =
                        sorted.into_iter().map(|(_, addr)| (None, addr)).collect();
                    (SearchResult::Sorted(docs), total_hits)
                }};
            }

            // u64, i64, f64 and Date sort on their FAST column; text sorts on its own when it
            // has one, and falls back to a post-fetch sort of scored candidates when it does not.
            match field_entry.field_type() {
                tantivy::schema::FieldType::U64(_) => collect_sorted!(u64),
                tantivy::schema::FieldType::I64(_) => collect_sorted!(i64),
                tantivy::schema::FieldType::F64(_) => collect_sorted!(f64),
                tantivy::schema::FieldType::Date(_) => collect_sorted!(tantivy::DateTime),
                // A text field with a fast column is sorted by the column, in the collector,
                // like any other sortable type. Tantivy keys this on the term ordinal, which
                // its term dictionary holds in lexicographic order, so the result is a true
                // alphabetical total order over *every* match rather than over a sample —
                // which is what makes a deep page of a text sort mean anything.
                tantivy::schema::FieldType::Str(_) if field_entry.is_fast() => {
                    let top_docs_collector = tantivy::collector::TopDocs::with_limit(limit)
                        .order_by_string_fast_field(column_name, order);
                    let count_collector = tantivy::collector::Count;
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let sorted: Vec<(Option<String>, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);

                    // Ordering is carried by the sequence from here on, as it is for the
                    // numeric branches. The key survives only when the sort named the
                    // document key — then the column ordered on is `id` and each key is
                    // the hit's identifier; any other string column's key is the field's
                    // value, not an id, and is dropped as before.
                    let docs: Vec<(Option<String>, tantivy::DocAddress)> = sorted
                        .into_iter()
                        .map(|(key, addr)| (if sorts_by_document_key { key } else { None }, addr))
                        .collect();
                    (SearchResult::Sorted(docs), total_hits)
                }
                tantivy::schema::FieldType::Str(_) => {
                    // No fast column on this field, so there is nothing to order on in the
                    // collector: candidates are taken by relevance and sorted alphabetically
                    // after the redb fetch. The result is the alphabetical order *of the
                    // highest-scoring `limit * 2`*, not of everything that matched — the
                    // alphabetically first document does not score its way in unless the query
                    // happens to favour it.
                    //
                    // Declaring the field `fast` takes the branch above instead and removes the
                    // approximation. The column is written at index time, so that declaration
                    // has to be in place before the data is: on an index that already exists
                    // there is no way to add one, and no reindex to add it with (ROADMAP
                    // Phase 15). `sortable` on the field's description reports which case an
                    // index is in.
                    //
                    // `debug`, not `warn`: this fires once per shard per search, and the caller
                    // is told in the response itself through `approximate_sort` — which is where
                    // it can be acted on. A log line per query would be noise in front of an
                    // operator who cannot fix it from there anyway.
                    debug!(
                        index = %index,
                        field = %sort_spec.field,
                        "sorting on a text field without a fast column; the order returned is \
                         the alphabetical order of the top-scoring candidates, not of all matches"
                    );
                    let budget = limit.saturating_mul(2);
                    string_sort = Some((document_name, sort_spec.order));

                    let top_docs_collector =
                        tantivy::collector::TopDocs::with_limit(budget).order_by_score();
                    let count_collector = tantivy::collector::Count;
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let top_docs: Vec<(f32, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);
                    (SearchResult::Unsorted(top_docs), total_hits)
                }
                _ => {
                    return Err(StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Field '{}' type {:?} is not sortable. Supported types: u64, i64, f64, date (FAST), text, string.",
                            sort_spec.field,
                            field_entry.field_type()
                        ),
                    )));
                }
            }
        } else {
            // Default: sort by relevance score using MultiCollector
            let top_docs_collector =
                tantivy::collector::TopDocs::with_limit(limit).order_by_score();
            let count_collector = tantivy::collector::Count;
            let mut multi_collector = tantivy::collector::MultiCollector::new();
            let top_docs_handle = multi_collector.add_collector(top_docs_collector);
            let count_handle = multi_collector.add_collector(count_collector);

            let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
            let top_docs: Vec<(f32, tantivy::DocAddress)> =
                top_docs_handle.extract(&mut multi_fruit);
            let total_hits = count_handle.extract(&mut multi_fruit);

            (SearchResult::Unsorted(top_docs), total_hits)
        };

        debug!(
            index = %index,
            hits_returned = match &top_docs {
                SearchResult::Sorted(docs) => docs.len(),
                SearchResult::Unsorted(docs) => docs.len(),
            },
            total_hits = total_hits,
            "Tantivy search completed"
        );

        let is_empty = match &top_docs {
            SearchResult::Sorted(docs) => docs.is_empty(),
            SearchResult::Unsorted(docs) => docs.is_empty(),
        };

        if is_empty {
            return Ok(SearchOutcome::counted(total_hits, discarded, emptied));
        }

        // Step 1: Extract document IDs from Tantivy results. `id` carries a fast column on
        // indexes built since it gained one, but a per-hit `ord_to_str` re-opens the term
        // block and scans it from the top — slower than a stored-document read on a warm
        // block cache. The column pays off only as a batch: collect a segment's hit ords,
        // sort them, and let `sorted_ords_to_term_cb` walk the dictionary once. That is
        // worth doing only when a segment contributes enough hits packed densely enough —
        // a handful of hits spread over the term range would scan most of the dictionary
        // to answer a few ids, so sparse hits keep the stored-document read, which is also
        // the fallback for a hit the column does not answer. Whether this index has the
        // column at all is a question its own schema answers — the declared `fast` is the
        // builder's intent, not what an older index was built with — so a legacy index
        // takes the stored-document path it always used.
        const DENSE_HIT_MIN: usize = 32;
        const DENSE_HIT_SPAN: u64 = 4;

        let capacity = match &top_docs {
            SearchResult::Sorted(docs) => docs.len(),
            SearchResult::Unsorted(docs) => docs.len(),
        };
        let mut doc_ids_with_scores = Vec::with_capacity(capacity);

        // A document-key sort's keys are the ids themselves — they arrive with the hits and
        // answer every position outright; every other hit starts unresolved.
        let mut resolved: Vec<Option<String>> = Vec::with_capacity(capacity);
        let hits: Vec<(f32, tantivy::DocAddress)> = match top_docs {
            // For sorted results the order is what matters, not the key — a 1.0 placeholder
            // score, exactly as before.
            SearchResult::Sorted(docs) => docs
                .into_iter()
                .map(|(id, addr)| {
                    resolved.push(id);
                    (1.0, addr)
                })
                .collect(),
            SearchResult::Unsorted(docs) => docs
                .into_iter()
                .map(|(score, addr)| {
                    resolved.push(None);
                    (score, addr)
                })
                .collect(),
        };

        let id_is_fast = tantivy_index.schema().get_field_entry(fields.id).is_fast();
        if id_is_fast && hits.len() >= DENSE_HIT_MIN {
            let mut by_segment: Vec<Vec<usize>> = (0..searcher.segment_readers().len())
                .map(|_| Vec::new())
                .collect();
            for (pos, (_, doc_address)) in hits.iter().enumerate() {
                if resolved[pos].is_none() {
                    by_segment[doc_address.segment_ord as usize].push(pos);
                }
            }
            for (segment_ord, positions) in by_segment.iter().enumerate() {
                if positions.len() < DENSE_HIT_MIN {
                    continue;
                }
                let Some(column) = searcher.segment_readers()[segment_ord]
                    .fast_fields()
                    .str("id")
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                let mut ord_pos: Vec<(u64, usize)> = positions
                    .iter()
                    .filter_map(|&pos| {
                        column
                            .term_ords(hits[pos].1.doc_id)
                            .next()
                            .map(|ord| (ord, pos))
                    })
                    .collect();
                ord_pos.sort_unstable_by_key(|&(ord, _)| ord);
                let (Some(&(first_ord, _)), Some(&(last_ord, _))) =
                    (ord_pos.first(), ord_pos.last())
                else {
                    continue;
                };
                if last_ord - first_ord + 1 > DENSE_HIT_SPAN * ord_pos.len() as u64 {
                    continue;
                }
                // One walk over the hit ords; the callback fires once per ord in order. A
                // failed or truncated walk leaves positions unresolved, and the stored
                // document answers them below.
                let mut positions_it = ord_pos.iter().map(|&(_, pos)| pos);
                let _ = column.dictionary().sorted_ords_to_term_cb(
                    ord_pos.iter().map(|&(ord, _)| ord),
                    |term| {
                        if let Some(pos) = positions_it.next() {
                            resolved[pos] = std::str::from_utf8(term).ok().map(str::to_string);
                        }
                        Ok(())
                    },
                );
            }
        }

        for (pos, (score, doc_address)) in hits.into_iter().enumerate() {
            let id_str = match resolved[pos].take() {
                Some(id) => Some(id),
                None => {
                    let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
                    doc.get_first(fields.id)
                        .and_then(|value| value.as_str())
                        .map(str::to_string)
                }
            };
            match id_str {
                Some(id_str) => {
                    debug!(
                        index = %index,
                        doc_id = %id_str,
                        doc_addr = ?doc_address,
                        "Tantivy document matched"
                    );
                    doc_ids_with_scores.push((score, id_str));
                }
                None => {
                    // The stored document answered nothing — fetch it once more only to say
                    // what it was. Corrupt-state logging, not a hot path.
                    let tantivy_doc = searcher
                        .doc(doc_address)
                        .map(|doc: tantivy::TantivyDocument| doc.to_json(&tantivy_index.schema()))
                        .unwrap_or_else(|_| "<unreadable>".to_string());
                    warn!(
                        index = %index,
                        doc_addr = ?doc_address,
                        tantivy_doc = %tantivy_doc,
                        "Tantivy document missing or invalid 'id' field"
                    );
                }
            }
        }

        debug!(
            index = %index,
            ids_extracted = doc_ids_with_scores.len(),
            "Extracted document IDs from tantivy results"
        );

        // Step 2: Batch retrieve complete documents from redb (single transaction)
        let doc_ids: Vec<String> = doc_ids_with_scores
            .iter()
            .map(|(_, id)| id.clone())
            .collect();

        let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

        debug!(
            index = %index,
            requested_ids = doc_ids.len(),
            retrieved_docs = redb_docs.len(),
            "Retrieved documents from redb"
        );

        // Create lookup map for O(1) access
        let doc_map: std::collections::HashMap<String, Vec<u8>> = redb_docs.into_iter().collect();

        // Step 3: Combine scores with complete documents
        let mut results = Vec::new();
        for (score, doc_id) in doc_ids_with_scores {
            if let Some(doc_bytes) = doc_map.get(&doc_id) {
                // Deserialize complete document from redb
                let stored_doc: StoredDocOwned = serde_json::from_slice(doc_bytes)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                // Get schema for shadow field reconstruction
                let schema = if let Some(schema) = self.get_schema_cached(index)? {
                    schema
                } else {
                    self.get_schema(index)?
                        .map(Arc::new)
                        .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                };

                // OPTIMIZATION: Pass ownership to avoid cloning all fields
                let final_doc = if let Some(json_blob) = stored_doc.json_blob {
                    reconstruct_shadow_fields_owned(json_blob, &schema, &doc_id)
                } else {
                    // Fallback if blob was empty
                    serde_json::json!({ "id": doc_id })
                };

                results.push((score, final_doc));
            } else {
                trace!(index = %index, doc_id = %doc_id, "Document not found in redb lookup map");
            }
        }

        // Post-fetch alphabetic sort for string fields.
        // Candidates were collected with budget = limit*2; sort and truncate to limit.
        //
        // Documents arrive in Tantivy's order, which is total — it breaks its own ties on
        // document address — so a value repeated across documents falls back to that order
        // rather than to whatever the comparison happened to leave in place. Stated as a
        // comparison on the original position instead of relying on the sort being stable: a
        // later switch to `sort_unstable_by` would otherwise make this shard's answer vary
        // between runs, and every merge above it inherits that.
        // Read before the sort below consumes it: an approximate order is a property of the
        // answer, and the caller has to be able to see it on the answer.
        //
        // Named as the *documents* name it, not as the request did. The two differ only when
        // the sort is on the document key of a shadow index, where a caller may say `id` and
        // every hit comes back carrying the shadow name instead — reporting `id` there names a
        // field absent from every hit in the same response, which is the one thing a caller
        // cannot check the order against.
        let approximate_sort = string_sort.as_ref().map(|(name, _)| name.clone());

        if let Some((field, order)) = string_sort {
            let mut ranked: Vec<(usize, _)> = std::mem::take(&mut results)
                .into_iter()
                .enumerate()
                .collect();
            ranked.sort_by(|(left_position, a), (right_position, b)| {
                let av = a.1.get(&field).and_then(|v| v.as_str());
                let bv = b.1.get(&field).and_then(|v| v.as_str());
                let base = match (av, bv) {
                    (Some(ax), Some(bx)) => ax.cmp(bx),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let base = match order {
                    SortOrder::Asc => base,
                    SortOrder::Desc => base.reverse(),
                };
                base.then_with(|| left_position.cmp(right_position))
            });
            results = ranked.into_iter().map(|(_, hit)| hit).collect();
            results.truncate(limit);
        }

        Ok(SearchOutcome {
            hits: results,
            total_hits,
            discarded,
            approximate_sort,
            emptied,
        })
    }

    /// Get adaptive sample count based on table size.
    /// Larger tables get more samples for better statistical accuracy,
    /// while maintaining O(1) fixed cost (not O(N)).
    pub(crate) fn get_adaptive_sample_count(table_count: u64) -> u64 {
        match table_count {
            0..=200 => table_count,     // Exact for tiny tables
            201..=10_000 => 200,        // 200 samples for small tables
            10_001..=100_000 => 300,    // 300 samples for medium tables
            100_001..=1_000_000 => 400, // 400 samples for large tables
            _ => 500,                   // 500 samples for huge tables (millions+)
        }
    }

    /// Calculate table size using Hybrid Exact/Sampling Estimation algorithm
    ///
    /// Uses adaptive sampling: larger tables get more samples for better accuracy.
    /// - Tiny tables (≤200): Exact calculation by iterating all records
    /// - Small tables (≤10K): 200 samples
    /// - Medium tables (≤100K): 300 samples
    /// - Large tables (≤1M): 400 samples
    /// - Huge tables (>1M): 500 samples
    ///
    /// Returns (raw_size, is_estimated) where raw_size is the calculated/estimated size
    /// and is_estimated indicates whether sampling was used
    pub(crate) fn calculate_table_size_estimated(
        &self,
        table: &redb::ReadOnlyTable<&str, &[u8]>,
    ) -> Result<(u64, bool), StoreError> {
        let count = table.len()?;
        let sample_count = Self::get_adaptive_sample_count(count);

        if count <= sample_count {
            // Exact calculation for small tables
            let mut total_size = 0u64;
            for result in table.iter()? {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                total_size += key.value().len() as u64 + value.value().len() as u64;
            }
            Ok((total_size, false))
        } else {
            // Sample-based estimation for large tables
            let mut sample_size = 0u64;
            let mut actual_samples = 0u64;

            for result in table.iter()?.take(sample_count as usize) {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                sample_size += key.value().len() as u64 + value.value().len() as u64;
                actual_samples += 1;
            }

            let average_row_size = if actual_samples > 0 {
                sample_size as f64 / actual_samples as f64
            } else {
                0.0
            };

            let estimated_raw_size = (average_row_size * count as f64) as u64;

            tracing::trace!(
                table_count = count,
                sample_count = actual_samples,
                avg_row_size = average_row_size as u64,
                estimated_size = estimated_raw_size,
                "Adaptive sampling used for table size estimation"
            );

            Ok((estimated_raw_size, true))
        }
    }

    /// Gather per-index statistics and timing information for this shard.
    ///
    /// PERFORMANCE: This function now uses batch measurement to avoid N² complexity.
    /// All indexes are measured once in a single pass, reusing a single transaction.
    pub fn gather_index_stats(
        &self,
        include_data_size: bool,
    ) -> Result<ShardStatsSnapshot, StoreError> {
        let mut per_index = HashMap::new();

        let mut index_names: HashSet<String> = HashSet::new();
        let redb_phase_start = Instant::now();
        let read_txn = self.kv.begin_read()?;

        if let Ok(schema_table) = read_txn.open_table(TABLE_SCHEMA) {
            for result in schema_table.iter()? {
                let (index_name, bytes) = result?;
                if schema_records_a_deletion(bytes.value()) {
                    continue;
                }
                index_names.insert(index_name.value().to_string());
            }
        }

        let indices_dir = self.config.shard_path.join("indices");
        if indices_dir.exists() {
            for entry in fs::read_dir(&indices_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    index_names.insert(entry.file_name().to_string_lossy().to_string());
                }
            }
        }

        // Batch measure all indexes once (eliminates N² pattern)
        let all_sizes =
            self.batch_measure_all_indexes(&index_names, &read_txn, include_data_size)?;

        // Build result from batch measurements
        for index_name in &index_names {
            if let Some(sizes) = all_sizes.get(index_name) {
                // Check if Tantivy index directory exists (not just if it has size)
                // This ensures empty indexes (after schema creation) are counted
                let tantivy_index_exists = self
                    .index_dir(index_name)
                    .map(|p| p.join("meta.json").exists())
                    .unwrap_or(false);

                per_index.insert(
                    index_name.clone(),
                    IndexShardStats {
                        document_count: sizes.document_count,
                        redb_bytes: sizes.redb_bytes,
                        tantivy_bytes: sizes.tantivy_bytes,
                        tantivy_index_exists,
                        tantivy_scan_ms: 0,
                        warmup_state: self
                            .warmup_states
                            .get(index_name)
                            .map(|state| *state.value())
                            .unwrap_or(IndexWarmupState::Cold),
                        searchable_fields: self.searchable_fields(index_name),
                        sortable_fields: self.sortable_fields(index_name),
                    },
                );
            }
        }
        let redb_duration = redb_phase_start.elapsed();
        drop(read_txn);

        Ok(ShardStatsSnapshot {
            per_index,
            timings: ShardStatsTimings {
                redb_ms: redb_duration.as_millis(),
                tantivy_ms: 0, // Included in redb calculation now
                total_ms: redb_duration.as_millis(),
            },
        })
    }

    /// Field names the built Tantivy index has a column for.
    ///
    /// Answers "can a query reach this field *now*", which the schema alone cannot: `indexed`
    /// there is a declaration, and a field declared after the index was built has no column until
    /// the data is rebuilt. See [`IndexShardStats::searchable_fields`].
    ///
    /// Free when the index has been touched — the field handles are already cached — and one
    /// `meta.json` read when it has not. Returns empty rather than erroring for an index with no
    /// built directory, since that is a normal state and not a failure to report.
    /// Both paths report the same set: every column except `_seq`, which is WAL bookkeeping and
    /// not something a caller queries. `id` *is* included — `id:value` is answerable, and is in
    /// fact the one lookup served without touching the search index at all — even though
    /// [`SchemaFields::indexed_fields`] omits it, since that map exists to drive document
    /// building where `id` is handled separately.
    pub fn searchable_fields(&self, index: &str) -> HashSet<String> {
        if let Some(fields) = self.fields_cache.get(index) {
            let mut names: HashSet<String> = fields.indexed_fields.keys().cloned().collect();
            names.insert("id".to_string());
            return names;
        }

        let Ok(index_path) = self.index_dir(index) else {
            return HashSet::new();
        };
        if !index_path.join("meta.json").exists() {
            return HashSet::new();
        }

        // Opening reads `meta.json` for the schema; it does not take the writer lockfile, so this
        // is safe against a live writer.
        match open_tantivy_index(&index_path) {
            Ok(opened) => opened
                .schema()
                .fields()
                .map(|(_, entry)| entry.name().to_string())
                .filter(|name| name != "_seq")
                .collect(),
            Err(err) => {
                tracing::debug!(index = %index, error = %err, "Could not read searchable fields");
                HashSet::new()
            }
        }
    }

    /// Field names the built Tantivy index has a *fast column* for, and can therefore sort
    /// exactly.
    ///
    /// The same distinction [`Self::searchable_fields`] draws, one property along: `fast` in the
    /// schema is a declaration, the column is written at index time from that declaration, and
    /// the two part company for any field declared after the index was built. A caller that
    /// reads the declaration and sorts on it gets an answer — an approximate one for a text
    /// field, an error for a numeric one — so the declaration alone cannot answer "will a sort
    /// on this field be exact".
    ///
    /// Read from the built index rather than from the cached field handles, because
    /// [`SchemaFields`] records which fields exist and not which carry a column. Empty for an
    /// index with no built directory, as above.
    pub fn sortable_fields(&self, index: &str) -> HashSet<String> {
        let Ok(index_path) = self.index_dir(index) else {
            return HashSet::new();
        };
        if !index_path.join("meta.json").exists() {
            return HashSet::new();
        }

        match open_tantivy_index(&index_path) {
            Ok(opened) => opened
                .schema()
                .fields()
                .filter(|(_, entry)| entry.is_fast())
                .map(|(_, entry)| entry.name().to_string())
                .filter(|name| name != "_seq")
                .collect(),
            Err(err) => {
                tracing::debug!(index = %index, error = %err, "Could not read sortable fields");
                HashSet::new()
            }
        }
    }

    /// Get list of index names from redb schema table only
    pub fn get_index_names(&self) -> Result<Vec<String>, StoreError> {
        let mut index_names = Vec::new();

        let read_txn = self.kv.begin_read()?;

        // Only check redb schema table - no filesystem access, no Tantivy loading
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => {
                for result in schema_table.iter()? {
                    let (index_name, bytes) = result?;
                    if schema_records_a_deletion(bytes.value()) {
                        continue;
                    }
                    index_names.push(index_name.value().to_string());
                }
            }
            Err(_) => {
                // Schema table doesn't exist yet - return empty list
            }
        }

        Ok(index_names)
    }

    pub(crate) fn measure_tantivy_bytes(&self, index_name: &str) -> Result<u64, StoreError> {
        let index_dir = self.index_dir(index_name)?;
        if !index_dir.exists() {
            return Ok(0);
        }

        let mut total_size = 0u64;
        for entry in WalkDir::new(&index_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            if let Ok(metadata) = entry.metadata()
                && metadata.is_file()
                && Self::is_tantivy_data_file(entry.path())
            {
                total_size += metadata.len();
            }
        }

        Ok(total_size)
    }

    /// Measure redb stats using an existing transaction (avoids opening new transaction).
    /// This is more efficient when measuring multiple indexes.
    pub(crate) fn measure_redb_stats_with_txn(
        &self,
        index_name: &str,
        read_txn: &redb::ReadTransaction,
    ) -> Result<(u64, u64), StoreError> {
        let data_table_name = format!("data_{}", index_name);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let (doc_count, raw_bytes) = match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                let doc_count = data_table.len().unwrap_or(0);
                let (raw_size, _) = self.calculate_table_size_estimated(&data_table)?;
                (doc_count, raw_size)
            }
            Err(_) => (0, 0),
        };

        Ok((doc_count, raw_bytes))
    }

    /// Get document count from Tantivy index (O(1) operation).
    /// This is faster than querying redb when we don't need size calculation.
    pub(crate) fn get_document_count_from_tantivy(
        &self,
        index_name: &str,
    ) -> Result<u64, StoreError> {
        // get_reader owns the reader cache: it serves the cached reader when there is one and
        // otherwise opens the index and caches it. The previous implementation fell back to
        // get_or_create_index, which only populates the *writer* cache, so the follow-up
        // reader lookup always missed and this reported 0 documents for any index that had
        // not been searched yet.
        match self.get_reader(index_name) {
            Ok(Some((reader, _fields))) => Ok(reader.searcher().num_docs()),
            Ok(None) => Ok(0), // Index has no Tantivy directory yet
            Err(e) => {
                tracing::debug!(index = %index_name, error = %e, "Failed to open reader for document count");
                Ok(0)
            }
        }
    }

    /// Drop both cached size entries (`fast` and `full`) for `index`.
    ///
    /// The tuple key is what makes this exact: the cache's keys used to be the formatted
    /// string `{shard}:{fast|full}:{index}`, and invalidation was a `contains(":{index}")`
    /// match — evicting `"a"` also evicted `"ab"`, `"aa"` and every other index name holding
    /// the substring, silently. Deleting or committing one index costing its neighbours their
    /// cached sizes is invisible to tests because an over-eager cache is still correct, only
    /// slower. Exact keys mean only this index's entries go.
    pub(crate) fn invalidate_size_cache(&self, index: &str) {
        let mut size_cache = self
            .index_size_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        size_cache.remove(&(false, index.to_string()));
        size_cache.remove(&(true, index.to_string()));
    }

    /// Batch measure all indexes in a single pass with shared transaction.
    /// This eliminates the N² complexity of the old approach where get_index_sizes_cached
    /// was called once per index, and each call measured ALL indexes.
    ///
    /// Returns a HashMap of index_name -> IndexSizes for all indexes.
    pub(crate) fn batch_measure_all_indexes(
        &self,
        index_names: &HashSet<String>,
        read_txn: &redb::ReadTransaction,
        include_data_size: bool,
    ) -> Result<HashMap<String, IndexSizes>, StoreError> {
        let mut results = HashMap::new();

        // Check cache first for all indexes
        {
            let cache = self
                .index_size_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for index_name in index_names {
                if let Some(entry) = cache.get(&(include_data_size, index_name.clone()))
                    && entry.timestamp.elapsed() < self.index_cache_expiry
                {
                    results.insert(
                        index_name.clone(),
                        IndexSizes {
                            tantivy_bytes: entry.tantivy_bytes,
                            redb_bytes: entry.redb_bytes,
                            document_count: entry.document_count,
                        },
                    );
                }
            }
        }

        // If all cached, return early
        if results.len() == index_names.len() {
            tracing::debug!(
                shard = %self.config.shard_path.display(),
                cached_count = results.len(),
                "All index sizes retrieved from cache"
            );
            return Ok(results);
        }

        // Measure uncached indexes
        let mut per_index_stats = Vec::new();
        let mut total_raw_redb_size = 0u64;

        for idx_name in index_names {
            if results.contains_key(idx_name) {
                continue; // Skip cached
            }

            let tantivy_bytes = self.measure_tantivy_bytes(idx_name)?;

            let (doc_count, raw_redb_bytes) = if include_data_size {
                // When calculating data size, get count from redb for consistency
                self.measure_redb_stats_with_txn(idx_name, read_txn)?
            } else {
                // When skipping data size, use Tantivy count (faster, no redb access)
                let doc_count = self.get_document_count_from_tantivy(idx_name)?;
                (doc_count, 0)
            };

            per_index_stats.push((idx_name.clone(), tantivy_bytes, doc_count, raw_redb_bytes));
            total_raw_redb_size = total_raw_redb_size.saturating_add(raw_redb_bytes);
        }

        tracing::debug!(
            shard = %self.config.shard_path.display(),
            uncached_count = per_index_stats.len(),
            cached_count = results.len(),
            "Measured uncached indexes"
        );

        // Calculate correction factor (only when include_data_size is true)
        let correction_factor = if include_data_size && total_raw_redb_size > 0 {
            let physical_db_size =
                match std::fs::metadata(self.config.shard_path.join("store.redb")) {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to get database file size, using raw estimation"
                        );
                        total_raw_redb_size
                    }
                };
            physical_db_size as f64 / total_raw_redb_size as f64
        } else {
            1.0
        };

        // Cache and build results for uncached indexes
        // OPTIMIZATION: Populate BOTH fast and full cache entries to enable cache sharing
        {
            let mut cache = self
                .index_size_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            for (idx_name, tantivy_bytes, doc_count, raw_redb_bytes) in per_index_stats {
                let corrected_redb_bytes = if include_data_size {
                    (raw_redb_bytes as f64 * correction_factor) as u64
                } else {
                    0
                };

                // Always cache the "fast" entry (tantivy bytes + doc count, no redb size)
                cache.insert(
                    (false, idx_name.clone()),
                    IndexSizeCache {
                        tantivy_bytes,
                        redb_bytes: 0,
                        document_count: doc_count,
                        timestamp: Instant::now(),
                    },
                );

                // When we have redb data, also cache the "full" entry
                if include_data_size {
                    cache.insert(
                        (true, idx_name.clone()),
                        IndexSizeCache {
                            tantivy_bytes,
                            redb_bytes: corrected_redb_bytes,
                            document_count: doc_count,
                            timestamp: Instant::now(),
                        },
                    );
                }

                results.insert(
                    idx_name.clone(),
                    IndexSizes {
                        tantivy_bytes,
                        redb_bytes: corrected_redb_bytes,
                        document_count: doc_count,
                    },
                );
            }
        }

        Ok(results)
    }

    pub(crate) fn is_tantivy_data_file(path: &std::path::Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| TANTIVY_DATA_FILE_EXTENSIONS.contains(&ext))
            .unwrap_or(false)
    }

    /// Get field names from actual documents in an index by sampling
    pub fn get_index_field_names(&self, index: &str) -> Result<Vec<String>, StoreError> {
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;
        let mut field_names = std::collections::HashSet::new();

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                const MAX_SAMPLES: usize = 100; // Sample up to 100 documents

                for (sample_count, result) in data_table.iter()?.enumerate() {
                    if sample_count >= MAX_SAMPLES {
                        break;
                    }

                    let (_, value) = result?;

                    // Parse the document JSON to extract field names
                    if let Ok(doc_data) = serde_json::from_slice::<JsonValue>(value.value()) {
                        if let Some(json_blob) = doc_data.get("json_blob")
                            && let Some(json_obj) = json_blob.as_object()
                        {
                            for field_name in json_obj.keys() {
                                field_names.insert(field_name.clone());
                            }
                        }

                        // Also check top-level fields in the document
                        if let Some(doc_obj) = doc_data.as_object() {
                            for field_name in doc_obj.keys() {
                                if field_name != "body" && field_name != "json_blob" {
                                    field_names.insert(field_name.clone());
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Table doesn't exist, return empty list
            }
        }

        let mut field_names_vec: Vec<String> = field_names.into_iter().collect();

        // Sort fields with "id" first, then alphabetically
        field_names_vec.sort_by(|a, b| {
            match (a.as_str(), b.as_str()) {
                ("id", "id") => std::cmp::Ordering::Equal,
                ("id", _) => std::cmp::Ordering::Less, // "id" comes first
                (_, "id") => std::cmp::Ordering::Greater, // "id" comes first
                (a, b) => a.cmp(b),                    // alphabetical for others
            }
        });

        Ok(field_names_vec)
    }
}
