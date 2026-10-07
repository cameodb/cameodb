//! Unit tests for the storage engine, kept beside the modules they exercise.
use crate::search::{BUDGET_CACHE_TTL, BudgetCacheEntry};
use crate::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;
use tantivy::schema::{FAST, Schema};
use tantivy::{Index, doc};

#[cfg(test)]
mod index_dir_tests {
    use super::{StoreError, resolve_index_dir};
    use std::path::Path;

    fn base() -> &'static Path {
        Path::new("/shard/indices")
    }

    #[test]
    fn accepts_plain_names() {
        assert_eq!(
            resolve_index_dir(base(), "docs").unwrap(),
            Path::new("/shard/indices/docs")
        );
        assert_eq!(
            resolve_index_dir(base(), "my-index_2.v1").unwrap(),
            Path::new("/shard/indices/my-index_2.v1")
        );
    }

    /// The guard must hold without touching the filesystem: these paths do not
    /// exist, which is exactly the case where a canonicalize-based check would
    /// silently pass and let `create_dir_all` escape the shard.
    #[test]
    fn rejects_traversal_and_separators() {
        for name in [
            "..",
            ".",
            "../etc",
            "../../etc/passwd",
            "a/b",
            "a/../../b",
            "/etc",
            "/etc/passwd",
            "",
            "./x",
        ] {
            let err =
                resolve_index_dir(base(), name).expect_err(&format!("'{}' must be rejected", name));
            assert!(
                matches!(err, StoreError::InvalidIndexName(_)),
                "'{}' produced the wrong error: {:?}",
                name,
                err
            );
        }
    }

    /// A rejected name must never yield a path outside the base, and an accepted
    /// one must always stay directly beneath it.
    #[test]
    fn accepted_names_stay_within_base() {
        for name in ["docs", "a.b", "x-1"] {
            let path = resolve_index_dir(base(), name).unwrap();
            assert_eq!(path.parent(), Some(base()));
            assert!(path.starts_with(base()));
        }
    }
}

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// An id-only WAL entry round-trips, whatever the id looks like.
    ///
    /// The empty id and one beginning with `{` are the two that could collide with the legacy
    /// format if the tag byte were ever dropped in favour of sniffing the payload.
    #[test]
    fn a_wal_entry_round_trips_any_document_id() {
        for id in ["doc-1", "", "{not-json", "ünïcodé", "a b\tc", "\u{1F600}"] {
            let encoded = encode_wal_entry(id);
            assert_eq!(encoded[0], WAL_ENTRY_ID_ONLY, "entry must carry its tag");
            assert_eq!(
                decode_wal_entry(&encoded).expect("decode"),
                id,
                "id must survive the round trip"
            );
        }
    }

    /// A checkpoint scan that finds a sortable `_seq` but cannot read its stored value fails,
    /// rather than inventing a number.
    ///
    /// `get_highest_indexed_seq` orders on the `_seq` fast field but reads the answer from the
    /// document's stored fields, because the sort key is `u64::MAX` minus it. A document that is
    /// fast-only has a sort key and no stored value, and the scan used to log an error and hand
    /// back the inverted sort key as if it were the checkpoint — a value its own comment called
    /// wrong, which every caller then trusted as the start of the replay window. The branch now
    /// fails the scan, and with it the index open, rather than seeding recovery from a number
    /// nobody can trust.
    #[test]
    fn a_checkpoint_scan_it_cannot_read_fails_instead_of_lying() {
        // Fast but not stored: ordering on the column finds the document, and the stored-field
        // read on it finds nothing.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("_seq", FAST);
        let tantivy_index = Index::create_in_ram(schema_builder.build());

        let seq_field = tantivy_index.schema().get_field("_seq").unwrap();
        let mut writer = tantivy_index.writer(50_000_000).unwrap();
        writer.add_document(doc!(seq_field => 42_u64)).unwrap();
        writer.commit().unwrap();

        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        assert!(
            store.get_highest_indexed_seq(&tantivy_index).is_err(),
            "a checkpoint the index cannot prove must fail the scan, not seed a replay window \
             from an untrusted number"
        );
    }

    /// A poisoned size-cache mutex must not panic every stats or shutdown call that follows.
    ///
    /// The release chose `panic = "unwind"` so a contained panic costs one request — a
    /// `.lock().unwrap()` on a poisoned mutex turns that one contained panic into a panic on
    /// every later call, which is the failure this test is named after. The cache holds only
    /// derived figures, so recovering the guard is safe, and is what every other mutex in the
    /// process already does (`audit.rs`, `session.rs`, the rate limiter).
    #[test]
    fn a_poisoned_size_cache_does_not_poison_the_calls_that_follow() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let cache = Arc::clone(&store.index_size_cache);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = cache.lock().unwrap();
            panic!("the contained panic the cache is expected to survive");
        }));
        assert!(
            cache.is_poisoned(),
            "the fixture must leave the cache poisoned"
        );

        store
            .gather_index_stats(false)
            .expect("stats recover the guard rather than panicking");
        store.invalidate_size_cache("anything");
        store
            .shutdown()
            .expect("shutdown recovers the guard rather than panicking");
    }

    /// A value that is not a facet path is refused, not handed to a constructor that panics.
    ///
    /// `Facet: From<&str>` unwraps `from_text`, so `add_facet(field, "electronics/phones")` panics
    /// — on a shard's writer thread, from a document body, and with `panic = "abort"` in the
    /// release profile that ends the process rather than the request. Nothing reaches it today
    /// because the orchestrator refuses every value against a declared facet field, which makes
    /// that refusal load-bearing by accident; this is the guard that means it does not have to be.
    ///
    /// Driven straight at the write paths, since the orchestrator is what currently stops a facet
    /// value getting this far and the point is what happens when it does.
    #[test]
    fn a_value_that_is_not_a_facet_path_is_refused_rather_than_fatal() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "id".to_string(),
            FieldDef::new("id".to_string(), TantivyFieldType::Text),
        );
        schema.fields.insert(
            "cat".to_string(),
            FieldDef::new("cat".to_string(), TantivyFieldType::Facet),
        );
        schema.normalize_after_deserialization();
        store.store_schema_and_cache("shop", &schema).unwrap();

        // Every shape `from_text` rejects: no leading slash, and empty.
        for bad in ["electronics/phones", ""] {
            let refused = store.apply_write(
                "shop",
                WalOp::Put {
                    id: "x".to_string(),
                    json_blob: Some(serde_json::json!({"id": "x", "cat": bad})),
                },
            );
            match refused {
                Err(StoreError::InvalidFieldValue { field, reason }) => {
                    assert_eq!(field, "cat", "the refusal names the field");
                    assert!(
                        reason.contains("facet path") && reason.contains("/electronics/phones"),
                        "and says what one looks like: {reason}"
                    );
                }
                other => panic!("{bad:?} should be refused as a bad value, got {other:?}"),
            }

            // The batch path builds its documents separately and needs its own guard.
            assert!(
                matches!(
                    store.apply_batch(
                        "shop",
                        vec![WalOp::Put {
                            id: "y".to_string(),
                            json_blob: Some(serde_json::json!({"id": "y", "cat": bad})),
                        }]
                    ),
                    Err(StoreError::InvalidFieldValue { .. })
                ),
                "the batch path must refuse {bad:?} too"
            );
        }

        // A real path is accepted on both, and the ancestors Tantivy indexes are what make a
        // parent match its descendants.
        store
            .apply_write(
                "shop",
                WalOp::Put {
                    id: "a".to_string(),
                    json_blob: Some(serde_json::json!({"id": "a", "cat": "/electronics/phones"})),
                },
            )
            .expect("a facet path is accepted");
        store
            .apply_batch(
                "shop",
                vec![WalOp::Put {
                    id: "b".to_string(),
                    json_blob: Some(
                        serde_json::json!({"id": "b", "cat": "/electronics/phones/cases"}),
                    ),
                }],
            )
            .expect("and in a batch");
        store.commit_index("shop").expect("commit");

        let parent = store
            .search_documents("shop", "cat:/electronics", 10, None)
            .expect("search the parent path");
        assert_eq!(
            parent.hits.len(),
            2,
            "a parent path matches both descendants: {:?}",
            parent.discarded
        );
    }

    /// A batch that names the same id twice keeps the last document, not one per put.
    ///
    /// redb already holds the last row; the Tantivy index used to get one document per put, so a
    /// content query returned the same id twice until the next rewrite or delete. The batch now
    /// coalesces to the last operation per id.
    #[test]
    fn a_batch_keeps_the_last_document_of_a_repeated_id() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
        );
        schema.normalize_after_deserialization();
        store.store_schema_and_cache("notes", &schema).unwrap();

        store
            .apply_batch(
                "notes",
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"title": "hello"})),
                    },
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"title": "world"})),
                    },
                ],
            )
            .expect("write");
        store.commit_index("notes").expect("commit");

        let all = store
            .search_documents("notes", "*", 10, None)
            .expect("search");
        assert_eq!(
            all.hits.len(),
            1,
            "one id written twice is one document, not two: {:?}",
            all.hits
        );

        let last = store
            .search_documents("notes", "title:world", 10, None)
            .expect("search");
        assert_eq!(last.hits.len(), 1, "the last put wins: {:?}", last.hits);

        let first = store
            .search_documents("notes", "title:hello", 10, None)
            .expect("search");
        assert_eq!(
            first.hits.len(),
            0,
            "the overwritten put must not linger: {:?}",
            first.hits
        );
    }

    /// A delete and a re-put of one id in a single batch replace the committed document.
    ///
    /// The other half of the same rule, and the one that survived the first fix. A delete
    /// earlier in the batch removes the redb row, so the re-put's `insert` displaces nothing and
    /// the id looked new — no `delete_term` was issued, and the document committed by an earlier
    /// batch stayed in the index beside the replacement. Two hits for one id, both reading back
    /// as the new body, because the read path joins to redb by id and redb held only one.
    ///
    /// Reachable without a hand-built batch: a single delete travels as a `WalOp::Delete` and
    /// coalesces into the same `apply_batch` as the writes that arrive with it.
    #[test]
    fn a_batch_replaces_a_committed_document_it_deletes_and_puts_again() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
        );
        schema.normalize_after_deserialization();
        store.store_schema_and_cache("notes", &schema).unwrap();

        // Committed by an earlier batch, so the index holds it before the one under test starts.
        store
            .apply_batch(
                "notes",
                vec![WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"title": "hello"})),
                }],
            )
            .expect("write the first version");
        store.commit_index("notes").expect("commit");

        store
            .apply_batch(
                "notes",
                vec![
                    WalOp::Delete {
                        id: "d1".to_string(),
                    },
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"title": "world"})),
                    },
                ],
            )
            .expect("delete and re-put in one batch");
        store.commit_index("notes").expect("commit");

        let all = store
            .search_documents("notes", "*", 10, None)
            .expect("search");
        assert_eq!(
            all.hits.len(),
            1,
            "the re-put replaces the committed document rather than joining it: {:?}",
            all.hits
        );

        let stale = store
            .search_documents("notes", "title:hello", 10, None)
            .expect("search");
        assert_eq!(
            stale.hits.len(),
            0,
            "the deleted version must be gone from the index: {:?}",
            stale.hits
        );
    }

    /// A batch that deletes an id it never re-puts leaves nothing behind.
    ///
    /// The companion to the case above, so the `existed_before` bookkeeping cannot be "satisfied"
    /// by never removing anything.
    #[test]
    fn a_batch_that_puts_then_deletes_an_id_leaves_no_document() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
        );
        schema.normalize_after_deserialization();
        store.store_schema_and_cache("notes", &schema).unwrap();

        store
            .apply_batch(
                "notes",
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"title": "hello"})),
                    },
                    WalOp::Delete {
                        id: "d1".to_string(),
                    },
                ],
            )
            .expect("put then delete in one batch");
        store.commit_index("notes").expect("commit");

        let all = store
            .search_documents("notes", "*", 10, None)
            .expect("search");
        assert_eq!(
            all.hits.len(),
            0,
            "the delete is the batch's last word on the id: {:?}",
            all.hits
        );
    }

    /// A delete must not bring an index into existence.
    ///
    /// The write path opens through `get_or_create_index`, which creates an index when it is
    /// absent — right for a put, which is a caller asking for the index, and wrong for a delete,
    /// where an empty index and a Tantivy directory on disk would be the trace left by removing
    /// nothing. Asserted on both write paths, and on the batch rule that a put in the same batch
    /// still creates.
    #[test]
    fn deleting_from_an_unknown_index_creates_nothing() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();
        let indices = temp_dir.path().join("indices");

        for op in [
            WalOp::Delete {
                id: "d1".to_string(),
            },
            WalOp::Delete {
                id: "d2".to_string(),
            },
        ] {
            let refused = store.apply_write("ghost", op);
            assert!(
                matches!(refused, Err(StoreError::IndexNotFound(_))),
                "a delete against an unknown index must be refused: {refused:?}"
            );
        }
        assert!(
            matches!(
                store.apply_batch(
                    "ghost",
                    vec![WalOp::Delete {
                        id: "d1".to_string()
                    }]
                ),
                Err(StoreError::IndexNotFound(_))
            ),
            "and so must a batch of nothing but deletes"
        );
        assert!(
            !indices.join("ghost").exists(),
            "nothing may be created on disk for an index that was never created"
        );
        assert!(!store.index_exists("ghost"));

        // A created index accepts a delete for a document it does not hold: the id is absent,
        // which is not the same as the index being absent, and deletion is idempotent.
        store
            .store_schema_and_cache("real", &IndexSchema::default())
            .unwrap();
        assert!(store.index_exists("real"));
        store
            .apply_write(
                "real",
                WalOp::Delete {
                    id: "never-written".to_string(),
                },
            )
            .expect("a delete of an absent id is not an error");

        // A batch carrying a put still creates, because that put asked for the index.
        store
            .apply_batch(
                "fresh",
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1"})),
                    },
                    WalOp::Delete {
                        id: "d1".to_string(),
                    },
                ],
            )
            .expect("a batch with a put creates the index");
        assert!(indices.join("fresh").exists());
    }

    /// A key lookup answers with the row as it now stands, after an update and after a delete.
    ///
    /// This used to be a property of an invalidation protocol rather than of the store. A
    /// per-index cache of document bodies sat in front of redb, its only writer a hydration on
    /// the read path, and until 2026-08-26 its only invalidation was dropping an entire index —
    /// so a document read once and then updated kept serving its previous body, and one read
    /// once and then deleted kept being served at all, which is fatal for deletion because an
    /// `id:VALUE` lookup is answered from redb and never consults Tantivy. The cache is gone
    /// (2026-09-19), so the property now holds by construction: `get_by_key` reads redb, and
    /// redb's own page cache is coherent with its own writes.
    ///
    /// The test stays, because "a read after a write sees the write" is worth asserting however
    /// it comes to be true, and because both paths — single write and batch — are covered.
    #[test]
    fn a_key_lookup_sees_the_latest_row_on_both_write_paths() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(small_store_config(&temp_dir), 1).unwrap();

        let body = |index: &str, id: &str| -> Option<String> {
            store
                .get_by_key(index, id)
                .expect("read")
                .map(|bytes| String::from_utf8(bytes).expect("utf-8"))
        };

        // --- single-write path
        let single = "cache_single";
        store
            .apply_write(
                single,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"id": "d1", "title": "v1"})),
                },
            )
            .unwrap();
        assert!(
            body(single, "d1").expect("v1 present").contains("v1"),
            "the first read populates the cache"
        );

        store
            .apply_write(
                single,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"id": "d1", "title": "v2"})),
                },
            )
            .unwrap();
        let updated = body(single, "d1").expect("v2 present");
        assert!(
            updated.contains("v2") && !updated.contains("v1"),
            "an update must be visible, not shadowed by the cached body: {updated}"
        );

        store
            .apply_write(
                single,
                WalOp::Delete {
                    id: "d1".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            body(single, "d1"),
            None,
            "a deleted document must not be served from the cache"
        );
        assert!(
            store
                .get_batch_by_keys(single, &["d1".to_string()])
                .unwrap()
                .is_empty(),
            "the batch path hydrates search hits and must agree"
        );

        // --- batch path
        let batch = "cache_batch";
        store
            .apply_batch(
                batch,
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1", "title": "v1"})),
                    },
                    WalOp::Put {
                        id: "d2".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d2", "title": "keep"})),
                    },
                ],
            )
            .unwrap();
        assert!(body(batch, "d1").is_some() && body(batch, "d2").is_some());

        store
            .apply_batch(
                batch,
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1", "title": "v2"})),
                    },
                    WalOp::Delete {
                        id: "d2".to_string(),
                    },
                ],
            )
            .unwrap();
        let updated = body(batch, "d1").expect("v2 present");
        assert!(
            updated.contains("v2") && !updated.contains("v1"),
            "a batched update must be visible: {updated}"
        );
        assert_eq!(body(batch, "d2"), None, "a batched delete must be visible");
    }

    fn small_store_config(temp_dir: &TempDir) -> StorageConfig {
        StorageConfig {
            max_open_indexes: 0,
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        }
    }

    /// Invalidating one index's cached sizes leaves its neighbours' entries alone.
    ///
    /// The cache keys used to be formatted strings and invalidation was a substring match, so
    /// evicting `"a"` also evicted `"ab"`. The fix is only pinned by asserting the neighbour
    /// survives, because an over-evicting cache still answers correctly — it just measures
    /// again, which no caller can see.
    #[test]
    fn invalidating_one_indexs_cached_sizes_leaves_its_neighbours_alone() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(small_store_config(&temp_dir), 1).expect("store");

        let entry = IndexSizeCache {
            tantivy_bytes: 1,
            redb_bytes: 2,
            document_count: 3,
            timestamp: Instant::now(),
        };
        {
            let mut cache = store.index_size_cache.lock().unwrap();
            for index in ["a", "ab"] {
                cache.insert((false, index.to_string()), entry.clone());
                cache.insert((true, index.to_string()), entry.clone());
            }
        }

        store.invalidate_size_cache("a");

        let cache = store.index_size_cache.lock().unwrap();
        assert_eq!(cache.len(), 2, "only 'a' should have been evicted");
        assert!(cache.contains_key(&(false, "ab".to_string())));
        assert!(cache.contains_key(&(true, "ab".to_string())));
        assert!(!cache.contains_key(&(false, "a".to_string())));
        assert!(!cache.contains_key(&(true, "a".to_string())));
    }

    /// A commit does not re-walk the index directory to refresh the memory budget.
    ///
    /// `commit_index` used to call `get_optimal_memory_budget` on every commit, which is a
    /// `read_dir` plus a `stat` per file — some three hundred syscalls on a fifty-segment index
    /// — on the writer thread, in the window between the Tantivy commit and the checkpoint
    /// transaction. It fed one thing: the five-bucket size class that scales how many
    /// operations accumulate before the *next* commit, whose boundaries are 100MB, 500MB, 2GB
    /// and 8GB apart. A commit cannot move an index across one of those, so the measurement was
    /// precision nobody could use.
    ///
    /// The timestamp is the evidence: it is stamped when the budget is measured, so an
    /// unchanged one means no walk happened. `should_commit_writer` ages it out on its own TTL,
    /// which bounds the walk per index rather than per commit.
    #[test]
    fn a_commit_does_not_re_measure_the_memory_budget() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(small_store_config(&temp_dir), 1).expect("store");
        let index = "cadence";

        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "one" })),
                },
            )
            .expect("write");

        let measured_at = store
            .budget_cache
            .get(index)
            .expect("opening the index measures the budget once")
            .value()
            .measured_at;

        for _ in 0..3 {
            store.commit_index(index).expect("commit");
        }

        let after = store
            .budget_cache
            .get(index)
            .expect("the entry must still be there")
            .value()
            .measured_at;
        assert_eq!(
            measured_at, after,
            "a commit must not re-measure the budget; the TTL decides when that happens"
        );

        // And the heuristic the budget feeds still answers, from the entry that stood.
        assert!(
            !store.should_commit_writer(index, 0),
            "nothing pending, so no commit is due"
        );
        assert_eq!(
            store
                .budget_cache
                .get(index)
                .expect("entry")
                .value()
                .measured_at,
            measured_at,
            "a fresh entry must be used as-is rather than re-measured"
        );
    }

    /// A budget entry past its TTL does not deadlock the writer thread.
    ///
    /// `should_commit_writer` reads the cached budget and, when it is stale, re-measures and
    /// writes the result back. `DashMap::get` hands back a `Ref` holding a read lock on the
    /// map's shard, and a match scrutinee's temporary lives to the end of the match — so the
    /// original form asked that same shard for its write lock from inside an arm, while this
    /// thread still held the read lock. dashmap's `RwLock` is not reentrant, so the thread
    /// waited for itself, on the shard's writer thread, forever.
    ///
    /// Two details are why it survived review and every test above. It needs the *stale* arm,
    /// unreachable until `BUDGET_CACHE_TTL` has passed since the index's writer was opened, so
    /// short tests all took the fresh arm — [`a_commit_does_not_re_measure_the_memory_budget`]
    /// included. And it needs the entry to be *present*: a missing one makes `get` return
    /// `None`, which holds no guard, so the insert goes through. Present-but-stale is the only
    /// state that hangs, and it is the state every long-lived index reaches.
    ///
    /// Asserted with a deadline on another thread, because the regression is a hang and an
    /// assertion on a return value cannot fail if the call never returns. ROADMAP OB14.
    #[test]
    fn a_stale_budget_entry_does_not_deadlock_the_writer() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(HybridStore::new(small_store_config(&temp_dir), 1).expect("store"));
        let index = "stale";

        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "one" })),
                },
            )
            .expect("write");

        // Age the entry past the TTL in place, so the stale arm is taken without waiting out
        // `BUDGET_CACHE_TTL` in real time. The entry stays present, which is the state that hangs.
        let aged = Instant::now()
            .checked_sub(BUDGET_CACHE_TTL + Duration::from_secs(1))
            .expect("clock far enough from its origin to age an entry");
        store.budget_cache.insert(
            index.to_string(),
            BudgetCacheEntry {
                budget: 32 * 1024 * 1024,
                measured_at: aged,
            },
        );
        assert!(
            store
                .budget_cache
                .get(index)
                .expect("entry")
                .value()
                .is_stale(),
            "the entry has to be stale for this test to exercise anything"
        );

        let (tx, rx) = std::sync::mpsc::channel();
        let probe = Arc::clone(&store);
        std::thread::spawn(move || {
            let _ = probe.should_commit_writer(index, 0);
            let _ = tx.send(());
        });

        rx.recv_timeout(Duration::from_secs(10)).expect(
            "should_commit_writer must return on a stale entry, not deadlock on its own shard",
        );

        assert!(
            !store
                .budget_cache
                .get(index)
                .expect("entry")
                .value()
                .is_stale(),
            "the stale entry must have been replaced by a fresh measurement"
        );
    }

    /// The tenant stamp survives the schema Tantivy is merged into.
    ///
    /// `get_schema_cached` prefers Tantivy as the source of truth for *fields*, and the tempting
    /// reading of that is that the returned schema is the derived one. It is not: the stored
    /// schema is the base and Tantivy's fields are merged onto it, which is the only reason
    /// `tenant` — and `description`, and the timestamps — survive a read at all.
    ///
    /// Worth pinning because the failure is silent and expensive. `derive_index_schema_from_tantivy`
    /// builds its value with `tenant: None`, since Tantivy stores fields and not ownership. If
    /// that value ever became the base rather than the field source, every index would read back
    /// unowned, every tenant's usage would drop to zero, and the quota would stop bounding
    /// anything without a single error anywhere.
    #[test]
    fn the_tenant_stamp_survives_a_schema_read() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(small_store_config(&temp_dir), 1).expect("store");
        let index = "owned";

        let mut schema = IndexSchema {
            tenant: Some("acme".to_string()),
            ..IndexSchema::default()
        };
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
        );
        store
            .store_schema_and_cache(index, &schema)
            .expect("store schema");

        // A write builds the Tantivy index, so the read below takes the merge path rather than
        // the stored-only fallback.
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "one" })),
                },
            )
            .expect("write");
        store.invalidate_schema_cache(index);

        let read = store
            .get_schema_cached(index)
            .expect("read schema")
            .expect("the index has one");
        assert_eq!(
            read.tenant.as_deref(),
            Some("acme"),
            "the stamp was lost on the way back out; every index would read as unowned"
        );
    }

    /// A WAL entry written by the previous build still decodes.
    ///
    /// Those entries are whole `WalOp` JSON values, and an upgrade can find a tail of them left
    /// behind by the process that died. Only the id is taken — the body they carry is ignored in
    /// favour of the `data_<index>` row — so one replay path serves both formats and no
    /// migration of the tail is needed.
    #[test]
    fn a_legacy_wal_entry_still_yields_its_document_id() {
        let legacy_put = serde_json::to_vec(&WalOp::Put {
            id: "doc-7".to_string(),
            json_blob: Some(serde_json::json!({ "title": "written by the old build" })),
        })
        .expect("serialize legacy put");
        assert_eq!(
            decode_wal_entry(&legacy_put).expect("decode legacy put"),
            "doc-7"
        );

        let legacy_delete = serde_json::to_vec(&WalOp::Delete {
            id: "doc-8".to_string(),
        })
        .expect("serialize legacy delete");
        assert_eq!(
            decode_wal_entry(&legacy_delete).expect("decode legacy delete"),
            "doc-8"
        );

        // Legacy entries are JSON objects, so they cannot be mistaken for the tagged format.
        assert_ne!(legacy_put[0], WAL_ENTRY_ID_ONLY);
        assert_ne!(legacy_delete[0], WAL_ENTRY_ID_ONLY);
    }

    /// An id-only entry is a fraction of the bytes the old format wrote.
    ///
    /// This is the point of the change: the document was already being written to
    /// `data_<index>` in the same transaction, so the copy in the WAL doubled what every write
    /// serialised and fsynced for nothing.
    #[test]
    fn a_wal_entry_no_longer_carries_the_document() {
        let body = serde_json::json!({
            "title": "a representative document",
            "body": "x".repeat(1024),
            "tags": ["alpha", "beta", "gamma"],
        });
        let legacy = serde_json::to_vec(&WalOp::Put {
            id: "doc-1".to_string(),
            json_blob: Some(body),
        })
        .expect("serialize legacy put");
        let current = encode_wal_entry("doc-1");

        assert_eq!(current.len(), 6, "one tag byte plus the id");
        assert!(
            current.len() * 50 < legacy.len(),
            "an id-only entry should be orders of magnitude smaller: {} vs {}",
            current.len(),
            legacy.len()
        );
    }

    /// A field type serializes under the name every other surface calls it.
    ///
    /// The derived implementation emitted the variant name, so a schema said `Date` while the
    /// syntax reference, the per-field query hints and the deserializer's own canonical list all
    /// said `date`. An agent reading the schema and then reading how to query it was given two
    /// spellings of one type.
    #[test]
    fn a_field_type_serializes_as_the_name_everything_else_uses() {
        for field_type in [
            TantivyFieldType::Text,
            TantivyFieldType::String,
            TantivyFieldType::I64,
            TantivyFieldType::U64,
            TantivyFieldType::F64,
            TantivyFieldType::Date,
            TantivyFieldType::Boolean,
            TantivyFieldType::Bytes,
            TantivyFieldType::Ip,
            TantivyFieldType::Json,
            TantivyFieldType::Facet,
        ] {
            let serialized = serde_json::to_value(&field_type).unwrap();
            assert_eq!(
                serialized,
                JsonValue::String(field_type.to_string().to_string()),
                "{field_type:?} should serialize as its canonical lowercase name"
            );

            let round_tripped: TantivyFieldType = serde_json::from_value(serialized).unwrap();
            assert_eq!(round_tripped, field_type, "{field_type:?} must round-trip");
        }
    }

    /// Schemas persisted before the change above still load.
    ///
    /// This is what makes it safe to change at all: deserialization lowercases before matching,
    /// so a redb table full of `"Date"` is read exactly as one full of `"date"`.
    #[test]
    fn a_schema_written_with_the_old_capitalized_names_still_loads() {
        for (stored, expected) in [
            ("\"Date\"", TantivyFieldType::Date),
            ("\"Text\"", TantivyFieldType::Text),
            ("\"Boolean\"", TantivyFieldType::Boolean),
            ("\"I64\"", TantivyFieldType::I64),
        ] {
            let parsed: TantivyFieldType = serde_json::from_str(stored).unwrap();
            assert_eq!(parsed, expected, "{stored} should still deserialize");
        }
    }

    /// Warming must actually fill the per-field caches, and must skip a generation it has
    /// already warmed.
    ///
    /// Both halves are invisible to black-box tests: a `warm_index` that silently did nothing
    /// would leave every query correct but cold, and a missing generation guard would re-warm
    /// an idle index on every request. This inspects the store's own bookkeeping and tantivy's
    /// per-segment cache to pin down both.
    #[test]
    fn warm_index_fills_caches_and_skips_warm_generations() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            max_open_indexes: 0,
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        };

        let store = HybridStore::new(config, 1).unwrap();
        let index = "warm_wiring";
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .unwrap();

        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "first" })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let first = store.warm_index(index).unwrap().expect("stats");
        assert!(first.segments > 0, "committed data must produce a segment");
        assert_eq!(
            first.segments_warmed, first.segments,
            "the first warm must warm every segment"
        );

        // Nothing reloads a reader except `commit_index`, so the generation cannot move
        // under us here and a second warm must do no work.
        let second = store.warm_index(index).unwrap().expect("stats");
        assert_eq!(
            second.generation, first.generation,
            "no commit means no reload, so no new generation"
        );
        assert_eq!(
            second.segments_warmed, 0,
            "re-warming an unchanged generation must be a no-op"
        );

        // Prove the caches are actually populated rather than merely reported as warm: a
        // warmed segment answers inverted_index() from its cache. Comparing against a
        // freshly opened SegmentReader for the same segment shows the difference.
        let (reader, _) = store.get_reader(index).unwrap().unwrap();
        let searcher = reader.searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let id_field = segment_reader.schema().get_field("id").unwrap();
        assert!(
            segment_reader.inverted_index(id_field).is_ok(),
            "warmed segment should resolve its inverted index"
        );

        // A new commit publishes a new generation, which must be warmed again — the per-field
        // caches live on SegmentReaders that tantivy rebuilds on every reload.
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-2".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "second" })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let third = store.warm_index(index).unwrap().expect("stats");
        assert!(
            third.segments_warmed > 0,
            "a new searcher generation must be warmed, not skipped"
        );
        assert_eq!(third.num_docs, 2, "both documents should be searchable");
    }

    /// Both overridden analyzers must keep a token of exactly `MAX_INDEXED_TOKEN_LEN` bytes and
    /// drop the one byte past it.
    ///
    /// Tantivy's builtins cap these at 40 bytes, so the lower bound fails without the override.
    /// The upper bound is what pins `RemoveLongFilter`'s strictly-less-than limit: written
    /// without the `+ 1`, a token of exactly the cap disappears and this test is the only thing
    /// that says so.
    #[test]
    fn overridden_tokenizers_keep_tokens_up_to_the_cap() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            max_open_indexes: 0,
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        };

        let store = HybridStore::new(config, 1).unwrap();
        let index = "long_tokens";
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "fields": {
                "title": {"field_type": "text", "indexed": true},
                "body": {"field_type": "text", "indexed": true, "tokenizer": "en_stem"}
            }
        }))
        .unwrap();
        schema.normalize_after_deserialization();
        store.store_schema_and_cache(index, &schema).unwrap();

        let long_token = "a".repeat(MAX_INDEXED_TOKEN_LEN);
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": long_token })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-2".to_string(),
                    json_blob: Some(serde_json::json!({ "body": long_token })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        // One byte past the cap, in a third document, to fix where the boundary falls.
        let over_cap = "b".repeat(MAX_INDEXED_TOKEN_LEN + 1);
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-3".to_string(),
                    json_blob: Some(serde_json::json!({ "title": over_cap })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let outcome = store
            .search_documents(index, &format!("title:{long_token}"), 10, None)
            .unwrap();
        assert_eq!(
            outcome.total_hits, 1,
            "a {MAX_INDEXED_TOKEN_LEN}-byte token must be indexed and searchable"
        );

        // Same bound through the stemming tokenizer. Index-time and query-time analysis both
        // resolve `en_stem` from this index, so the two stay symmetric by construction.
        let stemmed = store
            .search_documents(index, &format!("body:{long_token}"), 10, None)
            .unwrap();
        assert_eq!(
            stemmed.total_hits, 1,
            "en_stem must also keep {MAX_INDEXED_TOKEN_LEN}-byte tokens"
        );

        let dropped = store
            .search_documents(index, &format!("title:{over_cap}"), 10, None)
            .unwrap();
        assert_eq!(
            dropped.total_hits, 0,
            "a token past the cap is dropped at index time, so nothing matches it"
        );
    }

    fn single_shard_store(dir: &TempDir) -> HybridStore {
        let config = StorageConfig {
            max_open_indexes: 0,
            shard_path: dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        };
        HybridStore::new(config, 1).unwrap()
    }

    /// An unknown tokenizer is refused when the schema is stored, not discovered at commit.
    ///
    /// Before the check, `hr_stem` — a name this engine did not have — stored, took a write into
    /// the WAL and answered success, and then every commit failed with "Error getting tokenizer
    /// for field" while searches quietly found nothing. The index could not recover without a
    /// new schema.
    #[test]
    fn an_unknown_tokenizer_is_refused_before_the_index_takes_writes() {
        let temp_dir = TempDir::new().unwrap();
        let store = single_shard_store(&temp_dir);
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "fields": {
                "title": {"field_type": "text", "indexed": true, "tokenizer": "hr_stemm"},
                "body": {"field_type": "text", "indexed": false, "tokenizer": "croatian"},
                "tag": {"field_type": "string", "indexed": true}
            }
        }))
        .unwrap();
        schema.normalize_after_deserialization();

        let reason = schema.validate_tokenizers().expect_err("must be refused");
        // Sorted, so the first offender named is stable; the unindexed field counts, since it
        // can be switched on later without its tokenizer being looked at again.
        assert!(
            reason.starts_with("field 'body' names tokenizer 'croatian'"),
            "{reason}"
        );
        assert!(reason.contains("(and 1 other field)"), "{reason}");
        assert!(
            reason.contains("hr_stem_fold"),
            "must list what is available: {reason}"
        );

        let err = store
            .store_schema_and_cache("wedged", &schema)
            .expect_err("the store must refuse it too");
        assert!(matches!(err, StoreError::Serialization(_)), "{err:?}");
        assert!(
            store.get_schema_cached("wedged").unwrap().is_none(),
            "nothing may be stored"
        );
    }

    /// The language analyzers, end to end: declared in a schema, written, committed, and found
    /// by an inflected form the document does not contain.
    #[test]
    fn language_tokenizers_find_inflected_forms() {
        let temp_dir = TempDir::new().unwrap();
        let store = single_shard_store(&temp_dir);
        let index = "sluzbene_novine";
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "fields": {
                "text_hr": {"field_type": "text", "indexed": true, "tokenizer": "hr_stem_fold"},
                "text_it": {"field_type": "text", "indexed": true, "tokenizer": "it_stem"},
                "text_plain": {"field_type": "text", "indexed": true}
            }
        }))
        .unwrap();
        schema.normalize_after_deserialization();
        schema.validate_tokenizers().unwrap();
        store.store_schema_and_cache(index, &schema).unwrap();

        let text_hr = "Županijska skupština Istarske županije donosi Odluku o proračunu";
        let text_it = "L'Assemblea della Regione Istriana emana i regolamenti";
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "akt-1".to_string(),
                    json_blob: Some(serde_json::json!({
                        "text_hr": text_hr,
                        "text_it": text_it,
                        "text_plain": text_hr,
                    })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let hits = |query: &str| {
            store
                .search_documents(index, query, 10, None)
                .unwrap()
                .total_hits
        };
        // Other cases of the same words, and written without diacritics.
        assert_eq!(hits("text_hr:županija"), 1);
        assert_eq!(hits("text_hr:zupanija"), 1);
        assert_eq!(hits("text_hr:proracun"), 1);
        assert_eq!(hits("text_hr:odluka"), 1);
        assert_eq!(hits(r#"text_hr:"istarska županija""#), 1);
        assert_eq!(hits("text_it:regolamento"), 1);
        // The same text under `default` matches only the forms written.
        assert_eq!(hits("text_plain:županija"), 0);
        assert_eq!(hits("text_plain:županije"), 1);
    }

    /// A traversal index name must not create or remove anything outside the
    /// shard. This exercises the real write and delete paths rather than the
    /// validator in isolation, because the earlier canonicalize-based guard
    /// passed its unit tests while still allowing `create_dir_all` to escape.
    #[test]
    fn traversal_index_name_cannot_touch_paths_outside_shard() {
        let parent = TempDir::new().unwrap();
        let shard_path = parent.path().join("shard");
        std::fs::create_dir_all(&shard_path).unwrap();

        // A sibling of the shard that must survive untouched.
        let victim = parent.path().join("victim");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("keep.txt"), b"precious").unwrap();

        let config = StorageConfig {
            max_open_indexes: 0,
            shard_path: shard_path.clone(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        };
        let store = HybridStore::new(config, 1).unwrap();

        for name in ["../victim", "..", "../../etc", "a/b"] {
            assert!(
                matches!(store.index_dir(name), Err(StoreError::InvalidIndexName(_))),
                "index_dir must reject '{}'",
                name
            );

            // The write path creates the index directory, so it must refuse too.
            let write = store.apply_write(
                name,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "x" })),
                },
            );
            assert!(write.is_err(), "apply_write must reject '{}'", name);

            // The delete path removes a directory, so it must refuse too.
            assert!(
                store.delete_index_data(name, true).is_err(),
                "delete_index_data must reject '{}'",
                name
            );
        }

        // Nothing outside the shard was created or removed.
        assert!(victim.join("keep.txt").exists(), "sibling file was deleted");
        assert_eq!(
            std::fs::read(victim.join("keep.txt")).unwrap(),
            b"precious",
            "sibling file was modified"
        );
        assert!(
            !parent.path().join("etc").exists(),
            "a directory was created outside the shard"
        );
    }

    #[test]
    fn test_multi_tenant_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            max_open_indexes: 0,
            shard_path: temp_dir.path().to_path_buf(),

            // Memory Budget Configuration
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 32,
            indexer_memory_max_mb: 512,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,

            // Thread Configuration
            indexer_num_threads: 1,
            merge_num_threads: 2,

            // Other Configuration
            default_batch_size: 1000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: Default::default(),
        };

        let store = HybridStore::new(config, 1).unwrap();

        // Write to index1
        let op1 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq1 = store.apply_write("index1", op1).unwrap();
        assert_eq!(seq1, 1);

        // Write to index2
        let op2 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq2 = store.apply_write("index2", op2).unwrap();
        assert_eq!(seq2, 1); // Independent sequence

        // Verify directories exist
        let index1_path = temp_dir.path().join("indices").join("index1");
        let index2_path = temp_dir.path().join("indices").join("index2");
        assert!(index1_path.exists());
        assert!(index2_path.exists());

        // Delete index1 (with schema deletion)
        store.delete_index_data("index1", true).unwrap();

        // Verify index1 is gone but index2 remains
        assert!(!index1_path.exists());
        assert!(index2_path.exists());

        // Verify index2 still works
    }

    #[test]
    fn test_field_type_inference() {
        use crate::{FieldDef, TantivyFieldType};
        use serde_json::json;

        // Test field type inference from JSON values
        let test_cases = vec![
            (json!("hello"), TantivyFieldType::Text),
            (json!("2023-01-01T00:00:00Z"), TantivyFieldType::Date),
            (json!("192.168.1.1"), TantivyFieldType::Ip),
            (json!(42), TantivyFieldType::I64),
            (json!(std::f64::consts::PI), TantivyFieldType::F64),
            (json!(true), TantivyFieldType::Boolean),
            (json!(null), TantivyFieldType::Text),
            (json!({"key": "value"}), TantivyFieldType::Json),
            // A list is several values of one field, so it is typed by what it holds. Nulls
            // store nothing, so they are not evidence against the type; elements that disagree,
            // and lists or objects inside a list, leave text as the only type holding both.
            (json!([1, 2, 3]), TantivyFieldType::I64),
            (json!([1, null, 3]), TantivyFieldType::I64),
            (json!([1, 2.5]), TantivyFieldType::F64),
            (json!([true, false]), TantivyFieldType::Boolean),
            (json!(["2023-01-01T00:00:00Z"]), TantivyFieldType::Date),
            (json!([1, "two"]), TantivyFieldType::Text),
            (json!([[1, 2]]), TantivyFieldType::Text),
            (json!([{"k": 1}]), TantivyFieldType::Text),
            (json!([]), TantivyFieldType::Text),
            (json!([null]), TantivyFieldType::Text),
        ];

        for (value, expected_type) in test_cases {
            let inferred_type = FieldDef::infer_type_from_value(&value);
            assert_eq!(
                inferred_type, expected_type,
                "Failed to infer type for value: {:?}",
                value
            );
        }

        println!("✅ Field type inference works correctly!");
    }

    #[test]
    fn test_field_def_creation() {
        use crate::{FieldDef, TantivyFieldType};

        // Test FieldDef creation with different types
        let text_field = FieldDef::new("title".to_string(), TantivyFieldType::Text);
        assert_eq!(text_field.field_type, TantivyFieldType::Text);
        assert!(text_field.indexed);
        assert!(!text_field.stored); // Only "id" field is stored in Tantivy
        assert!(!text_field.is_fast()); // Text fields are not fast by default

        let i64_field = FieldDef::new("count".to_string(), TantivyFieldType::I64);
        assert_eq!(i64_field.field_type, TantivyFieldType::I64);
        assert!(i64_field.indexed);
        assert!(!i64_field.stored); // Only "id" field is stored in Tantivy
        assert!(i64_field.is_fast()); // Numeric fields are fast by default

        // Test the "id" field special case
        let id_field = FieldDef::new("id".to_string(), TantivyFieldType::Text);
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored); // "id" field is stored in Tantivy
        assert!(id_field.is_fast()); // the builder always gives the key a fast column

        let json_field = FieldDef::new("metadata".to_string(), TantivyFieldType::Json);
        assert_eq!(json_field.field_type, TantivyFieldType::Json);
        assert!(json_field.indexed);
        assert!(!json_field.stored); // Only "id" field is stored in Tantivy
        assert!(!json_field.is_fast()); // JSON fields are not fast by default

        println!("✅ FieldDef creation works correctly!");
    }

    #[test]
    fn test_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial fields
        let doc1 = json!({
            "name": "Test",
            "value": 123
        });

        let evolved_fields = schema.evolve_from_document(&doc1);
        assert_eq!(evolved_fields.len(), 2);
        assert_eq!(schema.fields.len(), 2);

        // Verify field types
        assert_eq!(
            schema.fields.get("name").unwrap().field_type,
            TantivyFieldType::Text
        );
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::I64
        );

        // Evolve with new document
        let doc2 = json!({
            "name": "Test 2",
            "value": 456.789, // Should evolve to F64
            "created_at": "2023-01-01T00:00:00Z" // New field
        });

        let evolved_fields = schema.evolve_from_document(&doc2);
        assert_eq!(evolved_fields.len(), 2); // value evolved + created_at added
        assert_eq!(schema.fields.len(), 3);

        // Verify evolution
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::F64
        );
        assert_eq!(
            schema.fields.get("created_at").unwrap().field_type,
            TantivyFieldType::Date
        );

        println!("✅ Schema evolution works correctly!");
    }

    #[test]
    fn test_tantivy_date_comparison_with_clamping() {
        use tantivy::DateTime;

        // Test that our clamping strategy works correctly
        // 1606-01-01 (Volpone publication - would overflow without clamping)
        let old_ts: i64 = -11_486_668_800;
        let clamped_old_ts = old_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let old_tantivy = DateTime::from_timestamp_secs(clamped_old_ts);

        // 2023-05-27 (Query bound)
        let new_ts: i64 = 1_685_145_600; // 2023-05-27T00:00:00Z
        let new_tantivy = DateTime::from_timestamp_secs(new_ts);

        println!(
            "1606-01-01 (clamped to 1677): timestamp={}, tantivy={:?}",
            clamped_old_ts, old_tantivy
        );
        println!(
            "2023-05-27: timestamp={}, tantivy={:?}",
            new_ts, new_tantivy
        );

        // With clamping, 1677 should be LESS than 2023
        assert!(
            old_tantivy < new_tantivy,
            "Clamped 1677 date should be less than 2023 date"
        );
        assert_eq!(
            clamped_old_ts, TANTIVY_MIN_TIMESTAMP_SECS,
            "Pre-1677 date should be clamped to minimum"
        );

        // Test future date clamping
        let future_ts: i64 = 10_000_000_000; // Beyond 2262
        let clamped_future =
            future_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        assert_eq!(
            clamped_future, TANTIVY_MAX_TIMESTAMP_SECS,
            "Post-2262 date should be clamped to maximum"
        );

        println!("✅ Tantivy DateTime clamping works correctly for out-of-range dates!");
    }

    #[test]
    fn test_background_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial document with new fields
        let doc = json!({
            "title": "Test Document",
            "count": 42,
            "timestamp": "2023-01-01T00:00:00Z"
        });

        let evolved_fields = schema.evolve_from_document(&doc);
        assert_eq!(evolved_fields.len(), 3, "Should discover 3 new fields");

        // Verify all new fields are non-indexed
        let title_field = schema.fields.get("title").unwrap();
        assert_eq!(title_field.field_type, TantivyFieldType::Text);
        assert!(!title_field.indexed, "New fields should be non-indexed");
        assert!(
            !title_field.stored,
            "Only 'id' field should be stored in Tantivy"
        );

        let count_field = schema.fields.get("count").unwrap();
        assert_eq!(count_field.field_type, TantivyFieldType::I64);
        assert!(!count_field.indexed, "New fields should be non-indexed");
        assert!(count_field.is_fast(), "Numeric fields should be fast");

        let timestamp_field = schema.fields.get("timestamp").unwrap();
        assert_eq!(timestamp_field.field_type, TantivyFieldType::Date);
        assert!(!timestamp_field.indexed, "New fields should be non-indexed");

        // Verify we can get non-indexed fields
        let non_indexed = schema.get_non_indexed_fields();
        assert_eq!(non_indexed.len(), 3, "Should have 3 non-indexed fields");
        assert!(non_indexed.contains(&"title".to_string()));
        assert!(non_indexed.contains(&"count".to_string()));
        assert!(non_indexed.contains(&"timestamp".to_string()));

        // Test promoting a field to indexed
        let promoted = schema.promote_field_to_indexed("title");
        assert!(promoted, "Should successfully promote field");
        assert!(
            schema.fields.get("title").unwrap().indexed,
            "Field should now be indexed"
        );

        // Verify non-indexed count decreased
        let non_indexed_after = schema.get_non_indexed_fields();
        assert_eq!(
            non_indexed_after.len(),
            2,
            "Should have 2 non-indexed fields after promotion"
        );
        assert!(
            !non_indexed_after.contains(&"title".to_string()),
            "Promoted field should not be in list"
        );

        // Test promoting already indexed field
        let promoted_again = schema.promote_field_to_indexed("title");
        assert!(!promoted_again, "Should not promote already indexed field");

        println!("✅ Background schema evolution works correctly!");
    }

    #[test]
    fn test_type_aliases_deserialization() {
        use serde_json;

        // Test various type aliases deserialize correctly
        let test_cases = vec![
            ("float", TantivyFieldType::F64),
            ("double", TantivyFieldType::F64),
            ("decimal", TantivyFieldType::F64),
            ("integer", TantivyFieldType::I64),
            ("int", TantivyFieldType::I64),
            ("number", TantivyFieldType::I64),
            ("signed", TantivyFieldType::I64),
            ("unsigned", TantivyFieldType::U64),
            ("uint", TantivyFieldType::U64),
            ("bool", TantivyFieldType::Boolean),
            ("datetime", TantivyFieldType::Date),
            ("timestamp", TantivyFieldType::Date),
            ("binary", TantivyFieldType::Bytes),
            ("blob", TantivyFieldType::Bytes),
            ("object", TantivyFieldType::Json),
            ("document", TantivyFieldType::Json),
            ("category", TantivyFieldType::Facet),
            ("tag", TantivyFieldType::Facet),
            // Test canonical names still work
            ("text", TantivyFieldType::Text),
            ("string", TantivyFieldType::String),
            ("i64", TantivyFieldType::I64),
            ("u64", TantivyFieldType::U64),
            ("f64", TantivyFieldType::F64),
            ("date", TantivyFieldType::Date),
            ("boolean", TantivyFieldType::Boolean),
            ("bytes", TantivyFieldType::Bytes),
            ("ip", TantivyFieldType::Ip),
            ("json", TantivyFieldType::Json),
            ("facet", TantivyFieldType::Facet),
        ];

        for (alias, expected) in test_cases {
            let json = format!(r#"{{"field_type": "{}"}}"#, alias);
            let field_def: FieldDef = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("Failed to deserialize '{}': {}", alias, e));

            assert_eq!(
                field_def.field_type, expected,
                "Alias '{}' should map to {:?}, got {:?}",
                alias, expected, field_def.field_type
            );
        }

        // Test case-insensitive
        let json = r#"{"field_type": "FLOAT"}"#;
        let field_def: FieldDef = serde_json::from_str(json).unwrap();
        assert_eq!(field_def.field_type, TantivyFieldType::F64);

        // Test invalid type gives helpful error
        let json = r#"{"field_type": "invalid_type"}"#;
        let result: Result<FieldDef, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unknown field type: 'invalid_type'")
        );
        assert!(error.to_string().contains("Supported types:"));

        println!("✅ Type alias deserialization works correctly!");
    }

    #[test]
    fn test_python_schema_compatibility() {
        use serde_json;

        // Test the exact schema from ingest_urls.py
        let python_schema = serde_json::json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true, "stored": true},
                "sha1": {"field_type": "text", "indexed": false, "stored": false, "is_shadow": true},
                "first_analysis": {"field_type": "date", "indexed": true, "stored": false},
                "last_analysis": {"field_type": "date", "indexed": true, "stored": false},
                "platform": {"field_type": "text", "indexed": true, "stored": false},
                "classification": {"field_type": "text", "indexed": true, "stored": false},
                "risk_score": {"field_type": "float", "indexed": true, "stored": false},
                "threat_names": {"field_type": "text", "indexed": true, "stored": false},
                "file_types": {"field_type": "text", "indexed": true, "stored": false},
                "signatures": {"field_type": "text", "indexed": true, "stored": false},
                "urls": {"field_type": "text", "indexed": true, "stored": false}
            }
        });

        let schema: IndexSchema = serde_json::from_value(python_schema).unwrap();

        // Verify the float field was correctly mapped to F64
        let risk_field = schema.fields.get("risk_score").unwrap();
        assert_eq!(risk_field.field_type, TantivyFieldType::F64);
        assert!(risk_field.indexed);
        assert!(!risk_field.stored);

        // Verify other fields are correct
        let id_field = schema.fields.get("id").unwrap();
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored);

        let date_field = schema.fields.get("first_analysis").unwrap();
        assert_eq!(date_field.field_type, TantivyFieldType::Date);
        assert!(date_field.indexed);
        assert!(!date_field.stored);

        println!("✅ Python schema compatibility works correctly!");
    }

    #[test]
    fn test_ted_schema_compatibility() {
        use serde_json;

        // Test the exact schema from ingest_ted.py with integer type
        let ted_schema = serde_json::json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true, "stored": true},
                "video_id": {"field_type": "text", "indexed": false, "stored": false, "is_shadow": true},
                "title": {"field_type": "text", "indexed": true, "stored": false},
                "speaker": {"field_type": "text", "indexed": true, "stored": false},
                "channel": {"field_type": "text", "indexed": true, "stored": false},
                "description": {"field_type": "text", "indexed": true, "stored": false},
                "tags": {"field_type": "text", "indexed": true, "stored": false},
                "topic_categories": {"field_type": "text", "indexed": true, "stored": false},
                "category_id": {"field_type": "integer", "indexed": true, "stored": false},
                "category_label": {"field_type": "text", "indexed": true, "stored": false},
                "view_count": {"field_type": "integer", "indexed": true, "stored": false},
                "like_count": {"field_type": "integer", "indexed": true, "stored": false},
                "comment_count": {"field_type": "integer", "indexed": true, "stored": false},
                "caption": {"field_type": "boolean", "indexed": true, "stored": false},
                "published_at": {"field_type": "date", "indexed": true, "stored": false},
                "duration_seconds": {"field_type": "integer", "indexed": true, "stored": false}
            }
        });

        let schema: IndexSchema = serde_json::from_value(ted_schema).unwrap();

        // Verify the integer fields were correctly mapped to I64
        for field_name in [
            "category_id",
            "view_count",
            "like_count",
            "comment_count",
            "duration_seconds",
        ] {
            let field = schema.fields.get(field_name).unwrap();
            assert_eq!(field.field_type, TantivyFieldType::I64);
            assert!(field.indexed);
            assert!(!field.stored);
        }

        // Verify boolean field
        let caption_field = schema.fields.get("caption").unwrap();
        assert_eq!(caption_field.field_type, TantivyFieldType::Boolean);
        assert!(caption_field.indexed);
        assert!(!caption_field.stored);

        println!("✅ TED schema compatibility works correctly!");
    }

    #[test]
    fn test_schema_enrichment_preserves_explicit_values() {
        use serde_json;

        // Schema with a mix of minimal and explicit field definitions
        let schema_json = serde_json::json!({
            "fields": {
                "id": {"field_type": "text"},
                "title": {"field_type": "text", "indexed": true},
                "body": {"field_type": "text", "indexed": true, "tokenizer": "en_stem", "index_record_option": "Basic"},
                "score": {"field_type": "float", "indexed": true},
                "created_at": {"field_type": "date"},
                "tag": {"field_type": "string", "indexed": true, "tokenizer": "raw"},
                "sha1": {"field_type": "text", "is_shadow": true},
                "notes": {"field_type": "text", "indexed": false}
            }
        });

        let mut schema: IndexSchema = serde_json::from_value(schema_json).unwrap();
        schema.normalize_after_deserialization();

        // --- id field: always forced to specific Tantivy attributes ---
        let id = schema.fields.get("id").unwrap();
        assert_eq!(id.name, "id");
        assert!(id.indexed, "id must always be indexed");
        assert!(id.stored, "id must always be stored");
        assert_eq!(
            id.tokenizer.as_deref(),
            Some("raw"),
            "id must use raw tokenizer"
        );
        assert_eq!(
            id.index_record_option.as_deref(),
            Some("Basic"),
            "id must use Basic index option"
        );

        // --- title: minimal Text field gets enriched with defaults ---
        let title = schema.fields.get("title").unwrap();
        assert_eq!(title.name, "title");
        assert!(title.indexed);
        assert_eq!(
            title.tokenizer.as_deref(),
            Some("default"),
            "Text field should get default tokenizer"
        );
        assert_eq!(
            title.index_record_option.as_deref(),
            Some("WithFreqsAndPositions"),
            "Text field should get WithFreqsAndPositions"
        );

        // --- body: explicit tokenizer and index_record_option are PRESERVED ---
        let body = schema.fields.get("body").unwrap();
        assert_eq!(body.name, "body");
        assert!(body.indexed);
        assert_eq!(
            body.tokenizer.as_deref(),
            Some("en_stem"),
            "Explicit tokenizer must be preserved"
        );
        assert_eq!(
            body.index_record_option.as_deref(),
            Some("Basic"),
            "Explicit index_record_option must be preserved"
        );

        // --- score: F64 gets fast=true enrichment ---
        let score = schema.fields.get("score").unwrap();
        assert_eq!(score.name, "score");
        assert_eq!(score.field_type, TantivyFieldType::F64);
        assert!(score.indexed);
        assert!(
            score.is_fast(),
            "Numeric fields should be enriched with fast=true"
        );
        assert!(
            score.tokenizer.is_none(),
            "Numeric fields should not have tokenizer"
        );

        // --- created_at: Date gets fast=true, indexed defaults to true ---
        let created = schema.fields.get("created_at").unwrap();
        assert_eq!(created.name, "created_at");
        assert_eq!(created.field_type, TantivyFieldType::Date);
        assert!(created.indexed, "indexed defaults to true");
        assert!(
            created.is_fast(),
            "Date fields should be enriched with fast=true"
        );

        // --- tag: String field with explicit tokenizer preserved ---
        let tag = schema.fields.get("tag").unwrap();
        assert_eq!(tag.name, "tag");
        assert_eq!(tag.field_type, TantivyFieldType::String);
        assert_eq!(
            tag.tokenizer.as_deref(),
            Some("raw"),
            "Explicit tokenizer preserved for String"
        );
        assert_eq!(
            tag.index_record_option.as_deref(),
            Some("Basic"),
            "String gets Basic index option"
        );

        // --- sha1: shadow field is NOT enriched ---
        let sha1 = schema.fields.get("sha1").unwrap();
        assert!(sha1.is_shadow);
        assert!(
            sha1.tokenizer.is_none(),
            "Shadow fields should not be enriched"
        );

        // --- notes: non-indexed field is NOT enriched ---
        let notes = schema.fields.get("notes").unwrap();
        assert!(!notes.indexed);
        assert!(
            notes.tokenizer.is_none(),
            "Non-indexed fields should not be enriched"
        );

        // --- _seq: no longer injected ---
        // Normalization used to force this field into every schema so the built index would
        // carry a column for the checkpoint scan to order on. The commit payload answers that
        // question in O(1) now, so a schema declares only what its author declared. Indices
        // built while the field was injected still have the column and are still written to;
        // nothing new grows one.
        assert!(
            !schema.fields.contains_key("_seq"),
            "normalization must not invent a field the caller never declared"
        );

        println!("✅ Schema enrichment correctly preserves explicit values and fills defaults!");
    }

    /// A description is the one part of a schema nothing can infer, so it has to survive
    /// everything the schema does on its own.
    /// The thumbprint has to see every disagreement a cluster would have to reconcile.
    ///
    /// The names-only form could not. These two schemas are the divergence reproduced on a live
    /// three-node cluster — one node had typed the index from a declaration, the others from the
    /// first document to reach them — and they hashed identically, so a check built on the
    /// thumbprint would have polled all three and concluded they agreed.
    #[test]
    fn the_thumbprint_sees_a_type_divergence() {
        let build = |amount, label| {
            let mut schema = IndexSchema::default();
            schema
                .fields
                .insert("amount".into(), FieldDef::new("amount".into(), amount));
            schema
                .fields
                .insert("label".into(), FieldDef::new("label".into(), label));
            schema
        };

        let declared = build(TantivyFieldType::F64, TantivyFieldType::Text);
        let inferred = build(TantivyFieldType::I64, TantivyFieldType::I64);

        assert_ne!(
            declared.calculate_fingerprint(),
            inferred.calculate_fingerprint(),
            "same field names, different types — the thumbprint must not call these equal"
        );
    }

    /// Every property that changes how the index behaves moves the thumbprint.
    ///
    /// One case per property, because a thumbprint blind to any one of them cannot be used to
    /// detect a divergence in it. `routing_field_name` matters most: two nodes disagreeing about
    /// it route the same document to different shards.
    #[test]
    fn every_load_bearing_property_moves_the_thumbprint() {
        let base = || {
            let mut schema = IndexSchema::default();
            schema
                .fields
                .insert("f".into(), FieldDef::new("f".into(), TantivyFieldType::I64));
            schema
        };
        let baseline = base().calculate_fingerprint();

        let mut renamed = base();
        renamed
            .fields
            .insert("g".into(), FieldDef::new("g".into(), TantivyFieldType::I64));
        assert_ne!(baseline, renamed.calculate_fingerprint(), "field set");

        let mut retyped = base();
        retyped.fields.get_mut("f").unwrap().field_type = TantivyFieldType::U64;
        assert_ne!(baseline, retyped.calculate_fingerprint(), "field type");

        let mut unindexed = base();
        unindexed.fields.get_mut("f").unwrap().indexed = false;
        assert_ne!(baseline, unindexed.calculate_fingerprint(), "indexed");

        let mut stored = base();
        stored.fields.get_mut("f").unwrap().stored = !stored.fields["f"].stored;
        assert_ne!(baseline, stored.calculate_fingerprint(), "stored");

        let mut unfast = base();
        unfast.fields.get_mut("f").unwrap().fast = Some(false);
        assert_ne!(baseline, unfast.calculate_fingerprint(), "fast");

        let mut described = base();
        described.fields.get_mut("f").unwrap().description = Some("what it holds".into());
        assert_ne!(
            baseline,
            described.calculate_fingerprint(),
            "field description"
        );

        let mut tokenized = base();
        tokenized.fields.get_mut("f").unwrap().tokenizer = Some("raw".into());
        assert_ne!(baseline, tokenized.calculate_fingerprint(), "tokenizer");

        let mut recorded = base();
        recorded.fields.get_mut("f").unwrap().index_record_option = Some("Basic".into());
        assert_ne!(baseline, recorded.calculate_fingerprint(), "record option");

        let mut rerouted = base();
        rerouted.routing_field_name = "f".into();
        assert_ne!(baseline, rerouted.calculate_fingerprint(), "routing field");

        let mut titled = base();
        titled.description = Some("the corpus".into());
        assert_ne!(
            baseline,
            titled.calculate_fingerprint(),
            "index description"
        );
    }

    /// What the thumbprint must *not* see, or every node reports every other as divergent.
    ///
    /// `version` is excluded because the pair `(version, thumbprint)` is compared as a pair: a
    /// node holding matching content at a different version has to be able to recognise that.
    /// The timestamps are excluded because they are per-node and would never agree.
    #[test]
    fn the_thumbprint_ignores_version_and_timestamps() {
        let mut schema = IndexSchema::default();
        schema
            .fields
            .insert("f".into(), FieldDef::new("f".into(), TantivyFieldType::I64));
        let baseline = schema.calculate_fingerprint();

        schema.version = 47;
        schema.created_at = 1_600_000_000;
        schema.updated_at = 1_700_000_000;

        assert_eq!(
            baseline,
            schema.calculate_fingerprint(),
            "version and timestamps are not part of what a schema *is*"
        );
    }

    /// Free text is length-prefixed in the hash, so no two schemas collide by concatenation.
    ///
    /// The previous NUL-separated form was written for field names, which may not contain the
    /// separator; descriptions and tokenizer names can contain any byte at all.
    ///
    /// An absent description and an empty one hash the *same*, and that is the intended answer
    /// rather than a limit of the hash: `normalize_description` resolves whitespace-only text to
    /// absent, so the two are one effective schema and the engine cannot tell them apart either.
    /// The hash reports what a schema is, and an empty description is not a property.
    #[test]
    fn the_thumbprint_reads_free_text_unambiguously() {
        let build = |description: Option<&str>| {
            let mut schema = IndexSchema::default();
            let mut field = FieldDef::new("f".into(), TantivyFieldType::Text);
            field.description = description.map(str::to_string);
            schema.fields.insert("f".into(), field);
            schema.calculate_fingerprint()
        };

        assert_eq!(
            build(None),
            build(Some("   ")),
            "whitespace-only normalizes to absent, so it is the same schema"
        );
        assert_ne!(
            build(None),
            build(Some("what this field holds")),
            "a description that survives normalization is part of the schema"
        );

        // Two field names that concatenate to the same bytes must not collide.
        let pair = |a: &str, b: &str| {
            let mut schema = IndexSchema::default();
            for name in [a, b] {
                schema.fields.insert(
                    name.into(),
                    FieldDef::new(name.into(), TantivyFieldType::Text),
                );
            }
            schema.calculate_fingerprint()
        };
        assert_ne!(pair("ab", "c"), pair("a", "bc"), "name boundaries");
    }

    /// The same schema written two ways has one thumbprint.
    ///
    /// This is the property the whole mechanism rests on, and it did not hold. A node holding
    /// only a declaration leaves `tokenizer` unset; a node that has built the Tantivy index
    /// reads back the tokenizer the engine chose, because `get_schema_cached` merges the derived
    /// schema into the stored one. Found on a live cluster: an index declared on one node and
    /// written to on another reported `id` as `tokenizer: None` and `"raw"` respectively, so two
    /// nodes holding the same schema disagreed about its thumbprint permanently — the one
    /// answer a divergence check must never give.
    #[test]
    fn a_declaration_and_a_built_index_agree_on_the_thumbprint() {
        // As declared: nothing said about tokenizers or index options.
        let mut declared = IndexSchema::default();
        declared.fields.insert(
            "id".into(),
            FieldDef::new("id".into(), TantivyFieldType::Text),
        );
        declared.fields.insert(
            "title".into(),
            FieldDef::new("title".into(), TantivyFieldType::Text),
        );

        // As read back from a built index: the engine's choices are now spelled out.
        let mut built = declared.clone();
        {
            let id = built.fields.get_mut("id").unwrap();
            id.tokenizer = Some("raw".into());
            id.index_record_option = Some("Basic".into());
        }
        {
            let title = built.fields.get_mut("title").unwrap();
            title.tokenizer = Some("default".into());
            title.index_record_option = Some("WithFreqsAndPositions".into());
        }

        assert_eq!(
            declared.calculate_fingerprint(),
            built.calculate_fingerprint(),
            "a default spelled out is the same schema as a default left unsaid"
        );

        // And a tokenizer that is *not* the default still moves it — the point is to resolve
        // defaults, not to stop reading the property.
        let mut retokenized = declared.clone();
        retokenized.fields.get_mut("title").unwrap().tokenizer = Some("raw".into());
        assert_ne!(
            declared.calculate_fingerprint(),
            retokenized.calculate_fingerprint(),
            "a deliberate tokenizer choice is part of the schema"
        );
    }

    /// A local edit advances the version, whichever path made it.
    ///
    /// `version` shipped as dead metadata — set to 1 at construction and never incremented — so
    /// a schema could be edited repeatedly and still call itself version 1. A cluster ordering
    /// changes by version needs each edit to move it.
    #[test]
    fn a_local_edit_advances_the_version() {
        let mut schema = IndexSchema::default();
        let start = schema.version;

        // A field the schema has never seen — the ordinary write-driven evolution.
        assert!(schema.evolve_field("discovered".to_string(), &serde_json::json!(7)));
        let after_discovery = schema.version;
        assert!(
            after_discovery > start,
            "discovering a field left the version at {start}"
        );

        // Retyping an existing field. Only a non-indexed one can be retyped inline: an indexed
        // field's type is pinned to the column the index already built for it.
        let mut pending = FieldDef::new("pending".into(), TantivyFieldType::Text);
        pending.indexed = false;
        schema.fields.insert("pending".into(), pending);
        assert!(schema.evolve_field("pending".to_string(), &serde_json::json!(7)));
        let after_evolution = schema.version;
        assert!(
            after_evolution > after_discovery,
            "evolving a type left the version at {after_discovery}"
        );

        assert!(schema.add_shadow_field("shadow".into(), TantivyFieldType::Text));
        let after_shadow = schema.version;
        assert!(
            after_shadow > after_evolution,
            "adding a shadow field left the version at {after_evolution}"
        );

        let mut plain = FieldDef::new("later".into(), TantivyFieldType::Text);
        plain.indexed = false;
        schema.fields.insert("later".into(), plain);
        assert!(schema.promote_field_to_indexed("later"));
        assert!(
            schema.version > after_shadow,
            "promoting a field left the version at {after_shadow}"
        );

        // A no-op is not an edit and must not move it.
        let settled = schema.version;
        assert!(!schema.add_shadow_field("shadow".into(), TantivyFieldType::Text));
        assert!(!schema.promote_field_to_indexed("later"));
        assert_eq!(settled, schema.version, "a no-op advanced the version");
    }

    #[test]
    fn descriptions_round_trip_and_survive_field_evolution() {
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "description": "  Quarterly filings, one document per filing.  ",
            "fields": {
                "title": {"field_type": "text", "description": "Filing headline"},
                "year": {"field_type": "i64", "description": "   "},
                "notes": {"field_type": "text", "indexed": false},
            }
        }))
        .expect("schema with descriptions");
        schema.normalize_after_deserialization();

        assert_eq!(
            schema.description.as_deref(),
            Some("Quarterly filings, one document per filing."),
            "surrounding whitespace is not part of what someone wrote"
        );
        assert_eq!(
            schema.fields["year"].description, None,
            "blank is the same as unset, or every reader has to check for it"
        );
        assert_eq!(schema.fields["notes"].description, None);

        // Evolution rewrites a field's type; it must not rewrite what the field means. Asserted
        // on the descriptions themselves rather than through the fingerprint: the fingerprint
        // hashes types now, so it moves on any evolution and can no longer stand in for "nothing
        // else changed" — the thing this test is actually about.
        let descriptions = |schema: &IndexSchema| {
            let mut named: Vec<(String, Option<String>)> = schema
                .fields
                .iter()
                .map(|(name, def)| (name.clone(), def.description.clone()))
                .collect();
            named.sort();
            (schema.description.clone(), named)
        };
        let before = descriptions(&schema);
        assert!(schema.evolve_field("notes".to_string(), &serde_json::json!(7)));
        assert_eq!(
            schema.fields["title"].description.as_deref(),
            Some("Filing headline")
        );
        assert_eq!(
            descriptions(&schema),
            before,
            "evolution changed a type, so no description may have moved with it"
        );

        // And a round trip through the stored form keeps both, while an index that describes
        // nothing serialises no description keys at all.
        let encoded = serde_json::to_value(&schema).expect("serialise");
        let decoded: IndexSchema = serde_json::from_value(encoded).expect("deserialise");
        assert_eq!(decoded.description, schema.description);
        assert_eq!(
            decoded.fields["title"].description.as_deref(),
            Some("Filing headline")
        );

        let bare = serde_json::to_value(IndexSchema::default()).expect("serialise default");
        assert!(
            bare.get("description").is_none(),
            "an undescribed index must not pay for the field: {bare}"
        );
    }

    /// An indexed field's type is pinned to the column the index built for it. Evolving it would
    /// write the new type against the old column and silently drop values; the inline evolution
    /// path must never do that.
    #[test]
    fn an_indexed_fields_type_is_pinned_to_its_column() {
        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "n".to_string(),
            FieldDef::new("n".to_string(), TantivyFieldType::I64),
        );
        assert!(
            schema.fields["n"].indexed,
            "a declared field is indexed by default"
        );

        // A float cannot retype an indexed i64 field (the value would be silently unindexed).
        assert!(
            !schema.evolve_field("n".to_string(), &serde_json::json!(4.5)),
            "an indexed field must not evolve its type"
        );
        assert_eq!(
            schema.fields["n"].field_type,
            TantivyFieldType::I64,
            "the declared type survives"
        );

        // A non-indexed field still evolves freely, since no column is pinned yet.
        let mut non_indexed = IndexSchema::default();
        non_indexed.fields.insert(
            "n".to_string(),
            FieldDef::new_non_indexed("n".to_string(), &serde_json::json!(1)),
        );
        assert!(
            non_indexed.evolve_field("n".to_string(), &serde_json::json!(4.5)),
            "a non-indexed field widens to a float"
        );
        assert_eq!(non_indexed.fields["n"].field_type, TantivyFieldType::F64);
    }

    /// The limits exist because a catalogue listing carries every index's description at once.
    #[test]
    fn an_over_long_description_is_refused_by_name() {
        let mut schema = IndexSchema {
            description: Some("x".repeat(MAX_INDEX_DESCRIPTION_CHARS + 1)),
            ..Default::default()
        };
        let err = schema.validate_descriptions().expect_err("must be refused");
        assert!(
            err.contains("index description")
                && err.contains(&MAX_INDEX_DESCRIPTION_CHARS.to_string()),
            "the refusal must say what was too long and by what measure: {err}"
        );

        schema.description = Some("x".repeat(MAX_INDEX_DESCRIPTION_CHARS));
        assert!(
            schema.validate_descriptions().is_ok(),
            "the limit is inclusive"
        );

        for name in ["zebra", "apple"] {
            let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
            def.description = Some("x".repeat(MAX_FIELD_DESCRIPTION_CHARS + 1));
            schema.fields.insert(name.to_string(), def);
        }
        let err = schema.validate_descriptions().expect_err("must be refused");
        assert!(
            err.starts_with("description for field 'apple'"),
            "fields live in a HashMap, so the one named has to be chosen in a stable order or \
             the same schema is refused differently each time: {err}"
        );
        assert!(
            err.contains("1 other field"),
            "an operator fixing one at a time needs to know there are more: {err}"
        );

        // A multi-byte description gets the same allowance as an ASCII one.
        let schema = IndexSchema {
            description: Some("é".repeat(MAX_INDEX_DESCRIPTION_CHARS)),
            ..Default::default()
        };
        assert!(
            schema.validate_descriptions().is_ok(),
            "the limit counts characters, not bytes"
        );
    }

    #[test]
    fn test_normalize_date_comparisons() {
        // Single-char operators should normalize the date
        assert_eq!(
            normalize_date_comparisons("created:>2026-01-14", "created"),
            "created:>2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:<2026-01-14", "created"),
            "created:<2026-01-14T00:00:00Z"
        );

        // Compound operators >= and <= must also normalize the date
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14", "created"),
            "created:>=2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:<=2026-01-14", "created"),
            "created:<=2026-01-14T00:00:00Z"
        );

        // Already RFC3339 should pass through unchanged
        assert_eq!(
            normalize_date_comparisons("created:>2026-01-14T00:00:00Z", "created"),
            "created:>2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14T00:00:00Z", "created"),
            "created:>=2026-01-14T00:00:00Z"
        );

        // Non-date field should not be touched
        assert_eq!(
            normalize_date_comparisons("count:>20", "created"),
            "count:>20"
        );

        // Mixed query with date comparison and other terms
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14 AND status:active", "created"),
            "created:>=2026-01-14T00:00:00Z AND status:active"
        );
    }
}

/// The commit policy with `commit_interval_ms` set: time decides, the count is a backstop, and
/// the interval runs from the oldest write still waiting.
#[cfg(test)]
mod commit_interval_tests {
    use super::*;
    use tempfile::TempDir;

    const BATCH: usize = 100;
    const INTERVAL_MS: u64 = 150;

    /// A count threshold of exactly `BATCH`: every name here is new, so its budget is the
    /// minimum and the budget-scaled threshold is the batch size itself.
    fn config(temp_dir: &TempDir, commit_interval_ms: u64) -> StorageConfig {
        StorageConfig {
            max_open_indexes: 0,
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: BATCH,
            wal_sync: false,
            commit_interval_ms,
            query: Default::default(),
        }
    }

    fn put(id: &str) -> WalOp {
        WalOp::Put {
            id: id.to_string(),
            json_blob: Some(serde_json::json!({"id": id, "title": "commit cadence"})),
        }
    }

    fn past_the_interval() {
        std::thread::sleep(Duration::from_millis(INTERVAL_MS + 50));
    }

    /// Crossing the count no longer commits: that is what made a bulk load commit on nearly
    /// every drain. Waiting out the interval does, with any amount pending — which is what
    /// bounds a trickle that would never have reached the count.
    #[test]
    fn with_an_interval_time_commits_and_the_count_does_not() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(config(&temp_dir, INTERVAL_MS), 1).expect("store");

        assert!(
            !store.should_commit_writer("cadence", 1),
            "the first pending operation starts the clock; it does not commit"
        );
        assert!(
            !store.should_commit_writer("cadence", BATCH as u64 * 5),
            "past the count but inside the interval: not yet"
        );
        past_the_interval();
        assert!(
            store.should_commit_writer("cadence", 1),
            "an interval has passed since the oldest pending operation"
        );
        assert!(
            !store.should_commit_writer("cadence", 0),
            "nothing pending is never a commit, however long it has been"
        );
    }

    /// The backstop: twenty times the count commits at once, so a load faster than one interval
    /// can cover still bounds the WAL tail a restart would replay.
    #[test]
    fn the_backstop_commits_without_waiting_for_the_clock() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(config(&temp_dir, INTERVAL_MS), 1).expect("store");
        let backstop = BATCH as u64 * COMMIT_BACKSTOP_MULTIPLE;

        assert!(!store.should_commit_writer("burst", backstop - 1));
        assert!(store.should_commit_writer("burst", backstop));
    }

    /// `0` is the count-only policy, unchanged: the threshold commits and the clock is not read.
    #[test]
    fn a_zero_interval_keeps_the_count_only_policy() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(config(&temp_dir, 0), 1).expect("store");

        assert!(!store.should_commit_writer("count", BATCH as u64 - 1));
        assert!(store.should_commit_writer("count", BATCH as u64));
        assert!(
            store.pending_since.is_empty(),
            "the count-only policy keeps no clock"
        );
    }

    /// End to end through the write path: a write waits, a write an interval later commits both
    /// and makes them searchable, and the commit restarts the clock for the next one — so a
    /// steady trickle commits about once an interval, not once per write.
    #[test]
    fn a_trickle_is_committed_once_an_interval_has_passed() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = HybridStore::new(config(&temp_dir, INTERVAL_MS), 1).expect("store");
        let index = "trickle";

        let (_, committed) = store
            .apply_write_and_maybe_commit(index, put("a"))
            .expect("write a");
        assert!(
            !committed,
            "one write, far below the count, inside the interval"
        );

        past_the_interval();
        let (_, committed) = store
            .apply_write_and_maybe_commit(index, put("b"))
            .expect("write b");
        assert!(committed, "the oldest pending write has waited an interval");
        assert_eq!(store.get_operations_count(index), 0);
        assert_eq!(
            store
                .get_document_count_from_tantivy(index)
                .expect("count from the index"),
            2,
            "the commit made both writes searchable"
        );

        let (_, committed) = store
            .apply_write_and_maybe_commit(index, put("c"))
            .expect("write c");
        assert!(
            !committed,
            "the commit restarted the clock; a write right after it waits its own interval"
        );
    }
}

#[cfg(test)]
mod schema_cache_tests {
    use super::*;

    /// A schema loaded on a cache miss never replaces one a committed write cached while the
    /// load was running: the loaded one read the row before that write, so it is the older.
    #[test]
    fn a_loaded_schema_does_not_overwrite_one_cached_meanwhile() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),
            ..StorageConfig::default()
        };
        let store = HybridStore::new(config, 1).expect("store");

        let newer = IndexSchema {
            version: 5,
            ..IndexSchema::default()
        };
        store
            .schema_cache
            .insert("idx".to_string(), Arc::new(newer));

        let stale = IndexSchema {
            version: 1,
            ..IndexSchema::default()
        };
        let cached = store.cache_loaded_schema("idx", stale);
        assert_eq!(
            cached.version, 5,
            "the load returns what is cached, not what it read"
        );
        assert_eq!(
            store.schema_cache.get("idx").expect("entry").version,
            5,
            "and leaves the newer schema in place"
        );
    }
}

/// The shapes a date field's value may be written in, and the ones it may not.
#[cfg(test)]
mod date_shape_tests {
    use crate::schema::{FieldDef, TantivyFieldType, parse_date_to_timestamp_secs};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;

    fn at(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> Option<i64> {
        let date = NaiveDate::from_ymd_opt(y, m, d)?.and_hms_opt(h, min, s)?;
        Some(Utc.from_utc_datetime(&date).timestamp())
    }

    /// A slash date with the year last is American: month, then day. Always, so one file's dates
    /// are never read two ways.
    #[test]
    fn a_slash_date_is_read_month_first() {
        assert_eq!(
            parse_date_to_timestamp_secs("03/04/2024"),
            at(2024, 3, 4, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("3/4/2024"),
            at(2024, 3, 4, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("03/15/2024"),
            at(2024, 3, 15, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("03/15/2024 16:13:13"),
            at(2024, 3, 15, 16, 13, 13)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("03/15/2024 16:13"),
            at(2024, 3, 15, 16, 13, 0)
        );
    }

    /// A dotted date with the year last is European: day, then month. Always, as slashes are
    /// always month first.
    #[test]
    fn a_dotted_date_is_read_day_first() {
        assert_eq!(
            parse_date_to_timestamp_secs("03.04.2024"),
            at(2024, 4, 3, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("15.03.2024"),
            at(2024, 3, 15, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("5.3.2024"),
            at(2024, 3, 5, 0, 0, 0)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("15.03.2024 16:13:13"),
            at(2024, 3, 15, 16, 13, 13)
        );
        assert_eq!(
            parse_date_to_timestamp_secs("15.03.2024 16:13"),
            at(2024, 3, 15, 16, 13, 0)
        );
    }

    /// A value only the separator's other order can read is refused, not read that way: taking
    /// `15/03/2024` day first would read `03/04/2024` in the same column the other way.
    #[test]
    fn a_date_only_the_other_order_can_read_is_refused() {
        assert_eq!(parse_date_to_timestamp_secs("15/03/2024"), None);
        assert_eq!(parse_date_to_timestamp_secs("03.15.2024"), None);
    }

    /// chrono takes `%Y` from any number of digits; a two-digit year is not the year 24.
    #[test]
    fn a_two_digit_year_is_not_read_as_the_first_century() {
        assert_eq!(parse_date_to_timestamp_secs("03/15/24"), None);
        assert_eq!(parse_date_to_timestamp_secs("Mar 15, 24"), None);
        assert_eq!(parse_date_to_timestamp_secs("15.03.24"), None);
        assert_eq!(parse_date_to_timestamp_secs("24-03-15"), None);
        // A year written out is a year, however early.
        assert!(parse_date_to_timestamp_secs("0476-09-04").is_some());
    }

    #[test]
    fn a_named_month_is_read_in_either_order() {
        let day = at(2024, 3, 15, 0, 0, 0);
        for written in [
            "Mar 15, 2024",
            "March 15, 2024",
            "Mar 15 2024",
            "15 Mar 2024",
            "15 March 2024",
        ] {
            assert_eq!(parse_date_to_timestamp_secs(written), day, "{written}");
        }
    }

    #[test]
    fn a_mail_header_date_is_read_with_its_offset() {
        assert_eq!(
            parse_date_to_timestamp_secs("Fri, 15 Mar 2024 18:13:13 +0200"),
            at(2024, 3, 15, 16, 13, 13)
        );
    }

    /// The shapes accepted before are read as they were.
    #[test]
    fn the_year_first_shapes_are_unchanged() {
        for (written, expected) in [
            ("2024-03-15", at(2024, 3, 15, 0, 0, 0)),
            ("2024/03/15", at(2024, 3, 15, 0, 0, 0)),
            ("2024.03.15", at(2024, 3, 15, 0, 0, 0)),
            ("20240315", at(2024, 3, 15, 0, 0, 0)),
            ("2024-03-15 16:13:13", at(2024, 3, 15, 16, 13, 13)),
            ("2024-03-15T16:13:13.123", at(2024, 3, 15, 16, 13, 13)),
            ("2024-03-15T18:13:13+02:00", at(2024, 3, 15, 16, 13, 13)),
            ("20240315161313", at(2024, 3, 15, 16, 13, 13)),
            ("1710519193", at(2024, 3, 15, 16, 13, 13)),
            ("2024-03", at(2024, 3, 1, 0, 0, 0)),
            ("2024", at(2024, 1, 1, 0, 0, 0)),
        ] {
            assert_eq!(parse_date_to_timestamp_secs(written), expected, "{written}");
        }
    }

    /// A value inference calls a date is one the writer indexes as one, and the other way round.
    #[test]
    fn inference_reads_dates_by_the_writers_rules() {
        for written in [
            "03/15/2024",
            "15.03.2024",
            "Mar 15, 2024",
            "Fri, 15 Mar 2024 16:13:13 +0000",
            "2024-03-15",
        ] {
            assert_eq!(
                FieldDef::infer_type_from_value(&json!(written)),
                TantivyFieldType::Date,
                "{written}"
            );
        }
        for written in ["15/03/2024", "03.15.2024", "03/15/24"] {
            assert_eq!(
                FieldDef::infer_type_from_value(&json!(written)),
                TantivyFieldType::Text,
                "{written}"
            );
        }
    }
}

/// The startup recovery fan-out and its single retry pass.
#[cfg(test)]
mod recovery_fanout_tests {
    use super::*;

    /// A thread that panics must still answer for its index: recorded failed, and
    /// no index comes back unrecorded — an unrecorded index used to sit in
    /// `Recovering` forever, absent from both outcome lists.
    #[test]
    fn a_panicking_recovery_thread_is_recorded_as_failed() {
        let indices = vec![
            "good".to_string(),
            "boom".to_string(),
            "also_good".to_string(),
        ];
        let results = HybridStore::recovery_fanout(&indices, &|name| {
            if name == "boom" {
                panic!("injected recovery panic");
            }
            Ok(())
        });

        assert_eq!(results.len(), 3, "every index is answered for exactly once");
        for name in ["good", "also_good"] {
            assert!(
                results.iter().any(|(n, ok)| n == name && *ok),
                "{name} should be recovered"
            );
        }
        assert!(
            results.iter().any(|(n, ok)| n == "boom" && !*ok),
            "a panicking thread counts as failed recovery"
        );
    }

    /// Pass-one failures are retried exactly once, pass-one recoveries not at all,
    /// and an index whose failure was an accident of boot pressure comes back
    /// without a write having to touch it.
    #[test]
    fn the_retry_pass_revisits_only_the_first_pass_failures() {
        let calls: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        let indices = vec![
            "stable".to_string(),
            "flaky".to_string(),
            "broken".to_string(),
        ];
        let (recovered, failed) = HybridStore::recover_with_a_retry(&indices, &|name| {
            let mut seen = calls.lock().unwrap();
            seen.push(name.to_string());
            let attempts = seen.iter().filter(|n| n.as_str() == name).count();
            drop(seen);
            match name {
                "flaky" if attempts == 1 => Err(StoreError::IndexNotFound(name.to_string())),
                "broken" => Err(StoreError::IndexNotFound(name.to_string())),
                _ => Ok(()),
            }
        });

        assert_eq!(
            failed,
            ["broken"],
            "only the permanent failure stays failed"
        );
        assert!(
            recovered.contains(&"stable".to_string()) && recovered.contains(&"flaky".to_string()),
            "retry brings the boot-pressure failure back: {recovered:?}"
        );

        let calls = calls.into_inner().unwrap();
        let count = |name: &str| calls.iter().filter(|n| n.as_str() == name).count();
        assert_eq!(count("stable"), 1, "a first-pass recovery is not retried");
        assert_eq!(count("flaky"), 2, "a first-pass failure gets its one retry");
        assert_eq!(count("broken"), 2);
    }

    #[test]
    fn no_failures_means_no_second_pass() {
        let calls = std::sync::Mutex::new(0usize);
        let (recovered, failed) =
            HybridStore::recover_with_a_retry(&["a".to_string(), "b".to_string()], &|_| {
                *calls.lock().unwrap() += 1;
                Ok(())
            });
        assert_eq!(recovered.len(), 2);
        assert!(failed.is_empty());
        assert_eq!(
            *calls.lock().unwrap(),
            2,
            "each index recovered exactly once, nothing retried"
        );
    }
}

/// What a schema change asks of an index already built: every column it changes, and a
/// different id — and nothing for what the built index serves as it is.
#[cfg(test)]
mod rebuild_changes_tests {
    use crate::{FieldDef, IndexSchema, TantivyFieldType};

    fn schema(fields: &[(&str, TantivyFieldType)]) -> IndexSchema {
        let mut schema = IndexSchema::default();
        for (name, field_type) in fields {
            schema.fields.insert(
                name.to_string(),
                FieldDef::new(name.to_string(), field_type.clone()),
            );
        }
        schema
    }

    fn listed(current: &IndexSchema, next: &IndexSchema) -> Vec<(String, bool)> {
        current
            .rebuild_changes(next)
            .into_iter()
            .map(|c| (c.what, c.conflicts))
            .collect()
    }

    #[test]
    fn a_change_to_a_built_column_is_listed() {
        let current = schema(&[
            ("score", TantivyFieldType::I64),
            ("mac", TantivyFieldType::Text),
            ("note", TantivyFieldType::Text),
        ]);
        let mut next = current.clone();
        next.fields.get_mut("score").unwrap().field_type = TantivyFieldType::F64;
        next.fields.get_mut("mac").unwrap().tokenizer = Some("raw".to_string());
        next.fields.remove("note");
        next.fields.insert(
            "added".to_string(),
            FieldDef::new("added".to_string(), TantivyFieldType::I64),
        );
        let changes = listed(&current, &next);
        let has = |what: &str, conflicts: bool| {
            changes
                .iter()
                .any(|(w, c)| w.starts_with(what) && *c == conflicts)
        };
        // The index would act against these until rebuilt…
        assert!(has("score: i64 → f64", true), "{changes:?}");
        assert!(has("mac: tokenizer default → raw", true), "{changes:?}");
        // …and merely lacks, or keeps, a column for these.
        assert!(has("note: no longer indexed", false), "{changes:?}");
        assert!(has("added: indexed as i64", false), "{changes:?}");
    }

    #[test]
    fn what_the_built_index_serves_as_it_is_is_not_listed() {
        let current = schema(&[("mac", TantivyFieldType::Text)]);
        let mut next = current.clone();
        next.description = Some("devices".to_string());
        next.fields.get_mut("mac").unwrap().description = Some("modem".to_string());
        // `None` and the default it resolves to are one declaration.
        next.fields.get_mut("mac").unwrap().tokenizer = Some("default".to_string());
        // Recording an id where none was is not a change of key.
        next.id_fields = vec!["mac".to_string()];
        assert!(listed(&current, &next).is_empty());

        let mut keyed = next.clone();
        keyed.id_fields = vec!["Hr".to_string(), "mac".to_string()];
        assert_eq!(
            listed(&next, &keyed),
            vec![("id: mac → Hr,mac".to_string(), true)]
        );
    }

    #[test]
    fn a_type_renamed_over_the_same_column_is_not_listed() {
        // `string` is a raw, `Basic` text column under another name, either way round.
        let exact = schema(&[("mac", TantivyFieldType::String)]);
        let mut raw = schema(&[("mac", TantivyFieldType::Text)]);
        let field = raw.fields.get_mut("mac").unwrap();
        field.tokenizer = Some("raw".to_string());
        field.index_record_option = Some("Basic".to_string());
        assert!(listed(&exact, &raw).is_empty());
        assert!(listed(&raw, &exact).is_empty());

        // A tokenizer a `string` field declares builds nothing: its column stays raw.
        let mut declared = exact.clone();
        declared.fields.get_mut("mac").unwrap().tokenizer = Some("default".to_string());
        assert!(listed(&exact, &declared).is_empty());

        // Any other analysis is a different column.
        let analysed = schema(&[("mac", TantivyFieldType::Text)]);
        assert_eq!(
            listed(&exact, &analysed),
            vec![("mac: string → text".to_string(), true)]
        );
        let mut positions = raw.clone();
        positions.fields.get_mut("mac").unwrap().index_record_option = None;
        assert_eq!(
            listed(&raw, &positions),
            vec![(
                "mac: index record option Basic → WithFreqsAndPositions".to_string(),
                true
            )]
        );
    }
}

mod learned_field_tests {
    use crate::{FieldDef, IndexSchema, TantivyFieldType};
    use serde_json::json;

    /// The join is the same whichever way round it is taken, so nodes that saw the same values
    /// in any order settle on one type.
    #[test]
    fn the_join_holds_both_and_does_not_depend_on_order() {
        use TantivyFieldType::*;
        for (a, b, joined) in [
            (I64, I64, I64),
            (I64, F64, F64),
            (U64, I64, F64),
            (I64, Text, Text),
            (Date, I64, Text),
            (Boolean, F64, Text),
            (String, Text, Text),
        ] {
            assert_eq!(a.widened(&b), joined, "{a:?} with {b:?}");
            assert_eq!(b.widened(&a), joined, "{b:?} with {a:?}");
        }
    }

    /// A learned field widens as values arrive and never narrows back: `text` refined to a number
    /// would refuse the values that made it text.
    #[test]
    fn a_learned_field_widens_and_never_narrows() {
        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "level".to_string(),
            FieldDef::new_learned("level".to_string(), TantivyFieldType::I64),
        );
        assert!(schema.evolve_field("level".to_string(), &json!(2.5)));
        assert_eq!(schema.fields["level"].field_type, TantivyFieldType::F64);
        assert!(schema.evolve_field("level".to_string(), &json!("x7")));
        assert_eq!(schema.fields["level"].field_type, TantivyFieldType::Text);
        assert!(!schema.evolve_field("level".to_string(), &json!(3)));
        assert_eq!(schema.fields["level"].field_type, TantivyFieldType::Text);
        assert!(!schema.fields["level"].indexed);
    }

    /// How a type was reached is not part of what it is: a learned field and the same field
    /// declared fingerprint alike.
    #[test]
    fn learned_is_not_in_the_fingerprint() {
        let mut learned = IndexSchema::default();
        learned.fields.insert(
            "n".to_string(),
            FieldDef::new_learned("n".to_string(), TantivyFieldType::F64),
        );
        let mut declared = learned.clone();
        declared.fields.get_mut("n").expect("n").learned = false;
        assert_eq!(
            learned.calculate_fingerprint(),
            declared.calculate_fingerprint()
        );
    }
}
