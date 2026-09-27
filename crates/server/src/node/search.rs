//! Read-side machinery: merge primitives, sort keys, projection, document validation,
//! and the reason accounting every search or fan-out response is assembled from.

use super::*;

use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use tracing::warn;
use uuid::Uuid;

// Re-export SortSpec and SortOrder from storage crate
use serde_json::Value as JsonValue;
use storage::{FieldDef, IndexSchema, TantivyFieldType};
pub use storage::{SortOrder, SortSpec};

/// The prefix a per-document reason carries: its place in the batch it was sent in.
pub(super) const DOCUMENT_PREFIX: &str = "document ";

/// Split `document 7: reason` into its position and its reason, if that is what it is.
pub(super) fn split_document_reason(error: &str) -> Option<(usize, &str)> {
    let rest = error.strip_prefix(DOCUMENT_PREFIX)?;
    let (position, reason) = rest.split_once(": ")?;
    Some((position.parse().ok()?, reason))
}

/// Restate reasons numbered against one batch in the numbering its caller used.
///
/// Whoever answered a batch numbered its reasons against the batch *it* received, which is not
/// the numbering the caller counts in: a peer answers about the slice forwarded to it, and a
/// micro-batch of an NDJSON stream answers about five hundred documents out of a file. A
/// position from the wrong numbering is worse than none, because it names a real item that is
/// fine.
///
/// Exactly `unwritten` reasons come back, which is what makes the answer addable — written plus
/// reasons is what was received. Reasons that name no item are attached to items they cost
/// rather than reported beside them, and a shortfall nobody explained is stated by
/// `unattributed` rather than left for the caller to find by subtracting.
///
/// The opposite imbalance — more reasons than items unwritten — cannot come from a peer running
/// this code, and the surplus is folded into the last reason rather than dropped. The count has
/// to hold for the answer to add up, but a reason someone took the trouble to send is the only
/// account of a failure that exists, and silently discarding it is how a real cause disappears.
pub(crate) fn renumber_reasons(
    reasons: &[String],
    positions: &[usize],
    label: &str,
    unwritten: usize,
    attribution: &str,
    unaccounted: impl Fn() -> String,
) -> Vec<String> {
    balance_reasons(
        reasons,
        unwritten,
        |reason| match split_document_reason(reason) {
            Some((theirs, why)) if theirs < positions.len() => {
                format!("{label} {}: {why}", positions[theirs])
            }
            // Not about one item, or numbered against a batch this is not: kept as what was
            // said, against an item it cost.
            _ => format!("{attribution}: {reason}"),
        },
        attribution,
        unaccounted,
    )
}

/// Exactly one reason per item that was not accounted for, whatever the reasons arrived as.
///
/// The counting half of [`renumber_reasons`], separated because there are two ways to name the
/// item a reason is about and only one way to make the count add up. A bulk write names
/// positions in the caller's batch; a bulk delete names ids, which mean the same thing on every
/// node and so need no renumbering at all. `restate` is the difference between them.
///
/// Two imbalances, both handled rather than trusted: too few reasons is padded with
/// `unexplained`, because a shortfall a caller has to find by subtracting is not an answer; too
/// many is folded onto the last reason, because a reason someone took the trouble to send is the
/// only account of a failure that exists.
pub(super) fn balance_reasons(
    reasons: &[String],
    unaccounted: usize,
    restate: impl Fn(&str) -> String,
    attribution: &str,
    unexplained: impl Fn() -> String,
) -> Vec<String> {
    let mut balanced: Vec<String> = Vec::with_capacity(unaccounted);
    let mut surplus: Vec<&str> = Vec::new();

    for reason in reasons {
        if balanced.len() == unaccounted {
            surplus.push(reason.as_str());
            continue;
        }
        balanced.push(restate(reason));
    }

    while balanced.len() < unaccounted {
        balanced.push(unexplained());
    }

    // More was said than there are items to say it against. Carried on the last reason so the
    // count still adds up and nothing said is lost.
    if !surplus.is_empty()
        && let Some(last) = balanced.last_mut()
    {
        last.push_str(&format!(
            " ({attribution} also reported: {})",
            surplus.join("; ")
        ));
    }

    balanced
}

/// What a peer's answer means for the documents this node forwarded to it.
///
/// One reason per document the peer did not write, which is what keeps the coordinating node's
/// own answer addable: `items_written` plus `errors` is `items_received`, on one node or five.
///
/// A peer numbers its reasons against the batch *it* received, so each is renumbered to the
/// position the caller used — a position from someone else's batch is worse than none, because
/// it names a real document that is fine. A reason in any other shape is about the request
/// rather than one document, so it is attached to the documents it cost rather than reported
/// beside them.
///
/// The last case is a peer whose numbers do not add up: fewer reasons than documents it failed
/// to write. It cannot happen between two nodes running this code, and it is stated rather than
/// left as a silent shortfall when it does, because a caller cannot act on a gap it has to infer
/// by subtracting.
pub(super) fn remote_rejections(
    node_id: Uuid,
    positions: &[usize],
    written: usize,
    reasons: &[String],
) -> Vec<String> {
    renumber_reasons(
        reasons,
        positions,
        "document",
        positions.len().saturating_sub(written),
        &format!("node {node_id}"),
        || {
            format!(
                "a document forwarded to node {node_id} was neither written nor refused: it \
                 reported {written} written of {} with {} reasons",
                positions.len(),
                reasons.len()
            )
        },
    )
}

/// What a peer's answer means for the ids this node forwarded to it, on a bulk delete.
///
/// The delete counterpart of [`remote_rejections`], and simpler for one reason: a delete's
/// reasons name ids, and an id means the same thing on every node. A position does not, which is
/// the whole reason the write path renumbers — so there is nothing to renumber here, and a reason
/// already about one of the forwarded ids is passed through as the peer said it.
///
/// A reason in any other shape is about the request rather than one id — "the shard did not take
/// the batch", say — so it is attributed to the node that said it and attached to an id it cost.
/// Which node matters there, and does not for an id-shaped reason: the caller's own local
/// failures read `{id}: why` too, and one flat list keyed by id is the answer, whichever node
/// handled the id.
pub(super) fn remote_delete_rejections(
    node_id: Uuid,
    ids: &[String],
    deleted: usize,
    reasons: &[String],
) -> Vec<String> {
    let forwarded: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let attribution = format!("node {node_id}");
    balance_reasons(
        reasons,
        ids.len().saturating_sub(deleted),
        |reason| match reason.split_once(": ") {
            Some((id, _)) if forwarded.contains(id) => reason.to_string(),
            _ => format!("{attribution}: {reason}"),
        },
        &attribution,
        || {
            format!(
                "an id forwarded to node {node_id} was neither deleted nor refused: it reported \
                 {deleted} deleted of {} with {} reasons",
                ids.len(),
                reasons.len()
            )
        },
    )
}

/// The borrowed pieces a scatter-gather search reads, whichever lane serves it: the actor's
/// own shard map or the engine's `ArcSwap` snapshot of the same map, the schema `load_schema`
/// resolved, and the fan-out bound both lanes carry. The gather body is written once against
/// this so the two paths cannot drift — same per-shard window, same failure accounting, same
/// merge order, same refusal sequence.
pub(super) struct ScatterCtx<'a> {
    pub(super) shards: &'a HashMap<Uuid, MicroshardActor>,
    pub(super) schema: &'a IndexSchema,
    pub(super) max_concurrent_shard_searches: usize,
}

impl ScatterCtx<'_> {
    /// Ask every shard for the whole window from the front — any of them may hold all of it —
    /// then merge the pages under the requested order and answer the caller's slice.
    pub(super) async fn gather(
        &self,
        index: &str,
        query: &str,
        window: SearchWindow,
        fields: Option<&[String]>,
        sort: Option<&SortSpec>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        let schema = self.schema;

        // The identifier travels under the shadow name on the way out, so the projection is
        // rewritten before it is checked or applied.
        let fields = fields.map(|list| normalize_projection_fields(schema, list));
        let fields = fields.as_deref();

        // Refuse a sort the index cannot answer before asking any shard: every shard would
        // fail the same way, and a scatter-gather reports that as a partial failure inside a
        // 200 rather than as the bad request it is.
        if let Some(refusal) = unsortable_sort_field(schema, sort) {
            return Err(refusal);
        }

        let shard_targets: Vec<(Uuid, MicroshardActor)> = self
            .shards
            .iter()
            .map(|(&shard_id, shard)| (shard_id, shard.clone()))
            .collect();
        let shard_results: Vec<_> =
            futures::stream::iter(shard_targets.into_iter().map(|(shard_id, shard)| {
                // Every shard is asked for the whole window from the front, because any of
                // them may hold all of it. The skip is applied once, below.
                let req = SearchRequest {
                    index: index.to_string(),
                    query: query.to_string(),
                    limit: Some(window.fetch_count()),
                    sort: sort.cloned(),
                };
                async move { (shard_id, shard.handle_search(req).await) }
            }))
            .buffer_unordered(self.max_concurrent_shard_searches.max(1))
            .collect()
            .await;

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        // Kept as values rather than formatted here: whether nothing ran at all, and whose fault
        // that was, is decided from the errors themselves once the gather is complete.
        let mut failures: Vec<(Uuid, OrchestratorError)> = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        // Every shard parses the same query string, so collect the distinct set.
        let mut discarded: Vec<String> = Vec::new();
        // Every shard runs the same sort against the same schema, so one shard reporting an
        // approximate order describes the whole answer. A shard with no built index reports
        // nothing, hence first-wins rather than agreement.
        let mut approximate_sort: Option<String> = None;
        // Every shard runs the node's one policy against the same schema, so one shard's account
        // of the default fields is the index's. First-wins, as for `approximate_sort`.
        let mut narrowed_default_fields: Option<storage::NarrowedDefaultFields> = None;
        // One shard is enough. Shards can hold different schemas for the same index, and a
        // query that one of them could not run at all is not answered by the ones that could.
        let mut emptied = false;
        for (shard_id, result) in shard_results {
            match result {
                Ok(r) => {
                    emptied |= r.emptied;
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    for note in r.discarded {
                        if !discarded.contains(&note) {
                            discarded.push(note);
                        }
                    }
                    approximate_sort = approximate_sort.or(r.approximate_sort);
                    narrowed_default_fields = narrowed_default_fields.or(r.narrowed_default_fields);
                    shard_success += 1;
                }
                Err(err) => {
                    warn!(%shard_id, error = %err, "Scatter search shard failed");
                    failures.push((shard_id, err));
                }
            }
        }

        // Nothing ran, so there is nothing to qualify: refuse rather than answer with the empty
        // page a partial outage would produce. This is where the one refusal the sort guard above
        // cannot make lands — `fast` is a declaration and the column is written from it when the
        // index is built, so a field declared fast after the fact has no column to order by, and
        // only the built index knows that.
        if let Some(refusal) = no_shard_answered(index, shard_success, &failures) {
            return Err(refusal);
        }
        let errors = shard_error_notes(&failures);

        // Order merged results: by the requested sort field when provided, otherwise by
        // score descending. Each shard already returned field-sorted results, so a global
        // re-sort here is required to interleave them correctly across shards.
        //
        // When sorting, stamp each hit with the normalized `SORT_KEY_FIELD` first (while
        // the full doc is still present) and key the sort on it. The metadata field
        // survives the field projection below and lets a downstream cross-node merge
        // re-order these results even if the sort field is not among the returned fields.
        if let Some(spec) = sort {
            stamp_sort_keys(&mut results, spec, schema);
        }
        order_shard_hits(&mut results, sort);
        let results: Vec<(Uuid, f32, JsonValue)> = window.apply(results);
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(_shard_id, score, mut doc)| {
                // Add metadata fields
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                }

                // Apply field projection if specified
                if let Some(field_list) = fields {
                    apply_field_projection(doc, field_list)
                } else {
                    doc
                }
            })
            .collect();
        let mut response = serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": window.limit,
            "offset": window.offset,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": self.shards.len(),
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
        });
        attach_shard_errors(&mut response, errors);
        // Refuse instead of answering. An emptied query ran as nothing, so the zero it
        // produces is not a negative result — reported as a 200 it cannot be told apart from
        // "no document matches", which is the same confusion an unrunnable sort caused.
        if emptied {
            return Err(OrchestratorError::UnrunnableQuery {
                notes: discarded.join("; "),
            });
        }

        discarded.extend(unknown_projection_fields(schema, fields));
        attach_discarded(&mut response, discarded);
        attach_approximate_sort(&mut response, approximate_sort);
        attach_narrowed_default_fields(&mut response, narrowed_default_fields);
        Ok(response)
    }
}

/// Apply field projection to a JSON document, keeping only specified fields.
/// Always preserves metadata fields (_score, _sort_key, etc.) that start with underscore.
///
/// User-specified fields are inserted first in the exact order given by the projection
/// list, so the response field order matches the user's `return` clause. Metadata fields
/// are appended afterwards. This guarantees a consistent field order whether or not a
/// sort is active — the internal `_sort_key` (if present) simply appears at the end and
/// is stripped by `strip_sort_keys` at the client boundary.
pub(super) fn apply_field_projection(doc: JsonValue, fields: &[String]) -> JsonValue {
    if let JsonValue::Object(mut map) = doc {
        let mut filtered = serde_json::Map::new();

        // Add requested fields first, in user-specified projection order
        for field in fields {
            if let Some(value) = map.remove(field) {
                filtered.insert(field.clone(), value);
            }
        }

        // Then append metadata fields (those starting with _)
        for (key, value) in map.iter() {
            if key.starts_with('_') {
                filtered.insert(key.clone(), value.clone());
            }
        }

        JsonValue::Object(filtered)
    } else {
        doc
    }
}

pub(super) fn hit_score(hit: &JsonValue) -> f64 {
    hit.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0)
}

/// Metadata field carrying the normalized sort value of a hit.
///
/// Injected by the shard-gather search paths (`engine_search` / `orch_search`) before
/// field projection runs, and consumed by every merge layer. Because it is `_`-prefixed
/// it survives `apply_field_projection` automatically, so cross-node merges can order
/// results even when the user's `return` projection excludes the sort field itself. It
/// is stripped from every hit at the client boundary (`route_and_handle`).
pub(super) const SORT_KEY_FIELD: &str = "_sort_key";

/// The refusal a scatter-gather owes its caller when not one shard could run the query.
///
/// A failed shard alongside a successful one is a partial answer, and reporting it as hits plus
/// `errors` is right: some of the data was read. With no successful shard there is no answer to
/// qualify — only a `200` whose empty `hits` array is indistinguishable from a search that ran
/// everywhere and matched nothing, with the reason in a key most callers never read.
///
/// The reasons are deduplicated and the shard ids dropped: every shard runs the same query
/// against the same schema, so they fail the same way, and repeating one reason per shard reads
/// as several different problems.
pub(super) fn no_shard_answered(
    index: &str,
    shard_success: usize,
    failures: &[(Uuid, OrchestratorError)],
) -> Option<OrchestratorError> {
    if shard_success > 0 || failures.is_empty() {
        return None;
    }

    // Every shard was shed — refused before it started, because the request's budget was spent
    // waiting for a read thread. Nothing about the request or the data is wrong: the node was
    // behind. So the answer is the shed itself, the `503` every other shed op answers, which
    // `ShedLog` counts rather than logging. Folded into `NoShardAnswered` it was a fault — a
    // `500` telling the client not to retry, and an `ERROR` line per search (ROADMAP M6,
    // session 4).
    if let Some(shed) = failures
        .iter()
        .map(|(_, err)| shed_again(err))
        .collect::<Option<Vec<_>>>()
        .and_then(|sheds| sheds.into_iter().next())
    {
        return Some(shed);
    }

    let mut reasons: Vec<String> = Vec::new();
    for (_, err) in failures {
        let reason = match err {
            OrchestratorError::Io(io) => io.to_string(),
            other => other.to_string(),
        };
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }

    Some(OrchestratorError::NoShardAnswered {
        index: index.to_string(),
        reasons: reasons.join("; "),
        // A shed shard never ran the query, so it says nothing about whose fault it was: the
        // shards that did run it decide. Counting it against the caller would turn a query the
        // caller must fix into a `500` whenever the node was also busy.
        caller_error: failures
            .iter()
            .filter(|(_, err)| shed_again(err).is_none())
            .all(|(_, err)| is_caller_error(err)),
    })
}

/// `err` again when it is shed work — refused before it started because the node was behind —
/// and `None` for anything else. The two kinds `AppError::from_route` answers as a shed.
fn shed_again(err: &OrchestratorError) -> Option<OrchestratorError> {
    match *err {
        OrchestratorError::ReadDeadlineExpired {
            waited_ms,
            budget_ms,
        } => Some(OrchestratorError::ReadDeadlineExpired {
            waited_ms,
            budget_ms,
        }),
        OrchestratorError::Overloaded {
            predicted_wait_ms,
            budget_ms,
        } => Some(OrchestratorError::Overloaded {
            predicted_wait_ms,
            budget_ms,
        }),
        _ => None,
    }
}

/// The per-shard failures as a response reports them, one line each, naming the shard.
pub(super) fn shard_error_notes(failures: &[(Uuid, OrchestratorError)]) -> Vec<String> {
    failures
        .iter()
        .map(|(shard_id, err)| format!("Shard {}: {}", shard_id, err))
        .collect()
}

/// Report the shards that could not be read, and only then.
///
/// Absent means every shard answered, which is what makes its presence worth reading — the same
/// rule the federated search follows for the indexes it could not reach. An `errors: []` on every
/// successful search teaches a caller to skip the key, which is precisely the habit that hides
/// the one response where it matters.
pub(super) fn attach_shard_errors(response: &mut JsonValue, errors: Vec<String>) {
    if errors.is_empty() {
        return;
    }
    if let Some(object) = response.as_object_mut() {
        object.insert(
            "errors".to_string(),
            JsonValue::Array(errors.into_iter().map(JsonValue::String).collect()),
        );
    }
}

/// Response key listing the clauses the query parser dropped.
///
/// Absent on a clean parse rather than present and empty, so a caller can test for presence.
pub(crate) const DISCARDED_CLAUSES_FIELD: &str = "_discarded_clauses";

/// Attach `DISCARDED_CLAUSES_FIELD` to a search response, if anything was discarded.
pub(super) fn attach_discarded(response: &mut JsonValue, discarded: Vec<String>) {
    if discarded.is_empty() {
        return;
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert(
            DISCARDED_CLAUSES_FIELD.to_string(),
            JsonValue::Array(discarded.into_iter().map(JsonValue::String).collect()),
        );
    }
}

/// Notes for projected fields the index does not have.
///
/// A projection drops an unknown field without complaint, so a keyword lifted out of prose —
/// `find tax return forms` — would otherwise answer with documents carrying no fields. Metadata
/// names and `shard_id` are added by the response itself and so are always projectable.
///
/// A sort is not noted here, because an unknown sort field is refused outright — see
/// [`unsortable_sort_field`]. A dropped projection still leaves an answer worth returning; a
/// dropped sort leaves the hits in an order the caller did not ask for.
///
/// Skipped entirely for an index whose schema is not yet known, where every field would look
/// unknown.
pub(super) fn unknown_projection_fields(
    schema: &IndexSchema,
    fields: Option<&[String]>,
) -> Vec<String> {
    if schema.fields.is_empty() {
        return Vec::new();
    }

    let known = |name: &str| name.starts_with('_') || schema.fields.contains_key(name);
    let note = |clause: &str, field: &str| {
        format!(
            "'{clause} {field}' names a field this index does not have, so the clause had no \
             effect; if it was meant as query text, quote it or drop the keyword"
        )
    };

    fields
        .unwrap_or_default()
        .iter()
        .filter(|field| !known(field))
        .map(|field| note("return", field))
        .collect()
}

/// The projection as the documents can answer it.
///
/// On an index with a shadow field the identifier travels under the shadow name and no hit
/// carries `id`, so a projection naming `id` is rewritten to the name the hits have. Everywhere
/// else the rewrite is identity: `document_key_field` is `id` on a plain index. Done once,
/// before the list is checked or applied, so `return id` on a shadow index finds the field
/// rather than reporting it missing and returning a document with nothing in it.
pub(super) fn normalize_projection_fields(schema: &IndexSchema, fields: &[String]) -> Vec<String> {
    let key = storage::document_key_field(schema);
    if key == "id" {
        return fields.to_vec();
    }
    fields
        .iter()
        .map(|field| {
            if field == "id" {
                key.clone()
            } else {
                field.clone()
            }
        })
        .collect()
}

/// Whether a value written under a name for the document key is that key.
///
/// The identifier itself is always a string — `DocPayload.id`, the redb key — while a body may
/// carry it as a number: a numeric primary key, an id read out of a column that was integral at
/// the source. So the identifier is read as a number too before those two are called a
/// disagreement, which is what keeps a body saying `"id": 42` beside an envelope saying `"42"`
/// from being refused. Any other JSON type is not an identifier and cannot name one.
pub(super) fn names_identifier(value: &JsonValue, id: &str) -> bool {
    match value {
        JsonValue::String(text) => text == id,
        JsonValue::Number(number) => id
            .parse::<serde_json::Number>()
            .is_ok_and(|written_as| &written_as == number),
        _ => false,
    }
}

/// Why a document cannot be stored under the identifier it arrived with, if it cannot.
///
/// The key travels beside the body, not inside it: `DocPayload.id` is the redb key, the term
/// tantivy indexes, and the value `reconstruct_shadow_fields_owned` writes back into every hit
/// on the way out. Keeping it in the stored blob as well would hold a second copy of the same
/// string, which is why the documented bulk shape — `{"id": "...", "doc": {...}}` — leaves it
/// out of `doc`, and why the read path treats the key as authoritative when the blob has no
/// `id` of its own. Validation reads the envelope for that reason: demanding `id` inside the
/// body refused every document written in the shape the API documents.
///
/// A body that does carry `id` is the older shape and still valid, but only while it agrees
/// with the envelope. Disagreement is refused for the same reason a disagreeing shadow field
/// is: the blob keeps its own `id` and reconstruction prefers it, so the document would answer
/// to the key it was stored under and report a different one to whoever found it.
pub(super) fn unusable_document_identity(id: &str, doc: &JsonValue) -> Option<String> {
    let obj = match doc.as_object() {
        Some(obj) => obj,
        None => return Some("Document body must be a JSON object".to_string()),
    };

    if id.is_empty() {
        return Some(
            "Document must be written with a non-empty 'id' beside its body: the identifier is \
             the key it is stored, updated and looked up under. The body itself does not have to \
             repeat it."
                .to_string(),
        );
    }

    let body_id = obj.get("id")?;
    (!names_identifier(body_id, id)).then(|| {
        format!(
            "document is written under id={id} but its body says id={body_id}. The identifier \
             beside the body is the key the document is stored under, so the two cannot differ: \
             a later read would find this document as {id} and report {body_id}. Leave 'id' out \
             of the body — it is supplied from the key — or correct one of the two."
        )
    })
}

/// Why a document's shadow fields cannot be stored as written, if they cannot.
///
/// A shadow field is the document key under the source's own name, and nothing holds a second
/// copy of the value: the write path strips the field out of the stored blob
/// (`filter_shadow_fields_owned`) and the read path writes the key back under it
/// (`reconstruct_shadow_fields_owned`). That round trip returns what was written only while the
/// two agree, so a document saying otherwise has that value silently and unrecoverably
/// discarded — it is refused here rather than accepted and destroyed.
///
/// The rest of the design already rests on this: `document_key_field` picks any one shadow name
/// to read the key back under and reconstruction writes the key under every one, both defensible
/// only if all of them mean the key. The bundled importer checks it before promoting a column to
/// shadow; this is the same rule for a write arriving by any other route.
///
/// Absence is not disagreement. A document may omit a shadow field entirely — the ordinary case
/// for a rewritten document — and reconstruction supplies it on the way out.
///
/// `id` is the identifier the write arrived with rather than anything read out of the body,
/// because a body need not carry `id` at all. Reading it from the body skipped this check
/// entirely on exactly the documents the documented bulk shape sends.
pub(super) fn disagreeing_shadow_field(
    doc: &JsonValue,
    schema: &IndexSchema,
    id: &str,
) -> Option<String> {
    if schema.shadow_fields.is_empty() {
        return None;
    }
    let obj = doc.as_object()?;

    // Sorted, so a document disagreeing under two names names the same one every time.
    let mut names: Vec<&String> = schema.shadow_fields.iter().collect();
    names.sort_unstable();

    names.into_iter().find_map(|name| {
        let value = obj.get(name)?;
        (!names_identifier(value, id)).then(|| {
            format!(
                "field '{name}' is a shadow of 'id' and must carry the same value, but this \
                 document has {name}={value} and id={id}. A shadow field is a name for the \
                 identifier rather than a field of its own, so nothing would store {value} and \
                 a later read would report {id} under '{name}'. Write the value under a \
                 different field name, or correct the identifier."
            )
        })
    })
}

/// The field type a value would be given if the schema had never seen the field.
///
/// `FieldDef::infer_type_from_value` is the one reading of this, shared with the storage layer's
/// own evolution and with the sampling that builds an index's first schema. It was two: this had
/// its own copy of the same match, and the two agreed only for as long as nobody edited one of
/// them — which is how a list came to be typed by its elements on a write to an existing field
/// and as text on the write that created the index.
///
/// It is deliberately not applied to `id`: the document key is text whatever it looks like, and
/// inferring `i64` from a numeric identifier is how an index came to declare `id` as a type the
/// key it builds does not use.
pub(super) fn infer_field_type(value: &JsonValue) -> TantivyFieldType {
    FieldDef::infer_type_from_value(value)
}

/// Whether a field declared as one type can hold a single value.
///
/// **A numeric field is asked the writer's own question**, not whether two type names match.
/// `add_json_value_to_doc` stores what `as_u64()`/`as_i64()`/`as_f64()` returns and skips the
/// value when one returns `None`, so that call *is* the definition of what the field can hold.
/// Comparing inferred names instead disagreed with it in both directions, because
/// `infer_type_from_value` reads every integer that fits in an `i64` as `I64`:
///
/// - a declared `u64` refused `2018` and `0` — every ordinary non-negative integer — and
///   accepted only what exceeds `i64::MAX`, while the writer's `as_u64()` would have stored
///   any of them.
/// - a declared `f64` refused `3`, `0` and `-5` — every whole number — and accepted only a
///   value written with a fraction, while `as_f64()` widens an integer to a float happily.
///
/// Both were refusals of documents the engine can hold, which is the mirror of the silent loss
/// this function exists to prevent, and both are gone now that the question is the writer's.
/// The refusals that matter still stand, because `as_*` returns `None` for exactly them: a
/// negative into a `u64`, a fraction into an integer, an integer past `i64::MAX` into an `i64`.
///
/// `String` is `Text` under an older name, and is the only widening left that is about names.
/// A type is never *changed* to fit a value — widening an already-built column needs a rebuild,
/// which no write path performs (see `IndexSchema::evolve_field`).
///
/// Values whose *shape* rather than type decides the answer — a list, a null, a text or json
/// field that takes anything, a facet path — are settled by `unstorable_value` before this is asked.
pub(super) fn scalar_type_is_storable(declared: &TantivyFieldType, value: &JsonValue) -> bool {
    match declared {
        TantivyFieldType::U64 => return value.as_u64().is_some(),
        TantivyFieldType::I64 => return value.as_i64().is_some(),
        TantivyFieldType::F64 => return value.as_f64().is_some(),
        _ => {}
    }

    let inferred = infer_field_type(value);
    if *declared == inferred {
        return true;
    }
    matches!(
        (declared, &inferred),
        (TantivyFieldType::String, TantivyFieldType::Text)
    )
}

/// Why the field cannot hold this value, if it cannot.
///
/// The question is what the write path can store, which is a wider question than whether two
/// type names match — see `storage::add_json_value_to_doc`, which is the other half of this
/// answer and has to agree with it exactly. Where they disagree the cost is silent: a value the
/// validator waves through and the writer skips leaves a document stored with that field
/// unindexed, and a query over the field simply never matches it.
///
/// **A list is several values of the field.** Every tantivy field is multivalued — two `add_i64`
/// calls under one field store two values of it, the fast column reports
/// `Cardinality::Multivalued`, and a range query matches the document if any one value falls
/// inside. So `{"risk_score": [9, 12]}` belongs in an `i64` field, and each element is checked
/// on its own. One level is flattened and no more, which is exactly what the writer does with
/// it: a list or an object *among* those elements is refused, because the writer has no `add_*`
/// call to make for one and would skip it.
///
/// **A text or json field takes anything**, because the writer serializes whatever it is given
/// into text. A list under one of those is indexed as its own JSON text rather than split, so
/// it is not looked into here either. **Bytes** wants a list of byte values and nothing else.
///
/// **Null is the absence of a value**, and any field may be absent. The writer adds nothing, no
/// query matches it, and the document reads back exactly as written — so refusing a document
/// for carrying an explicit null where it could have omitted the key would be a distinction
/// without a difference to anything downstream.
pub(super) fn unstorable_value(
    field: &str,
    declared: &TantivyFieldType,
    value: &JsonValue,
) -> Option<String> {
    if value.is_null() {
        return None;
    }

    match declared {
        // Takes the value whole, whatever shape it has.
        TantivyFieldType::Text | TantivyFieldType::Json => None,
        // Takes a list of byte values, and only that. Not routed through the element-wise
        // branch below, because a list means something different here: `[1, 2]` under an `i64`
        // is two values of the field, while under `bytes` it is one two-byte value. So every
        // element has to fit, and one that does not refuses the value rather than itself.
        TantivyFieldType::Bytes => match value.as_array() {
            Some(items) => items
                .iter()
                .find_map(storage::byte_value_error)
                .map(|why| format!("field '{field}': {why}")),
            None => Some(type_mismatch(field, declared, value)),
        },
        _ => match value.as_array() {
            // Several values of the field, each held to the declared type on its own.
            Some(items) => items
                .iter()
                .find_map(|item| unstorable_scalar(field, declared, item)),
            None => unstorable_scalar(field, declared, value),
        },
    }
}

/// Why the field cannot hold one value of it, if it cannot.
///
/// Split out so that a list and a scalar are judged identically: the writer flattens a list into
/// one `add_*` call per element, so an element the field cannot hold is exactly as unstorable in
/// a list of three as it is on its own.
///
/// A facet is the one type whose *value* rather than type decides the answer — `"/a/b"` and
/// `"a/b"` are both strings and only one is a path — so it is asked of the parser the writer
/// uses, `storage::facet_path_error`, rather than of `infer_field_type`, which reads every
/// facet path as text and would refuse the whole type.
///
/// A list or an object is refused before anything else is asked. `infer_field_type` reads a list
/// by what it holds — `[1, 2]` is `I64` — which is right for typing a field nobody declared and
/// wrong here: it made a *nested* list look like a value a numeric field accepts, where the
/// writer's `as_i64()` returns `None` and skips it, storing the document with the field
/// unindexed. That is the one outcome this function exists to prevent.
pub(super) fn unstorable_scalar(
    field: &str,
    declared: &TantivyFieldType,
    value: &JsonValue,
) -> Option<String> {
    if value.is_null() {
        return None;
    }

    let shape = match value {
        JsonValue::Array(_) => Some("a list"),
        JsonValue::Object(_) => Some("an object"),
        _ => None,
    };
    if let Some(shape) = shape {
        return Some(format!(
            "field '{field}': {shape} is not one value a {declared:?} field can hold. A list \
             under this field is several values of it, flattened one level — so the elements are \
             the values, and a list or an object among them is not one"
        ));
    }

    if matches!(declared, TantivyFieldType::Facet) {
        return match value.as_str() {
            Some(path) => {
                storage::facet_path_error(path).map(|why| format!("field '{field}': {why}"))
            }
            None => Some(type_mismatch(field, declared, value)),
        };
    }

    // A date is written or counted, and `infer_field_type` only knows the written form: it reads
    // a number as an integer, which the declared type does not match, so every timestamp sent as
    // epoch seconds — the shape most exporters emit — was refused.
    if matches!(declared, TantivyFieldType::Date) {
        return (!storage::is_date_value(value)).then(|| type_mismatch(field, declared, value));
    }

    (!scalar_type_is_storable(declared, value)).then(|| type_mismatch(field, declared, value))
}

/// The one wording for a value whose type the field cannot hold.
pub(super) fn type_mismatch(field: &str, declared: &TantivyFieldType, value: &JsonValue) -> String {
    format!(
        "Type mismatch for field '{field}': expected {declared:?}, got {:?}",
        infer_field_type(value)
    )
}

/// The sort field a search cannot be answered with, if the caller named one.
///
/// Why the engine will refuse to order by this field, if it will.
///
/// A sort fails in every shard at once or in none of them, and scatter-gather reports the first
/// as a partial outage: 200, an empty `hits` array, and the reason buried in per-shard `errors`,
/// which reads as "nothing matched". So a refusal the engine is certain to issue is issued here,
/// before the fan-out, where it can be an error about the request.
///
/// The question asked is the engine's own — "can I order by this column?" — not the narrower
/// "does a column of this name exist". Both refusals reach the caller identically, so checking
/// only for the name lets the other kind through.
///
/// What may be sorted on:
/// - a field with a fast column, which the collector orders by.
/// - a text or string field without one, which is ordered after the fetch. Approximate rather
///   than refused, and [`APPROXIMATE_SORT_FIELD`] says so.
/// - `id`, which every Tantivy schema carries and no `IndexSchema` lists.
/// - a shadow field, which is the caller's name for `id`. The engine sorts by the key's column
///   exactly as it queries by it, so both names order by the same values.
///
/// What may not: a name absent from the schema, `_seq`, or a non-text field without a fast
/// column. `_`-prefixed names are not waved through here the way [`unknown_projection_fields`]
/// waves them through: a projection asking for response metadata is meaningful, a sort on it is
/// not.
///
/// Decided from the declared schema, which is what a router holds without opening an index. That
/// leaves one refusal undecidable here: `fast` is a declaration and the column is written from it
/// at index time, so a numeric field declared `fast` after its index was built has no column to
/// order by and only the built index knows. Every other refusal the engine can reach is reachable
/// from the declaration.
///
/// Skipped entirely for an index whose schema is not known yet, where every field would look
/// unknown.
pub(super) fn unsortable_sort_field(
    schema: &IndexSchema,
    sort: Option<&SortSpec>,
) -> Option<OrchestratorError> {
    let field = &sort?.field;
    if schema.fields.is_empty() {
        return None;
    }

    // The document key, under its own name or under a shadow name that stands for it.
    if field == "id" || schema.is_shadow_field(field) {
        return None;
    }

    let refuse = |reason: String| {
        Some(OrchestratorError::UnsortableField {
            field: field.clone(),
            reason,
        })
    };

    // Retired, and so present only in the schema record of an index built before it was retired
    // — which is why its absence from the record cannot be what refuses it. Every field listing
    // hides it, and the engine refuses it in each shard.
    if field == "_seq" {
        return refuse("the field is internal and no index reports it".to_string());
    }

    let Some(def) = schema.fields.get(field) else {
        return refuse("the index has no column of that name".to_string());
    };

    if def.is_fast()
        || matches!(
            def.field_type,
            TantivyFieldType::Text | TantivyFieldType::String
        )
    {
        return None;
    }

    // Two different refusals, because a caller can act on one of them and not the other. A
    // numeric or date field is one schema edit and a rebuild away from sorting; a boolean, bytes,
    // ip, json or facet field is never getting a column, since the index builder adds those types
    // without reading `fast` at all. Telling the second kind to "declare it fast" sends the caller
    // to make an edit that changes nothing — and one that used to *look* like it worked, because
    // the declaration was reported back as `true` while the guard waved the sort through to fail
    // in every shard.
    if !FieldDef::can_be_fast(&def.field_type) {
        return refuse(format!(
            "a {} field has no column to sort on, and declaring it fast cannot give it one",
            def.field_type.to_string()
        ));
    }

    refuse(format!(
        "a {} field must be declared fast to sort, and this one is not",
        def.field_type.to_string()
    ))
}

/// Collect the distinct dropped clauses from per-node responses.
///
/// Cross-node merges see [`DISCARDED_CLAUSES_FIELD`] as JSON rather than as a typed reply.
pub(super) fn collect_discarded(responses: &[JsonValue]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for response in responses {
        let Some(notes) = response
            .get(DISCARDED_CLAUSES_FIELD)
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        for note in notes.iter().filter_map(|note| note.as_str()) {
            if !out.iter().any(|existing| existing == note) {
                out.push(note.to_string());
            }
        }
    }
    out
}

/// Response key naming the field whose sort order is an approximation.
///
/// Absent when the order is exact, which is the common case — present means the hits are in the
/// alphabetical order of a *sample* of the matches rather than of all of them. See
/// [`storage::SearchOutcome::approximate_sort`].
///
/// Carries the field name rather than `true`, because the caller's next move is to look that
/// field up in the schema and see that it has no fast column.
pub(crate) const APPROXIMATE_SORT_FIELD: &str = "_approximate_sort";

/// Attach [`APPROXIMATE_SORT_FIELD`] to a search response, if the order returned is approximate.
pub(super) fn attach_approximate_sort(response: &mut JsonValue, field: Option<String>) {
    let Some(field) = field else {
        return;
    };
    if let Some(obj) = response.as_object_mut() {
        obj.insert(APPROXIMATE_SORT_FIELD.to_string(), JsonValue::String(field));
    }
}

/// The approximated field from per-node responses, if any node reported one.
///
/// One field, not a list: every node ran the same sort on the same field, so either that field
/// has a fast column everywhere or it has one nowhere. A node whose shards are all empty reports
/// nothing at all, which is why the first answer wins rather than requiring agreement.
pub(super) fn collect_approximate_sort(responses: &[JsonValue]) -> Option<String> {
    responses.iter().find_map(|response| {
        response
            .get(APPROXIMATE_SORT_FIELD)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    })
}

/// The response key saying an unqualified term searched fewer default fields than the index has,
/// because the node's `max_default_fields` capped them. See
/// [`storage::SearchOutcome::narrowed_default_fields`].
///
/// Carries the fields searched rather than `true`, because the caller's next move is to see
/// whether the field it cares about is among them, and to name it if it is not.
pub(crate) const NARROWED_DEFAULT_FIELDS: &str = "_narrowed_default_fields";

/// Attach [`NARROWED_DEFAULT_FIELDS`] to a search response, if the default fields were narrowed.
pub(super) fn attach_narrowed_default_fields(
    response: &mut JsonValue,
    narrowed: Option<storage::NarrowedDefaultFields>,
) {
    let Some(narrowed) = narrowed else {
        return;
    };
    if let (Some(obj), Ok(value)) = (response.as_object_mut(), serde_json::to_value(narrowed)) {
        obj.insert(NARROWED_DEFAULT_FIELDS.to_string(), value);
    }
}

/// The narrowing from per-node responses, if any node reported one. First-wins, for the reason
/// [`collect_approximate_sort`] gives: the nodes ran one query against one index.
pub(super) fn collect_narrowed_default_fields(
    responses: &[JsonValue],
) -> Option<storage::NarrowedDefaultFields> {
    responses.iter().find_map(|response| {
        response
            .get(NARROWED_DEFAULT_FIELDS)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    })
}

/// Produce a comparable sort key for a hit's raw field value.
///
/// Date fields are keyed by the epoch second their fast column holds, so that cross-node merges
/// order them chronologically (matching each shard's FAST-field ordering) rather than by
/// lexicographic string comparison, which breaks across mixed date formats/offsets — and read
/// in every shape the writer indexes, epoch seconds included. Every other value passes through
/// unchanged — the merge comparator handles the numeric-vs-string distinction. Returns `None`
/// when the value cannot be keyed (e.g. an unparseable date string), in which case the hit
/// sorts last.
pub(super) fn normalize_sort_key(
    value: &JsonValue,
    field_def: Option<&FieldDef>,
) -> Option<JsonValue> {
    if let Some(def) = field_def
        && matches!(def.field_type, TantivyFieldType::Date)
    {
        return storage::date_sort_secs(value).map(|ts| JsonValue::Number(ts.into()));
    }
    Some(value.clone())
}

/// Attach the `SORT_KEY_FIELD` metadata value to each gathered hit, in place, so that
/// downstream merges (local multi-shard and cross-node) have a projection-independent
/// key to order by. No-op for hits lacking the sort field or an unparseable date.
///
/// The key is read under the name the *document* carries, which is not always the name the
/// caller sorted by. A sort on the document key is the case: an index with shadow fields
/// answers with the shadow name in place of `id`, so `sort=id` and `sort=<shadow>` both have to
/// look for whichever of the two is on the hit. Reading only the caller's name leaves every hit
/// unstamped, and an unstamped merge keeps each shard's block whole — a per-shard order
/// presented as a global one.
pub(super) fn stamp_sort_keys(
    hits: &mut [(Uuid, f32, JsonValue)],
    spec: &SortSpec,
    schema: &IndexSchema,
) {
    let field_def = schema.fields.get(&spec.field);
    let sorts_by_document_key = spec.field == "id" || schema.is_shadow_field(&spec.field);
    let fallback = sorts_by_document_key.then(|| storage::document_key_field(schema));

    for (_, _, doc) in hits.iter_mut() {
        if let JsonValue::Object(o) = doc
            && let Some(raw) = o
                .get(&spec.field)
                .or_else(|| fallback.as_deref().and_then(|name| o.get(name)))
                .or_else(|| sorts_by_document_key.then(|| o.get("id")).flatten())
            && let Some(key) = normalize_sort_key(raw, field_def)
        {
            o.insert(SORT_KEY_FIELD.to_string(), key);
        }
    }
}

/// Remove the internal `SORT_KEY_FIELD` from every hit in a search response, in place.
/// Called once at the client boundary so the key never leaks to callers.
pub(super) fn strip_sort_keys(response: &mut JsonValue) {
    if let Some(hits) = response.get_mut("hits").and_then(|h| h.as_array_mut()) {
        for hit in hits.iter_mut() {
            if let Some(o) = hit.as_object_mut() {
                o.remove(SORT_KEY_FIELD);
            }
        }
    }
}

/// Compare two hit documents by a named field for field-sorted search merges.
///
/// Integer values are compared as `i64` first (so keys beyond f64's exact-integer
/// range, e.g. large ids or nanosecond timestamps, order precisely); otherwise values
/// are compared as `f64`, then fall back to string comparison. Documents missing the
/// field always sort last, regardless of the requested order.
pub(super) fn compare_hits_by_field(
    a: &JsonValue,
    b: &JsonValue,
    field: &str,
    order: SortOrder,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a.get(field), b.get(field)) {
        (Some(x), Some(y)) => {
            let base = match (x.as_i64(), y.as_i64()) {
                (Some(nx), Some(ny)) => nx.cmp(&ny),
                _ => match (x.as_f64(), y.as_f64()) {
                    (Some(nx), Some(ny)) => nx.partial_cmp(&ny).unwrap_or(Ordering::Equal),
                    _ => match (x.as_str(), y.as_str()) {
                        (Some(sx), Some(sy)) => sx.cmp(sy),
                        _ => Ordering::Equal,
                    },
                },
            };
            match order {
                SortOrder::Asc => base,
                SortOrder::Desc => base.reverse(),
            }
        }
        // Present values sort before missing ones, independent of order.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Compare two hits by what the caller asked to order on.
///
/// Relevance score descending by default — the engine's own ranking, and the order in which it
/// hands hits back — or the injected `SORT_KEY_FIELD` when a sort was requested. Keyed on that
/// metadata field rather than on the sort field itself, because projection may have removed
/// the latter from the hit.
pub(super) fn compare_hits_primary(
    a: &JsonValue,
    b: &JsonValue,
    sort: Option<&SortSpec>,
) -> std::cmp::Ordering {
    match sort {
        Some(spec) => compare_hits_by_field(a, b, SORT_KEY_FIELD, spec.order),
        None => hit_score(b)
            .partial_cmp(&hit_score(a))
            .unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// Order one node's shard hits, deterministically.
///
/// The tuple form of [`order_hit_blocks`], for the scatter paths that hold a shard id and a
/// score beside each document rather than a finished hit. Shards are polled concurrently and
/// answer in whatever order they finish, so a tie falls back to the shard's id — fixed when the
/// shard was created — and then to the hit's place in that shard's own ordering, which Tantivy
/// has already made total. `results` holds each shard's hits contiguously, so a comparison of
/// positions within one shard is a comparison within its block.
pub(super) fn order_shard_hits(results: &mut Vec<(Uuid, f32, JsonValue)>, sort: Option<&SortSpec>) {
    let mut ranked: Vec<(usize, (Uuid, f32, JsonValue))> =
        std::mem::take(results).into_iter().enumerate().collect();

    ranked.sort_by(|(left_position, left), (right_position, right)| {
        let primary = match sort {
            Some(spec) => compare_hits_by_field(&left.2, &right.2, SORT_KEY_FIELD, spec.order),
            None => right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal),
        };
        primary
            .then_with(|| left.0.cmp(&right.0))
            .then_with(|| left_position.cmp(right_position))
    });

    *results = ranked.into_iter().map(|(_, tuple)| tuple).collect();
}

/// The slice of an ordered result a caller asked for.
///
/// Kept as one value rather than two loose numbers because the two are not independent, and the
/// relationship between them is the whole of paging: see [`SearchWindow::fetch_count`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchWindow {
    /// How many ordered hits to discard before the first one returned.
    pub(crate) offset: usize,
    /// How many to return after that.
    pub(crate) limit: usize,
}

impl SearchWindow {
    /// The first `limit` hits — what every caller that does not page asks for.
    pub(super) fn first(limit: usize) -> Self {
        SearchWindow { offset: 0, limit }
    }

    /// Resolve a request's `limit` and `offset` into a window, or say why it cannot be served.
    ///
    /// Every request surface goes through here, so that all of them apply the node's default and
    /// its ceiling the same way. Two things it settles that a per-surface check kept getting
    /// wrong:
    ///
    /// An absent `limit` means the node's default, not zero. Bounding `offset + 0` lets
    /// `offset = max_search_limit` past a check the engine then exceeds by the default, so the
    /// advertised ceiling was not the real one.
    ///
    /// The ceiling applies to `offset + limit` rather than to `limit`, because that sum is what
    /// gets fetched: every source is asked for the whole window from the front (see
    /// [`Self::fetch_count`]), and Tantivy's collector allocates against the number it is given
    /// before it has matched anything. So a deep page is exactly as expensive as a large limit,
    /// and `max_search_limit` has to bound both or it bounds neither.
    pub(crate) fn checked(
        limit: Option<usize>,
        offset: Option<usize>,
        default_limit: usize,
        max_search_limit: usize,
    ) -> Result<Self, String> {
        let window = SearchWindow {
            offset: offset.unwrap_or(0),
            limit: limit.unwrap_or(default_limit),
        };

        if window.limit > max_search_limit {
            return Err(format!(
                "limit {} is above the maximum of {max_search_limit}; ask for at most that many \
                 hits, or narrow the query",
                window.limit
            ));
        }

        if window.fetch_count() > max_search_limit {
            return Err(format!(
                "offset {} + limit {} = {} is above the maximum of {max_search_limit}; the \
                 engine fetches offset + limit hits, so a page this deep costs what a limit that \
                 large costs. Narrow the query, or sort on a field that lets you resume from the \
                 last hit instead of paging.",
                window.offset,
                window.limit,
                window.fetch_count()
            ));
        }

        Ok(window)
    }

    /// How many hits each source must return for this window to be servable from them.
    ///
    /// `offset + limit`, not `limit`. The skip happens once, after every source's hits have been
    /// merged into one order, so a source that returned only `limit` would leave the window short
    /// as soon as `offset` was non-zero.
    ///
    /// The tempting alternative — telling each source to skip `offset` itself — is wrong rather
    /// than merely different. Every hit in the window may come from a single source, so a source
    /// that skipped `offset` of *its own* hits would drop rows that belong in the answer and
    /// promote rows that do not. This is why Tantivy's own `and_offset` is not used here: it is
    /// the right tool for one segment and the wrong one for a scatter-gather.
    pub(crate) fn fetch_count(&self) -> usize {
        self.offset.saturating_add(self.limit)
    }

    /// Take this window out of a sequence that is already in its final order.
    pub(super) fn apply<T>(&self, ordered: Vec<T>) -> Vec<T> {
        ordered
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .collect()
    }
}

/// Order hits gathered from several sources and return the requested window of them.
///
/// `blocks` arrive in the order the sources were **dispatched**, never the order they answered,
/// and that is what makes this deterministic. Neither key a caller can order on is a total
/// order: every document matching one term scores identically, and a sort field repeats as
/// readily as any other value. Where a tie is settled by whichever source replied first, two
/// runs of one query return different documents — measured, not theorised — and a page of such
/// results is a page of nothing.
///
/// So a tie falls back to the source's rank, then to the hit's place within that source's own
/// ordering. Both are fixed before any result arrives, and each source has already ordered its
/// own hits totally — a shard through Tantivy, which breaks its own ties on document address;
/// a node through this same function. The composition is therefore one order, identical on
/// every run.
///
/// Every hit is held before sorting rather than kept in a running top-K. The bound is the same
/// either way, `window.fetch_count()` per source, and a running top-K cannot be made to agree
/// with this: it must decide what to discard while later sources are still unheard, so its answer
/// depends on the order they answer in — exactly what this function exists to remove.
///
/// The window is taken *after* the merge, which is what makes page *k* mean the same thing here
/// as it would on a single source — see [`SearchWindow::fetch_count`].
pub(crate) fn order_hit_blocks(
    blocks: Vec<Vec<JsonValue>>,
    sort: Option<&SortSpec>,
    window: SearchWindow,
) -> Vec<JsonValue> {
    let mut ranked: Vec<(usize, usize, JsonValue)> = blocks
        .into_iter()
        .enumerate()
        .flat_map(|(rank, hits)| {
            hits.into_iter()
                .enumerate()
                .map(move |(position, hit)| (rank, position, hit))
        })
        .collect();

    ranked.sort_by(
        |(left_rank, left_position, left), (right_rank, right_position, right)| {
            compare_hits_primary(left, right, sort)
                .then_with(|| left_rank.cmp(right_rank))
                .then_with(|| left_position.cmp(right_position))
        },
    );

    window
        .apply(ranked)
        .into_iter()
        .map(|(_, _, hit)| hit)
        .collect()
}

/// The page a fan-out has to compose, read off the operation that asked for it.
///
/// One function because there are two fan-outs — [`RouterActor::handle_broadcast`] and
/// [`RouterActor::handle_broadcast_streaming`] — and they answered this differently. Both had
/// their own `match &op`, and the streaming one discarded the offset outright, so a paged search
/// got page 1 whenever `enable_streaming_search` was on (ROADMAP OB8). They also disagreed about
/// `Stream`: one read its limit, the other fell through to the node default and ignored it.
///
/// A `Stream` has no offset to read. It hands the caller the whole result as it is produced, so
/// there is no page to take, and the HTTP stream route refuses an `offset` rather than accepting
/// one it would not honour. Its limit is still its own.
pub(super) fn search_window_for(op: &ClientOp, default_limit: usize) -> SearchWindow {
    match op {
        ClientOp::Search { limit, offset, .. } => SearchWindow {
            offset: offset.unwrap_or(0),
            limit: limit.unwrap_or(default_limit),
        },
        ClientOp::Stream { limit, .. } => SearchWindow::first(limit.unwrap_or(default_limit)),
        // Nothing else is paged. The value is unused on those paths rather than wrong on them.
        _ => SearchWindow::first(default_limit),
    }
}

/// Nothing about the checks needed to be split. The two hot paths differ in *where* the
/// work runs — inline, or fanned out over rayon — which is `parallel_validate_schema`'s
/// decision to make, and the per-document work is a handful of hash lookups either way.
///
/// `id` is the identifier the write arrived with, beside the body rather than in it — see
/// `unusable_document_identity` for why that is the authoritative one.
pub(super) fn validate_document(
    id: &str,
    doc: &JsonValue,
    schema_cache: &IndexSchema,
) -> SchemaValidationResult {
    // Check 1: the document has a usable identifier, and does not contradict it.
    if let Some(err) = unusable_document_identity(id, doc) {
        return SchemaValidationResult {
            needs_evolution: false,
            new_fields: Vec::new(),
            validation_error: Some(err),
        };
    }

    // Check 2: A shadow field is a name for the identifier, so it has to carry it.
    if let Some(err) = disagreeing_shadow_field(doc, schema_cache, id) {
        return SchemaValidationResult {
            needs_evolution: false,
            new_fields: Vec::new(),
            validation_error: Some(err),
        };
    }

    // Check 3: every field either fits what the schema declares, or is one the schema has
    // yet to hear about.
    let mut needs_evolution = false;
    let mut new_fields = Vec::new();

    if let Some(obj) = doc.as_object() {
        for (key, value) in obj {
            // The key is text whatever it looks like, and the index builds it itself.
            if key == "id" {
                continue;
            }

            match schema_cache.fields.get(key) {
                Some(existing_field) => {
                    if let Some(why) = unstorable_value(key, &existing_field.field_type, value) {
                        return SchemaValidationResult {
                            needs_evolution: false,
                            new_fields: Vec::new(),
                            validation_error: Some(why),
                        };
                    }
                }
                None => {
                    // A field the schema has never seen is typed by the value as a whole,
                    // so a list makes a text field rather than a field of its element type:
                    // the next document may carry a list of something else, and text is the
                    // only type that can hold both.
                    needs_evolution = true;
                    new_fields.push((key.clone(), infer_field_type(value)));
                }
            }
        }
    }

    SchemaValidationResult {
        needs_evolution,
        new_fields,
        validation_error: None,
    }
}
