//! Routing keys — the one ladder that decides which shard a document lands on.
//!
//! Every function here answers a rung of the same question, and they are in one file because the
//! answer has to agree with itself: whatever routes a document has to reach the same shard every
//! time, or the same id lands in two places and a search returns it twice. The ladder had been
//! written out four separate times before CH11 consolidated the *algorithms*; splitting
//! `node_orchestrator.rs` into `node/` then re-scattered the *family*, leaving
//! [`derive_routing_key_from_doc`] five thousand lines from the ladder that is its only
//! production caller. A module is the fence: the next split moves the file, not the members.
//!
//! The ladder, in the order of precedence that decides a write:
//!
//! 1. [`extract_routing_value`] on the schema's routing field — the document's own answer.
//! 2. The caller's `routing_key`, honoured only where the document did not answer.
//! 3. The id.
//! 4. [`derive_routing_key_from_doc`], so an unkeyed write is deterministic rather than random.
//!
//! [`effective_routing_key`] is the whole ladder; [`routing_key_for`] is the same ladder for
//! callers holding the routing field rather than the schema; [`routing_key_without_schema`] is
//! rungs 2–4, for the HTTP layer, which has to pick a node before any schema is resolved. A
//! delete has no document, so [`effective_delete_routing_key`] is its shorter ladder and the
//! refusal it can end in.

use super::*;

/// The key a write is routed by, in the order of precedence that decides it.
///
/// Written out identically in three places before this existed — the engine fast path and both
/// halves of the actor path — which is a rule that has to agree with itself to be a rule at all.
/// Whatever routes a document has to reach the same shard every time, or the same id lands in
/// two places and a search returns it twice.
///
/// 1. **The document's own routing field.** The schema names it, and it is the authority: a
///    shadow index routes by the source's name for the key, a tenant index by the tenant.
/// 2. **What the caller asked for.** Honoured only where the document does not answer, so a
///    caller cannot move a document off the shard its schema puts it on.
/// 3. **The id**, which is what the routing field resolves to on a default index anyway.
/// 4. **A hash of the document**, so an unkeyed write is still deterministic rather than random.
pub(super) fn effective_routing_key(
    schema: &IndexSchema,
    id: &str,
    routing_key: Option<String>,
    doc: &JsonValue,
) -> Option<String> {
    routing_key_for(schema.get_routing_field(), routing_key, id, doc)
}

/// The whole routing ladder, for callers that hold the routing field rather than the schema.
///
/// The bulk router resolves the field once and then routes thousands of documents against it, so
/// it cannot take a schema per document — which is how it came to spell the ladder out a fourth
/// time. Same rungs, one place.
pub(super) fn routing_key_for(
    routing_field: &str,
    routing_key: Option<String>,
    id: &str,
    doc: &JsonValue,
) -> Option<String> {
    extract_routing_value(doc, routing_field)
        .or_else(|| routing_key_without_schema(routing_key, id, doc))
}

/// The rungs of the routing precedence that need no schema: the caller's key, then the id, then
/// a hash of the document.
///
/// [`effective_routing_key`] starts one rung higher, at the schema's routing field, and falls
/// through to this. It was split out for the HTTP layer, which picked a node for a bulk batch
/// from its first document before any schema was resolved; the two copies had drifted onto
/// different hashes of different byte ranges, and unifying kept the orchestrator's, because the
/// shard a document lands on must not change across an upgrade. That hint is gone — a bulk op
/// now runs where it was received (see `route_and_handle_inner`) — and this ladder stays one.
pub(crate) fn routing_key_without_schema(
    routing_key: Option<String>,
    id: &str,
    doc: &JsonValue,
) -> Option<String> {
    routing_key
        .or_else(|| (!id.is_empty()).then(|| id.to_string()))
        .or_else(|| derive_routing_key_from_doc(doc))
}

/// The last rung: a deterministic key derived from the document's own bytes.
///
/// Reached only when the schema's routing field, the caller's key and the id have all come up
/// empty — an unkeyed write. Hashing the content keeps such a write *stable* rather than random,
/// so the same document re-sent lands on the same shard and an overwrite stays an overwrite.
///
/// The key is the hex of the document's first 64 serialised bytes rather than a digest of them:
/// `ConsistentRing` hashes the key again, so a second hash here would buy nothing, and the prefix
/// is what bounds the key's size for a document of any size. Two documents agreeing in those 64
/// bytes share a shard, which costs placement skew and never correctness — the shard is the unit
/// of placement, and identity is still the id.
pub(super) fn derive_routing_key_from_doc(doc: &JsonValue) -> Option<String> {
    let mut bytes = serde_json::to_vec(doc).ok()?;
    if bytes.is_empty() {
        // Use a fixed token to remain deterministic for empty objects
        return Some("empty-doc".to_string());
    }

    // Limit the number of bytes used to keep the key reasonably sized
    const MAX_PREFIX_LEN: usize = 64;
    if bytes.len() > MAX_PREFIX_LEN {
        bytes.truncate(MAX_PREFIX_LEN);
    }

    // Hex-encode the prefix to a string key; ConsistentRing will hash it again
    let mut key = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut key, "{:02x}", b);
    }
    Some(key)
}

/// Extract routing key value from JSON document using field name
pub(super) fn extract_routing_value(doc: &JsonValue, field_name: &str) -> Option<String> {
    let obj = doc.as_object()?;
    match obj.get(field_name)? {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The key a delete is routed by, or the reason it cannot be routed.
///
/// A write reads its routing key out of the document, which outranks everything the caller sent.
/// A delete has no document, so only two of those four rungs are left — and which of them applies
/// is decided entirely by the schema:
///
/// - **The routing field is the key.** `id` by default, or a shadow field, whose value *is* the
///   document key by definition. The id routes, exactly as the original write did.
/// - **The routing field is some other field**, a tenant or a customer. The id says nothing about
///   which shard holds the row, so the caller has to supply the same key the write used.
///
/// The refusal in that second case is deliberate and recorded as a non-goal: fanning a keyless
/// delete out to every shard on every node is *correct*, since a shard without the id removes
/// nothing, but it costs `shards × nodes` writer transactions to remove one row and the code
/// already refuses to broadcast a write. The error names the field so the caller knows what to
/// send, which it can read off the document with one search.
pub(super) fn effective_delete_routing_key(
    schema: &IndexSchema,
    id: &str,
    routing_key: Option<String>,
) -> Result<String, OrchestratorError> {
    let routing_field = schema.get_routing_field();

    // The routing field is the key — `id`, or a shadow field whose value *is* the key — so the
    // id names the shard the write used, and a caller-supplied `routing_key` cannot retarget it.
    // Accepting one here let a wrong key route a delete to a shard that holds no such row, where
    // it removed nothing and still answered "deleted".
    if routing_field == "id" || schema.is_shadow_field(routing_field) {
        return Ok(id.to_string());
    }

    // The routing field is a real field, so the id says nothing about the shard. The caller
    // must supply the same key the write used; an empty one is as useless as none.
    match routing_key {
        Some(key) if !key.is_empty() => Ok(key),
        _ => Err(OrchestratorError::Validation(format!(
            "index routes by '{routing_field}', which is not the document key, so a delete \
                 must carry the same routing_key the write used — read it off the document with \
                 a search for id:{id}"
        ))),
    }
}
