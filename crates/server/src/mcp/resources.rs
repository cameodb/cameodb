//! The `cameodb://` resource URIs, which are the browsing form of the same catalogue.

use cameodb_mcp::McpAuthzRef;
use futures::future::BoxFuture;
use serde_json::Value as JsonValue;

use crate::mcp::discovery::{describe_index, index_stats, list_indexes};
use crate::state::AppState;

fn resource_descriptor(uri: String, name: String, description: String) -> JsonValue {
    serde_json::json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

pub(super) fn list_resources(
    state: AppState,
    authz: McpAuthzRef,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        // Every per-index resource URI below is derived from this listing, so a scoped
        // caller is never handed a URI it would be refused for.
        let listing = list_indexes(state.clone(), authz).await?;
        let indexes = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        let mut resources = vec![resource_descriptor(
            "cameodb://indexes".to_string(),
            "CameoDB Index Catalog".to_string(),
            "Every CameoDB index, with its description, document count and field names. \
             Per-index schema is at cameodb://indexes/{index}."
                .to_string(),
        )];

        for item in indexes {
            let index_name = item
                .get("index")
                .and_then(|value| value.as_str())
                .ok_or_else(|| "Index entry missing index name".to_string())?
                .to_string();

            resources.push(resource_descriptor(
                format!("cameodb://indexes/{index_name}"),
                format!("Index {index_name}"),
                format!("Metadata resource for CameoDB index '{index_name}'."),
            ));
            resources.push(resource_descriptor(
                format!("cameodb://indexes/{index_name}/schema"),
                format!("Index {index_name} Schema"),
                format!("Schema resource for CameoDB index '{index_name}'."),
            ));
            resources.push(resource_descriptor(
                format!("cameodb://indexes/{index_name}/stats"),
                format!("Index {index_name} Statistics"),
                format!("Statistics resource for CameoDB index '{index_name}'."),
            ));
        }

        Ok(JsonValue::Array(resources))
    })
}

/// A resource URI names an index, so this is the one place the mcp crate cannot check
/// the scope for us: only the host knows that `cameodb://indexes/payroll/schema` is a
/// read of `payroll`.
pub(super) fn read_resource(
    state: AppState,
    uri: String,
    authz: McpAuthzRef,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        if uri == "cameodb://indexes" {
            return list_indexes(state.clone(), authz).await;
        }

        let resource = uri
            .strip_prefix("cameodb://indexes/")
            .ok_or_else(|| format!("Unsupported resource URI: {uri}"))?;

        let index_name = resource
            .strip_suffix("/schema")
            .or_else(|| resource.strip_suffix("/stats"))
            .unwrap_or(resource);
        if !authz.allows_index(index_name) {
            return Err(format!("this key is not permitted on index '{index_name}'"));
        }

        if let Some(index_name) = resource.strip_suffix("/schema") {
            let details = describe_index(state.clone(), index_name.to_string()).await?;
            return Ok(details.get("schema").cloned().unwrap_or(JsonValue::Null));
        }

        if let Some(index_name) = resource.strip_suffix("/stats") {
            return index_stats(state.clone(), Some(index_name.to_string()), authz).await;
        }

        describe_index(state.clone(), resource.to_string()).await
    })
}
