//! The indexes themselves: their schemas, their configuration, and the list of them.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::info;

use crate::authz::Authz;
use crate::cluster_coordinator::OperationType;
use crate::http_server::error::AppError;
use crate::node_orchestrator::ClientOp;
use crate::state::AppState;
use storage::IndexSchema;

/// Validate an index name for creation.
///
/// Rejects names that could escape the `shard_path/indices/` directory via path
/// traversal (`..`, `/`, `\`), empty names, names exceeding 255 bytes, and names
/// that don't start with an alphanumeric character.
fn validate_index_name(index: &str) -> Result<(), AppError> {
    if index.is_empty() {
        return Err(AppError::bad_request("index name must not be empty"));
    }
    if index.len() > 255 {
        return Err(AppError::bad_request(
            "index name must not exceed 255 characters",
        ));
    }
    if !index.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return Err(AppError::bad_request(
            "index name must start with an alphanumeric character",
        ));
    }
    if index.contains("..") {
        return Err(AppError::bad_request("index name must not contain '..'"));
    }
    if index.contains('/') || index.contains('\\') {
        return Err(AppError::bad_request(
            "index name must not contain path separators",
        ));
    }
    if !index
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(AppError::bad_request(
            "index name contains invalid characters (allowed: a-z, A-Z, 0-9, _, -, .)",
        ));
    }
    Ok(())
}

/// Schema update request payload for maintenance API
#[derive(Debug, Deserialize)]
pub struct SchemaUpdatePayload {
    /// Map of field_name -> indexed (true/false)
    pub field_updates: std::collections::HashMap<String, bool>,
}

/// Query parameters for the list indexes endpoint
#[derive(Debug, Deserialize, Default)]
pub struct ListIndexesQuery {
    /// Whether to include data size information (default: false)
    #[serde(default)]
    pub data_size: Option<bool>,
}

impl ListIndexesQuery {
    /// Helper to get the data_size flag with a default of false
    pub fn include_data_size(&self) -> bool {
        self.data_size.unwrap_or(false)
    }
}

/// Handler for listing all indexes across the cluster
///
/// Same filtering as [`list_indexes_handler`], and it has to reach further: the cluster
/// listing repeats every index name under each node it contacted.
pub(super) async fn list_cluster_indexes_handler(
    State(state): State<AppState>,
    Query(params): Query<ListIndexesQuery>,
    authz: Option<Extension<Authz>>,
) -> Result<Json<JsonValue>, AppError> {
    info!("List cluster indexes request");

    let client_op = ClientOp::ListClusterIndexes {
        include_data_size: params.include_data_size(),
    };

    let mut result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await
        .map_err(AppError::from_route)?;
    if let Some(Extension(authz)) = authz {
        crate::authz::filter_index_listing(&mut result, &authz);
    }
    Ok(Json(result))
}

/// Handler for creating/updating index configuration/schema
pub(super) async fn create_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(schema): Json<IndexSchema>,
) -> Result<Json<JsonValue>, AppError> {
    validate_index_name(&index)?;
    // Checked here rather than in the engine so that an over-long description is a 400 naming
    // the offender, instead of a write that half-succeeds across shards.
    schema
        .validate_descriptions()
        .map_err(AppError::bad_request)?;
    info!("Create config request - index: {}", index);

    let client_op = ClientOp::CreateConfig { index, schema };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Write)
        .await
        .map_err(AppError::from_route)?;
    Ok(Json(result))
}

/// Handler for retrieving index configuration/schema
pub(super) async fn get_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, AppError> {
    info!("Get config request - index: {}", index);

    let client_op = ClientOp::GetConfig { index };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await
        .map_err(AppError::from_route)?;
    Ok(Json(result))
}

/// Handler for listing all available indexes
///
/// Filtered to the caller's scope: a key restricted to named indexes is already refused when
/// it addresses another one, so enumeration is the only way left to learn the names.
pub(super) async fn list_indexes_handler(
    State(state): State<AppState>,
    Query(params): Query<ListIndexesQuery>,
    authz: Option<Extension<Authz>>,
) -> Result<Json<JsonValue>, AppError> {
    info!("List indexes request");

    let client_op = ClientOp::ListIndexes {
        include_data_size: params.include_data_size(),
    };

    let mut result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await
        .map_err(AppError::from_route)?;
    if let Some(Extension(authz)) = authz {
        crate::authz::filter_index_listing(&mut result, &authz);
    }
    Ok(Json(result))
}

/// Handler for schema updates (maintenance API)
///
/// Only the `indexed` flag can be changed, and only through the engine, which edits the stored
/// schema in place. The two things it must not do are what the previous implementation did: read
/// the schema out through the `GetConfig` response shape and write it back — which erased every
/// property that shape does not carry, `routing_field_name` among them — and replay the edit as
/// `CreateConfig`, which re-creates the Tantivy index and so failed on the writer lockfile for
/// every index that had ever been written to.
pub(super) async fn update_schema_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SchemaUpdatePayload>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        index = %index,
        field_count = payload.field_updates.len(),
        "Schema update request"
    );

    if payload.field_updates.is_empty() {
        return Err(AppError::bad_request(
            "No field updates supplied. Provide `field_updates` as a map of field name to the \
             desired `indexed` flag.",
        ));
    }

    let result = state
        .router
        .route_and_handle(
            ClientOp::UpdateSchema {
                index: index.clone(),
                field_updates: payload.field_updates.into_iter().collect(),
            },
            None,
            OperationType::Write,
        )
        .await
        .map_err(AppError::from_route)?;

    // The engine reports a refusal rather than raising it, because which HTTP status it deserves
    // is this layer's question. Nothing was written in that case, and only an unknown field
    // gets here — a flag that cannot take effect yet is applied and noted, not refused.
    if result.get("acknowledged").and_then(|v| v.as_bool()) == Some(false) {
        let reason = result
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("Schema update refused");
        return Err(AppError {
            error: anyhow::anyhow!("{}", reason),
            status: Some(StatusCode::CONFLICT),
        });
    }

    info!(
        index = %index,
        updated_fields = ?result.get("updated_fields"),
        "Schema updated successfully"
    );

    Ok(Json(result))
}

/// Handler for deleting an index and all its data
pub(super) async fn delete_index_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Query(params): Query<DeleteIndexParams>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Delete index request - index: {}, delete_schema: {:?}",
        index, params.delete_schema
    );

    // Require the index to exist before deleting. A name that was never created
    // cannot have passed `validate_index_name`, so this also rejects traversal
    // attempts. Distinguish "absent" from "lookup failed" so that an actor
    // timeout is not reported to the client as a missing index.
    if let Err(e) = state
        .router
        .handle_client_op(ClientOp::GetConfig {
            index: index.clone(),
        })
        .await
    {
        let msg = e.to_string();
        return Err(if msg.contains("NotFound") || msg.contains("not found") {
            AppError::not_found(format!("index '{}' not found", index))
        } else {
            AppError::from(anyhow::anyhow!(
                "Failed to look up index '{}': {}",
                index,
                msg
            ))
        });
    }

    // Use cluster coordinator for proper cluster-wide index deletion
    let delete_msg = crate::cluster_coordinator::DeleteIndexCluster {
        index: index.clone(),
        delete_schema: params.delete_schema.unwrap_or(false),
    };

    let result = state.coordinator.ask(delete_msg).await.map_err(|e| {
        AppError::from(anyhow::anyhow!(
            "Failed to delete index across cluster: {}",
            e
        ))
    })?;

    Ok(Json(result))
}

#[derive(Deserialize, Default)]
pub(super) struct DeleteIndexParams {
    delete_schema: Option<bool>,
}

#[cfg(test)]
mod index_name_validation_tests {
    use super::validate_index_name;

    #[test]
    fn valid_names() {
        assert!(validate_index_name("my-index").is_ok());
        assert!(validate_index_name("index_123").is_ok());
        assert!(validate_index_name("a").is_ok());
        assert!(validate_index_name("camelCase").is_ok());
        assert!(validate_index_name("dots.are.ok").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_index_name("").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(256);
        assert!(validate_index_name(&long).is_err());
    }

    #[test]
    fn rejects_non_alphanumeric_start() {
        assert!(validate_index_name("_bad").is_err());
        assert!(validate_index_name("-bad").is_err());
        assert!(validate_index_name(".bad").is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        assert!(validate_index_name("..").is_err());
        assert!(validate_index_name("../etc").is_err());
        assert!(validate_index_name("a..b").is_err());
        assert!(validate_index_name("..%2f..%2fetc").is_err());
    }

    #[test]
    fn rejects_path_separators() {
        assert!(validate_index_name("a/b").is_err());
        assert!(validate_index_name("a\\b").is_err());
        assert!(validate_index_name("/etc").is_err());
    }

    #[test]
    fn rejects_special_chars() {
        assert!(validate_index_name("a b").is_err());
        assert!(validate_index_name("a;b").is_err());
        assert!(validate_index_name("a&b").is_err());
        assert!(validate_index_name("a|b").is_err());
    }
}
