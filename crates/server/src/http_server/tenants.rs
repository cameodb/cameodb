//! `/_keys` and `/_tenants` — mint and revoke keys, with no tenant or in one.
//!
//! The handlers decide who may ask; [`crate::tenants::TenantStore`] decides what the files
//! become. The route table already required `key-manage`; here a caller is held to its
//! `allowed_tenants`, and a caller bounded by it to its tenants — never the keys file — and to
//! index scopes under its tenants' own names.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value as JsonValue;

use crate::authz::{AuditedQuery, Authz};
use crate::cluster_coordinator::OperationType;
use crate::http_server::error::AppError;
use crate::node::ClientOp;
use crate::state::AppState;
use crate::tenants::{KeySpec, TenantError, TenantSpec, TenantView};

/// `GET /_tenants` — every tenant the caller may manage, managed by the node or not.
pub(super) async fn list_tenants_handler(
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
) -> Json<JsonValue> {
    let tenants = state
        .tenants
        .list(|tenant| may_manage(authz.as_deref(), tenant));
    Json(serde_json::json!({ "tenants": tenants }))
}

/// `GET /_tenants/{tenant}` — its settings, its keys, and the indexes it owns.
pub(super) async fn get_tenant_handler(
    Path(tenant): Path<String>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
) -> Result<Json<JsonValue>, AppError> {
    check_tenant(authz.as_deref(), &tenant)?;
    let view = state.tenants.get(&tenant).map_err(to_app_error)?;
    let indexes = owned_indexes(&state, &tenant).await?;
    Ok(Json(with_indexes(view, indexes)))
}

/// `PUT /_tenants/{tenant}` — create the tenant, or change its quota and index scope.
pub(super) async fn put_tenant_handler(
    Path(tenant): Path<String>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
    Json(spec): Json<TenantSpec>,
) -> Result<Response, AppError> {
    check_tenant(authz.as_deref(), &tenant)?;
    // A caller bounded to some tenants is bounded to their namespaces too: otherwise a partner
    // could give one of its tenants a scope reaching another company's indexes.
    if let Some(Authz::Key(entry)) = authz.as_deref()
        && entry.is_tenant_scoped()
        && let Some(outside) = spec
            .allowed_indexes
            .iter()
            .flatten()
            .find(|pattern| !pattern.trim().starts_with(&format!("{tenant}-")))
    {
        return Err(AppError::forbidden(format!(
            "allowed_indexes '{outside}' is outside '{tenant}-': a key bounded by allowed_tenants \
             scopes a tenant to indexes named after it"
        )));
    }
    let view = state
        .tenants
        .put_tenant(&tenant, spec, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    let audited = format!("revision={}", view.revision.unwrap_or_default());
    Ok(audited_json(
        Json(serde_json::to_value(view).unwrap_or_default()),
        audited,
    ))
}

/// `DELETE /_tenants/{tenant}` — the tenant and every key it has. Refused while it owns an
/// index: its data would stay, counted against nobody and reachable by no key of its own.
pub(super) async fn delete_tenant_handler(
    Path(tenant): Path<String>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
) -> Result<StatusCode, AppError> {
    check_tenant(authz.as_deref(), &tenant)?;
    let indexes = owned_indexes(&state, &tenant).await?;
    if !indexes.is_empty() {
        return Err(AppError::with_status(
            StatusCode::CONFLICT,
            format!(
                "tenant '{tenant}' owns {} index(es) ({}); drop them with \
                 DELETE /api/{{index}}?delete_schema=true first",
                indexes.len(),
                indexes.join(", ")
            ),
        ));
    }
    state
        .tenants
        .delete_tenant(&tenant, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /_tenants/{tenant}/keys` — mint a key. With `POST /_keys`, the one response that
/// ever carries one.
pub(super) async fn create_tenant_key_handler(
    Path(tenant): Path<String>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
    Json(spec): Json<KeySpec>,
) -> Result<Response, AppError> {
    check_tenant(authz.as_deref(), &tenant)?;
    let minted = state
        .tenants
        .create_tenant_key(&tenant, spec, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    let audited = format!("key_id={} revision={}", minted.key_id, minted.revision);
    let mut response = audited_json(
        Json(serde_json::to_value(&minted).unwrap_or_default()),
        audited,
    );
    *response.status_mut() = StatusCode::CREATED;
    Ok(response)
}

/// `DELETE /_tenants/{tenant}/keys/{key_id}` — revoke one key, from its next request on.
pub(super) async fn revoke_tenant_key_handler(
    Path((tenant, key_id)): Path<(String, String)>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    check_tenant(authz.as_deref(), &tenant)?;
    let view = state
        .tenants
        .revoke_tenant_key(&tenant, &key_id, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    let audited = format!("revision={}", view.revision.unwrap_or_default());
    Ok(audited_json(
        Json(serde_json::to_value(view).unwrap_or_default()),
        audited,
    ))
}

/// `GET /_keys` — the keys the node manages with no tenant.
pub(super) async fn list_keys_handler(
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
) -> Result<Json<JsonValue>, AppError> {
    check_unbounded(authz.as_deref())?;
    let view = state.tenants.list_keys().map_err(to_app_error)?;
    Ok(Json(serde_json::to_value(view).unwrap_or_default()))
}

/// `POST /_keys` — mint a key with no tenant, scoped by its own `allowed_indexes`.
pub(super) async fn create_key_handler(
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
    Json(spec): Json<KeySpec>,
) -> Result<Response, AppError> {
    check_unbounded(authz.as_deref())?;
    let minted = state
        .tenants
        .create_key(spec, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    let audited = format!("key_id={} revision={}", minted.key_id, minted.revision);
    let mut response = audited_json(
        Json(serde_json::to_value(&minted).unwrap_or_default()),
        audited,
    );
    *response.status_mut() = StatusCode::CREATED;
    Ok(response)
}

/// `DELETE /_keys/{key_id}` — revoke a key with no tenant, from its next request on.
pub(super) async fn revoke_key_handler(
    Path(key_id): Path<String>,
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    check_unbounded(authz.as_deref())?;
    let view = state
        .tenants
        .revoke_key(&key_id, if_match(&headers)?)
        .await
        .map_err(to_app_error)?;
    let audited = format!("revision={}", view.revision);
    Ok(audited_json(
        Json(serde_json::to_value(view).unwrap_or_default()),
        audited,
    ))
}

/// A key bounded by `allowed_tenants` manages those tenants and nothing else: a key with no
/// tenant would reach whatever indexes it names, past every tenant the caller is held to.
fn check_unbounded(authz: Option<&Authz>) -> Result<(), AppError> {
    match authz {
        Some(Authz::Key(entry)) if entry.is_tenant_scoped() => Err(AppError::forbidden(
            "this key manages only its allowed_tenants; keys with no tenant are outside them",
        )),
        _ => Ok(()),
    }
}

/// With authentication off there is no caller to bound, as on every other route.
fn may_manage(authz: Option<&Authz>, tenant: &str) -> bool {
    match authz {
        Some(Authz::Key(entry)) => entry.allows_tenant(tenant),
        _ => true,
    }
}

fn check_tenant(authz: Option<&Authz>, tenant: &str) -> Result<(), AppError> {
    if may_manage(authz, tenant) {
        Ok(())
    } else {
        Err(AppError::forbidden(format!(
            "this key is not permitted to manage tenant '{tenant}'"
        )))
    }
}

/// `If-Match: 3` or `If-Match: "3"` — the revision the caller last saw.
fn if_match(headers: &HeaderMap) -> Result<Option<u64>, AppError> {
    let Some(value) = headers.get(axum::http::header::IF_MATCH) else {
        return Ok(None);
    };
    value
        .to_str()
        .ok()
        .map(|v| v.trim().trim_matches('"'))
        .and_then(|v| v.parse().ok())
        .map(Some)
        .ok_or_else(|| {
            AppError::bad_request("If-Match must be a tenant revision, e.g. If-Match: 3")
        })
}

/// The indexes on this node whose schema names `tenant` as owner.
async fn owned_indexes(state: &AppState, tenant: &str) -> Result<Vec<String>, AppError> {
    let listing = state
        .router
        .route_and_handle(
            ClientOp::ListIndexes {
                include_data_size: false,
            },
            None,
            OperationType::Read,
        )
        .await
        .map_err(AppError::from_route)?;
    Ok(listing["indexes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|index| index["tenant"].as_str() == Some(tenant))
        .filter_map(|index| index["name"].as_str().map(str::to_string))
        .collect())
}

fn with_indexes(view: TenantView, indexes: Vec<String>) -> JsonValue {
    let mut value = serde_json::to_value(view).unwrap_or_default();
    value["indexes"] = serde_json::json!(indexes);
    value
}

/// The response, carrying for the audit trail what the path does not say — the key minted,
/// the revision written. Never the key.
fn audited_json(body: Json<JsonValue>, audited: String) -> Response {
    let mut response = body.into_response();
    response.extensions_mut().insert(AuditedQuery(audited));
    response
}

fn to_app_error(err: TenantError) -> AppError {
    let message = err.to_string();
    match err {
        TenantError::NotConfigured(_)
        | TenantError::NotManaged { .. }
        | TenantError::ForeignFile(_) => AppError::with_status(StatusCode::CONFLICT, message),
        TenantError::InvalidName(_) | TenantError::Refused(_) => AppError::bad_request(message),
        TenantError::NotFound(_) => AppError::not_found("tenant_not_found", message),
        TenantError::KeyNotFound { .. } => AppError::not_found("key_not_found", message),
        TenantError::RevisionMismatch { .. } => {
            AppError::with_status(StatusCode::PRECONDITION_FAILED, message)
        }
        TenantError::Forbidden(_) => AppError::forbidden(message),
        // Not a caller's mistake: answered as a 500, its text kept for the log only.
        TenantError::Io(err) => AppError::from(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_match_takes_a_bare_or_quoted_revision() {
        let mut headers = HeaderMap::new();
        assert_eq!(if_match(&headers).unwrap(), None);
        headers.insert(axum::http::header::IF_MATCH, "3".parse().unwrap());
        assert_eq!(if_match(&headers).unwrap(), Some(3));
        headers.insert(axum::http::header::IF_MATCH, "\"7\"".parse().unwrap());
        assert_eq!(if_match(&headers).unwrap(), Some(7));
        headers.insert(axum::http::header::IF_MATCH, "*".parse().unwrap());
        assert!(if_match(&headers).is_err());
    }

    /// Each refusal reaches the caller as its own status, and a 404 says what was missing:
    /// a client matching `index_not_found` must never see it for a tenant.
    #[test]
    fn tenant_errors_map_to_their_statuses_and_codes() {
        let cases = [
            (TenantError::NotConfigured("key"), StatusCode::CONFLICT),
            (
                TenantError::InvalidName("..".into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                TenantError::Refused("overlaps".into()),
                StatusCode::BAD_REQUEST,
            ),
            (TenantError::NotFound("acme".into()), StatusCode::NOT_FOUND),
            (
                TenantError::RevisionMismatch {
                    place: "tenant 'acme'".into(),
                    current: 2,
                    expected: 1,
                },
                StatusCode::PRECONDITION_FAILED,
            ),
            (
                TenantError::Forbidden("admin".into()),
                StatusCode::FORBIDDEN,
            ),
        ];
        for (err, status) in cases {
            assert_eq!(to_app_error(err).status, Some(status));
        }
        assert_eq!(
            to_app_error(TenantError::NotFound("acme".into())).code,
            Some("tenant_not_found")
        );
        assert_eq!(
            to_app_error(TenantError::KeyNotFound {
                place: "in tenant 'acme'".into(),
                key_id: "k".into()
            })
            .code,
            Some("key_not_found")
        );
    }

    #[test]
    fn a_caller_is_held_to_its_allowed_tenants() {
        let ring = crate::auth::SecurityConfig {
            enabled: true,
            api_keys: vec![crate::auth::ApiKeyConfig {
                key_hash: Some(
                    crate::auth::ApiKey::generate()
                        .unwrap()
                        .digest()
                        .to_config_value(),
                ),
                role: Some(crate::auth::Role::Provisioner),
                allowed_tenants: Some(vec!["partner-x-*".to_string()]),
                ..Default::default()
            }],
            ..Default::default()
        }
        .load_keyring()
        .unwrap();
        let authz = Authz::Key(std::sync::Arc::clone(&ring.entries()[0]));
        assert!(check_tenant(Some(&authz), "partner-x-acme").is_ok());
        assert_eq!(
            check_tenant(Some(&authz), "acme").unwrap_err().status,
            Some(StatusCode::FORBIDDEN)
        );
        assert!(may_manage(None, "acme"));

        // Bounded to some tenants, it manages no key outside them: /_keys is refused.
        assert_eq!(
            check_unbounded(Some(&authz)).unwrap_err().status,
            Some(StatusCode::FORBIDDEN)
        );
        assert!(check_unbounded(None).is_ok());
    }
}
