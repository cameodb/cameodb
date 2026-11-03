//! The OAuth endpoints for MCP clients (ROADMAP T1): discovery, registration, the approval page
//! and the token endpoint. [`crate::oauth`] holds the rules; this is their HTTP shape.
//!
//! Every route here is public in the gate's table — a client reaches them before it holds
//! anything — and mounted only when `[security.oauth]` is on. What protects them is what they
//! check: the approval page takes a key and checks it against the ring, and the token endpoint
//! takes a code or a refresh token only this node's secret could have signed.

use axum::{
    Form, Json,
    extract::{Query, State, rejection::FormRejection, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
};
use base64::Engine;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use crate::auth::Capability;
use crate::oauth::{
    ClientAuth, ClientCredentials, OAuthError, RegisteredClient, SCOPE, TokenPair,
    is_s256_challenge,
};
use crate::state::AppState;

// --- Discovery ------------------------------------------------------------------------------

/// `GET /.well-known/oauth-protected-resource[/mcp]` — RFC 9728: what `/mcp` is and who issues
/// its tokens. The `/mcp` form is where the `401` points; the root form serves a client that
/// looks there first.
pub(super) async fn protected_resource_handler(State(state): State<AppState>) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(json!({
        "resource": oauth.resource(),
        "authorization_servers": [oauth.issuer()],
        "scopes_supported": [SCOPE],
        "bearer_methods_supported": ["header"],
        "resource_name": "CameoDB MCP",
    }))
    .into_response()
}

/// `GET /.well-known/oauth-authorization-server` — RFC 8414.
pub(super) async fn authorization_server_handler(State(state): State<AppState>) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let issuer = oauth.issuer();
    Json(json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/oauth/authorize"),
        "token_endpoint": format!("{issuer}/oauth/token"),
        "registration_endpoint": format!("{issuer}/oauth/register"),
        "scopes_supported": [SCOPE],
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported":
            ["none", "client_secret_post", "client_secret_basic"],
        "authorization_response_iss_parameter_supported": true,
    }))
    .into_response()
}

// --- Registration ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RegistrationRequest {
    #[serde(default)]
    redirect_uris: Vec<String>,
    client_name: Option<String>,
    token_endpoint_auth_method: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
}

/// `POST /oauth/register` — RFC 7591 dynamic client registration.
///
/// Open, as the MCP spec expects: a client registers before anyone has signed in. Nothing is
/// stored — the id returned *is* the registration, signed — so registering costs a hash and
/// leaves nothing behind.
pub(super) async fn register_handler(State(state): State<AppState>, body: Bytes) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata = |description: String| {
        error_json(
            StatusCode::BAD_REQUEST,
            &OAuthError {
                error: "invalid_client_metadata",
                description,
            },
        )
    };
    let request: RegistrationRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(e) => return metadata(format!("the body is not client metadata JSON: {e}")),
    };
    let auth = match request.token_endpoint_auth_method.as_deref() {
        None => ClientAuth::None,
        Some(method) => match ClientAuth::parse(method) {
            Some(auth) => auth,
            None => {
                return metadata(format!(
                    "token_endpoint_auth_method {method:?} is not supported: none, \
                     client_secret_post or client_secret_basic"
                ));
            }
        },
    };
    if let Some(grants) = &request.grant_types
        && let Some(other) = grants
            .iter()
            .find(|g| !matches!(g.as_str(), "authorization_code" | "refresh_token"))
    {
        return metadata(format!("grant type {other:?} is not supported"));
    }
    if let Some(types) = &request.response_types
        && let Some(other) = types.iter().find(|t| t.as_str() != "code")
    {
        return metadata(format!("response type {other:?} is not supported"));
    }

    match oauth.register(request.redirect_uris, request.client_name, auth) {
        Ok((client_id, client_secret, client)) => {
            let mut body = json!({
                "client_id": client_id,
                "client_id_issued_at": client.issued_at,
                "redirect_uris": client.redirect_uris,
                "token_endpoint_auth_method": client.auth.as_str(),
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "scope": SCOPE,
            });
            if let Some(name) = &client.name {
                body["client_name"] = json!(name);
            }
            if let Some(secret) = client_secret {
                body["client_secret"] = json!(secret);
                body["client_secret_expires_at"] = json!(0);
            }
            (StatusCode::CREATED, no_store(), Json(body)).into_response()
        }
        Err(e) => error_json(StatusCode::BAD_REQUEST, &e),
    }
}

// --- Authorization --------------------------------------------------------------------------

#[derive(Deserialize)]
pub(super) struct AuthorizeParams {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    state: Option<String>,
    resource: Option<String>,
}

/// `GET /oauth/authorize` — the approval page.
///
/// Errors follow RFC 6749 §4.1.2.1: a request whose client or redirect URI cannot be trusted is
/// answered on this page and never redirected, since redirecting would send the browser wherever
/// the request said; every other error goes back to the client.
pub(super) async fn authorize_page_handler(
    State(state): State<AppState>,
    params: Result<Query<AuthorizeParams>, QueryRejection>,
) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(Query(params)) = params else {
        return error_page("The authorization request could not be read.");
    };
    let Some(client) = params.client_id.as_deref().and_then(|id| oauth.client(id)) else {
        return error_page(
            "The application did not identify itself with a client this server registered. \
             Remove the connector and add it again.",
        );
    };
    let Some(redirect_uri) = client.redirect_uri(params.redirect_uri.as_deref()) else {
        return error_page(
            "The application asked to be sent to an address it did not register, so this \
             request is not trusted.",
        );
    };
    let redirect_uri = redirect_uri.to_string();
    let back = |error: &'static str, description: &str| {
        redirect_with(
            &redirect_uri,
            &[
                ("error", error),
                ("error_description", description),
                ("iss", oauth.issuer()),
            ],
            params.state.as_deref(),
        )
    };

    if params.response_type.as_deref() != Some("code") {
        return back("unsupported_response_type", "response_type must be code");
    }
    let challenge = match params.code_challenge.as_deref() {
        Some(c) if is_s256_challenge(c) => c,
        _ => return back("invalid_request", "a PKCE code_challenge is required"),
    };
    if params.code_challenge_method.as_deref() != Some("S256") {
        return back("invalid_request", "code_challenge_method must be S256");
    }
    if !oauth.accepts_resource(params.resource.as_deref()) {
        return back(
            "invalid_target",
            "this server issues tokens for its /mcp only",
        );
    }

    let sealed = oauth.seal_request(
        &client.id,
        &redirect_uri,
        challenge,
        params.state.as_deref(),
    );
    approval_page(&client, &redirect_uri, &sealed, None)
}

#[derive(Deserialize)]
pub(super) struct ApprovalForm {
    request: String,
    #[serde(default)]
    key: String,
    decision: Option<String>,
}

/// `POST /oauth/authorize` — the page's submission: approve with a key, or deny.
///
/// No session and no cookie: the request comes back signed in the form, and the key in the form
/// is the whole authentication. A page submitted from elsewhere carries no key it could know.
pub(super) async fn authorize_submit_handler(
    State(state): State<AppState>,
    form: Result<Form<ApprovalForm>, FormRejection>,
) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(Form(form)) = form else {
        return error_page("The approval form could not be read.");
    };
    let Some(request) = oauth.open_request(&form.request) else {
        return error_page(
            "This approval page has expired. Start again from the application you are \
             connecting.",
        );
    };
    let Some(client) = oauth.client(&request.client_id) else {
        return error_page("The application's registration is no longer valid.");
    };

    if form.decision.as_deref() != Some("approve") {
        return redirect_with(
            &request.redirect_uri,
            &[
                ("error", "access_denied"),
                ("error_description", "the user denied access"),
                ("iss", oauth.issuer()),
            ],
            request.state.as_deref(),
        );
    }

    let ring = state.keyring.current();
    let Some(entry) = ring.authenticate(form.key.trim()) else {
        return approval_page(
            &client,
            &request.redirect_uri,
            &form.request,
            Some("That key is not recognised. Paste the whole key, starting with cameo_v1_."),
        );
    };
    if !entry.has(Capability::Read) {
        return approval_page(
            &client,
            &request.redirect_uri,
            &form.request,
            Some("That key cannot read data, so it cannot be used here."),
        );
    }

    let code = oauth.issue_code(&request, &entry);
    info!(
        key_id = %entry.key_id(), label = %entry.label(),
        client = %client_label(&client), redirect_host = %redirect_host(&request.redirect_uri),
        "oauth: access approved"
    );
    redirect_with(
        &request.redirect_uri,
        &[("code", code.as_str()), ("iss", oauth.issuer())],
        request.state.as_deref(),
    )
}

// --- Token ----------------------------------------------------------------------------------

#[derive(Deserialize)]
pub(super) struct TokenForm {
    grant_type: Option<String>,
    code: Option<String>,
    redirect_uri: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    resource: Option<String>,
}

/// `POST /oauth/token` — a code or a refresh token for a new pair of tokens.
pub(super) async fn token_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    form: Result<Form<TokenForm>, FormRejection>,
) -> Response {
    // Mounted only when the node has one; absent, the route would not exist.
    let Some(oauth) = state.oauth.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(Form(form)) = form else {
        return error_json(
            StatusCode::BAD_REQUEST,
            &OAuthError::invalid_request("the body must be application/x-www-form-urlencoded"),
        );
    };

    let basic = basic_credentials(&headers);
    let credentials = match &basic {
        Some((id, secret)) => {
            if form.client_id.as_deref().is_some_and(|f| f != id) {
                return error_json(
                    StatusCode::BAD_REQUEST,
                    &OAuthError::invalid_request("client_id differs from the Basic credentials"),
                );
            }
            ClientCredentials {
                client_id: Some(id.as_str()),
                client_secret: Some(secret.as_str()),
                basic: true,
            }
        }
        None => ClientCredentials {
            client_id: form.client_id.as_deref(),
            client_secret: form.client_secret.as_deref(),
            basic: false,
        },
    };

    let ring = state.keyring.current();
    let result = match form.grant_type.as_deref() {
        Some("authorization_code") => match form.code.as_deref() {
            Some(code) => oauth.exchange_code(
                &ring,
                &credentials,
                code,
                form.redirect_uri.as_deref(),
                form.code_verifier.as_deref(),
                form.resource.as_deref(),
            ),
            None => Err(OAuthError::invalid_request("code is required")),
        },
        Some("refresh_token") => match form.refresh_token.as_deref() {
            Some(token) => oauth.refresh(&ring, &credentials, token, form.resource.as_deref()),
            None => Err(OAuthError::invalid_request("refresh_token is required")),
        },
        Some(other) => Err(OAuthError {
            error: "unsupported_grant_type",
            description: format!("grant_type {other:?} is not supported"),
        }),
        None => Err(OAuthError::invalid_request("grant_type is required")),
    };

    match result {
        Ok(TokenPair {
            access_token,
            refresh_token,
            expires_in,
        }) => (
            no_store(),
            Json(json!({
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": expires_in,
                "refresh_token": refresh_token,
                "scope": SCOPE,
            })),
        )
            .into_response(),
        Err(e) => {
            warn!(error = e.error, description = %e.description, "oauth: token request refused");
            let status = if e.error == "invalid_client" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_REQUEST
            };
            let mut response = error_json(status, &e);
            if status == StatusCode::UNAUTHORIZED {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Basic realm=\"cameodb\""),
                );
            }
            response
        }
    }
}

/// `Authorization: Basic` client credentials, each half form-decoded as RFC 6749 §2.3.1 asks.
fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (id, secret) = decoded.split_once(':')?;
    let unescape = |s: &str| -> String {
        form_urlencoded::parse(format!("v={s}").as_bytes())
            .next()
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    };
    Some((unescape(id), unescape(secret)))
}

// --- Responses ------------------------------------------------------------------------------

fn no_store() -> [(header::HeaderName, &'static str); 2] {
    [
        (header::CACHE_CONTROL, "no-store"),
        (header::PRAGMA, "no-cache"),
    ]
}

fn error_json(status: StatusCode, e: &OAuthError) -> Response {
    (
        status,
        no_store(),
        Json(json!({ "error": e.error, "error_description": e.description })),
    )
        .into_response()
}

/// Back to the client: `redirect_uri` with `params` and `state` added to its query.
fn redirect_with(redirect_uri: &str, params: &[(&str, &str)], state: Option<&str>) -> Response {
    let mut query = form_urlencoded::Serializer::new(String::new());
    for (name, value) in params {
        query.append_pair(name, value);
    }
    if let Some(state) = state {
        query.append_pair("state", state);
    }
    let separator = if redirect_uri.contains('?') { '&' } else { '?' };
    let location = format!("{redirect_uri}{separator}{}", query.finish());
    // 303, so the browser follows with a GET whatever method brought it here.
    let mut response = Redirect::to(&location).into_response();
    response.headers_mut().extend(page_headers());
    response
}

/// What every page and redirect from here carries: nothing cached, nothing framed — a framed
/// approval page is a clickjacking target — no referrer to leak the request into, and no script.
fn page_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; \
             base-uri 'none'",
        ),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers
}

/// The name a client gave itself, or that it gave none.
fn client_label(client: &RegisteredClient) -> String {
    client
        .client
        .name
        .clone()
        .unwrap_or_else(|| "An unnamed application".to_string())
}

/// The part of a redirect URI a person can judge: its host, or its scheme for a native app.
fn redirect_host(uri: &str) -> String {
    match uri.split_once("://") {
        Some((_, rest)) => rest
            .split(['/', '?'])
            .next()
            .unwrap_or_default()
            .to_string(),
        None => uri.split(':').next().unwrap_or_default().to_string(),
    }
}

fn approval_page(
    client: &RegisteredClient,
    redirect_uri: &str,
    sealed: &str,
    problem: Option<&str>,
) -> Response {
    let problem = problem
        .map(|p| format!(r#"<p class="problem" role="alert">{}</p>"#, escape(p)))
        .unwrap_or_default();
    let body = format!(
        r#"<h1>Connect to CameoDB</h1>
<p><strong>{name}</strong> <span class="muted">(name not verified)</span> asks to read the
CameoDB indexes your key can read, through MCP.</p>
<p>After you approve, your browser returns to <strong>{host}</strong>. If you did not start this
from that application, deny.</p>
{problem}
<form method="post" action="/oauth/authorize" autocomplete="off">
<input type="hidden" name="request" value="{sealed}">
<label for="key">Your CameoDB key</label>
<input id="key" name="key" type="password" autocomplete="current-password" placeholder="cameo_v1_…" autofocus>
<div class="buttons">
<button type="submit" name="decision" value="approve">Approve</button>
<button type="submit" name="decision" value="deny" class="secondary" formnovalidate>Deny</button>
</div>
</form>
<p class="muted">The application never sees your key. It receives a token that reads what your
key reads, and stops working when the key is revoked.</p>"#,
        name = escape(&client_label(client)),
        host = escape(&redirect_host(redirect_uri)),
        sealed = escape(sealed),
    );
    page(StatusCode::OK, &body)
}

fn error_page(message: &str) -> Response {
    page(
        StatusCode::BAD_REQUEST,
        &format!(
            "<h1>This request cannot continue</h1>\n<p>{}</p>",
            escape(message)
        ),
    )
}

fn page(status: StatusCode, body: &str) -> Response {
    let html = format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CameoDB authorization</title>
<style>
:root {{ color-scheme: light dark; --fg:#1b1b1f; --bg:#f6f6f8; --card:#fff; --muted:#666; --accent:#3b5bdb; --err:#c92a2a; }}
@media (prefers-color-scheme: dark) {{ :root {{ --fg:#e8e8ec; --bg:#141417; --card:#1e1e23; --muted:#9a9aa3; --accent:#7c94f2; --err:#ff8787; }} }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:16px/1.5 system-ui, sans-serif; }}
main {{ max-width:440px; margin:48px auto; padding:24px; background:var(--card); border-radius:12px; }}
h1 {{ font-size:1.3rem; margin-top:0; }}
label {{ display:block; font-weight:600; margin:16px 0 6px; }}
input[type=password] {{ width:100%; box-sizing:border-box; padding:10px; font:inherit; border:1px solid var(--muted); border-radius:8px; background:transparent; color:inherit; }}
.buttons {{ display:flex; gap:12px; margin-top:16px; }}
button {{ flex:1; padding:10px; font:inherit; border-radius:8px; border:1px solid var(--accent); background:var(--accent); color:#fff; cursor:pointer; }}
button.secondary {{ background:transparent; color:var(--fg); border-color:var(--muted); }}
.muted {{ color:var(--muted); font-size:.9rem; }}
.problem {{ color:var(--err); font-weight:600; }}
@media (max-width:480px) {{ main {{ margin:0; border-radius:0; padding:16px; }} }}
</style></head>
<body><main>
{body}
</main></body></html>"#
    );
    let mut response = (status, Html(html)).into_response();
    response.headers_mut().extend(page_headers());
    response
}

/// HTML-escape text for an element or a quoted attribute.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}
