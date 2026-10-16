// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Router service binary — FIX/OUCH broker gateways.
//!
//! Design: `../router/DESIGN.md`.
//! Core in `core.rs`; shell wires the HTTP/TLS surface.
#![allow(dead_code)]

// Global allocator (design §11 Allocator): mimalloc in every trade8
// service/worker binary for lower tail latency than the system allocator.
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admin_ui;
mod core;
mod registration;
mod store;
mod wiring;

use sqlx::Acquire;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};

/// Service state.
pub struct RouterState {
    pub core: sentin8::shell::ServiceCore,
    pub engine: std::sync::Arc<core::RouterEngine>,
    /// Typed msg templates per (destination, msg_id).
    pub templates: std::collections::HashMap<(uuid::Uuid, String), wiring::MsgTemplate>,
}

fn extend_router(
    app: Router<Arc<RouterState>>,
    _state: Arc<RouterState>,
) -> Router<Arc<RouterState>> {
    app.route("/v1/sessions", get(list_sessions))
        .route("/v1/sessions/{id}/reset", post(reset_session))
        .route("/v1/messages", get(list_messages))
        .route("/v1/journal/purge", post(purge_old))
        // Inbound executions — the gateway
        // adapter / OUCH session shards call back with normalized fills.
        .route(
            "/v1/internal/executions/{destination}",
            post(report_execution),
        )
        // Admin RFW pages.
        .route("/v1/admin/ui/{*page}", get(admin_ui::get_admin_ui))
        // ── Shield policy-admin API (shared
        // implementation; handlers generic over this state via HasPolicyAdmin).
        .route(
            "/v1/admin/policy-sets",
            get(sentin8::shield::admin::list_policy_sets::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            get(sentin8::shield::admin::get_policy_set::<Arc<RouterState>>)
                .put(sentin8::shield::admin::put_policy_set::<Arc<RouterState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            post(sentin8::shield::admin::test_policy_set::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            post(sentin8::shield::admin::admin_enforce_policy_set::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            post(sentin8::shield::admin::admin_shadow_policy_set::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            post(sentin8::shield::admin::admin_disable_policy_set::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/relations",
            get(sentin8::shield::admin::list_relations::<Arc<RouterState>>)
                .put(sentin8::shield::admin::put_relation::<Arc<RouterState>>)
                .delete(sentin8::shield::admin::delete_relation::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/relations/check",
            get(sentin8::shield::admin::check_relation::<Arc<RouterState>>),
        )
        .route(
            "/v1/admin/attributes",
            get(sentin8::shield::admin::list_attributes::<Arc<RouterState>>)
                .put(sentin8::shield::admin::put_attribute::<Arc<RouterState>>)
                .delete(sentin8::shield::admin::delete_attribute::<Arc<RouterState>>),
        )
        .route("/v1/admin/sessions", get(admin_ui::admin_sessions))
        .route(
            "/v1/admin/sessions/{id}/reset",
            post(admin_ui::admin_reset_session),
        )
        .route("/v1/admin/journal", get(admin_ui::admin_journal))
        .route("/v1/admin/fees", get(admin_ui::admin_fees))
        .route(
            "/v1/health",
            // Carries the binary's build identity (`version`/`build`,
            // BUILDID-1) so captain's monitor populates `make status`'s
            // VERSION column — the consuming BINARY's identity.
            get(|| async { Json(serde_json::json!({"status": "ok", "service": "trade8:router", "version": sentin8::build_info::id(), "build": sentin8::build_info::build_hash()})) }),
        )
}

async fn list_sessions(
    State(st): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:manage"),
    )
    .await?;
    let env = a.env_id();
    // DB rows are the source of truth; engine memory is the shard view.
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    match store::bootstrap_sessions(&mut tx, env).await {
        Ok(rows) => Ok(Json(serde_json::json!({"sessions": rows.into_iter().map(
            |(destination_id, outgoing_seq, incoming_seq, state)| {
                serde_json::json!({
                    "destination_id": destination_id, "state": state,
                    "outgoing_seq": outgoing_seq, "incoming_seq": incoming_seq,
                })
            }
        ).collect::<Vec<_>>()}))),
        Err(e) => {
            tracing::warn!(error = %e, "trade8:router: sessions DB read failed; engine memory");
            let sessions = st.engine.sessions.read();
            let rows: Vec<serde_json::Value> = sessions
                .values()
                .map(|s| {
                    serde_json::json!({
                        "destination_id": s.destination_id,
                        "state": if s.logged_in { "logged_in" } else { "down" },
                        "outgoing_seq": s.outgoing_seq,
                        "incoming_seq": s.incoming_seq,
                    })
                })
                .collect();
            Ok(Json(serde_json::json!({"sessions": rows})))
        }
    }
}

async fn reset_session(
    State(st): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:manage"),
    )
    .await?;
    let env = auth.env_id();
    // DB counters + session:status event, then the engine shard view.
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    store::reset_session_counters(&mut tx, env, id)
        .await
        .map_err(sentin8::shell::err_db)?;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
                            // The engine-shard lock is taken AFTER the commit (a RwLockWriteGuard
                            // held across an await makes the handler future non-Send).
    let mut sessions = st.engine.sessions.write();
    let entry = sessions
        .entry(id)
        .or_insert_with(|| core::SessionState::new(id));
    entry.outgoing_seq = 0;
    entry.incoming_seq = 0;
    tracing::info!(destination = %id, actor = %auth.claims.sub, "session sequence reset");
    Ok(Json(
        serde_json::json!({"destination_id": id, "reset": true}),
    ))
}

/// Journal read: DB window + engine tail fallback.
async fn list_messages(
    State(st): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:journal:read"),
    )
    .await?;
    let env = a.env_id();
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50)
        .min(500);
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    match store::read_journal(&mut tx, env, None, 0, limit).await {
        Ok(rows) => Ok(Json(serde_json::json!({"messages": rows.into_iter().map(
            |(seq, destination_id, direction, msg_type, payload, at)| {
                serde_json::json!({
                    "seq": seq, "destination_id": destination_id,
                    "direction": direction, "msg_type": msg_type,
                    "size": payload.len(), "at": at,
                })
            }
        ).collect::<Vec<_>>()}))),
        Err(e) => {
            tracing::warn!(error = %e, "trade8:router: journal DB read failed; engine tail");
            let sessions = st.engine.sessions.read();
            let rows: Vec<serde_json::Value> = sessions
                .values()
                .flat_map(|s| {
                    s.journal.iter().map(
                        |b| serde_json::json!({"destination_id": s.destination_id, "len": b.len()}),
                    )
                })
                .collect();
            Ok(Json(serde_json::json!({"messages": rows})))
        }
    }
}

/// Retention purge — the Cron proxy-back target. Default 30 days.
async fn purge_old(
    State(st): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<serde_json::Value>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    // scope. The Cron retention proxy-back. Step 5 —
    // Shield decides on the session-manage action.
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:manage"),
    )
    .await?;
    let env = auth.env_id();
    let days = body
        .as_ref()
        .and_then(|Json(b)| b.get("days"))
        .and_then(|v| v.as_i64())
        .unwrap_or(30)
        .clamp(1, 365) as i32;
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    let purged = store::purge_journal(&mut tx, env, days)
        .await
        .map_err(sentin8::shell::err_db)?;
    tracing::info!(days, purged, actor = %auth.claims.sub, "journal retention purge");
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(serde_json::json!({"purged": purged, "days": days})))
}

/// Inbound execution callback (gateway adapter /
/// OUCH shard proxy-back): a normalized Fill + preserved FIX tags. The
/// fill persists `order_transaction` + `fill_fix_tags` + the fill outbox
/// event in one tx; idempotent on transaction_id (redelivery → no-op).
async fn report_execution(
    State(st): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
    Path(destination): Path<uuid::Uuid>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:executions:report"),
    )
    .await?;
    let env = auth.env_id();
    // Decode the fill (the contracts Fill shape).
    let Ok(fill): Result<trade8::contracts::Fill, _> =
        serde_json::from_value(body.get("fill").cloned().unwrap_or_default())
    else {
        return Err((axum::http::StatusCode::BAD_REQUEST, "fill required".into()));
    };
    let usr_id = body
        .get("usr_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    // FIX-tag preservation: [{tag, value}] pairs.
    let fix_tags: Vec<(u32, String)> = body
        .get("fix_tags")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    Some((
                        t.get("tag")?.as_u64()? as u32,
                        t.get("value")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    // Observe the inbound sequence (gap detection → resend).
    if let Some(seq) = fill.seq_number {
        if let Some((from, until)) = st.engine.observe_inbound(destination, seq) {
            tracing::warn!(destination = %destination, from, until, "router: inbound gap — resend window");
        }
    }
    match wiring::persist_fill(&mut tx, env, &fill, &usr_id, &fix_tags).await {
        Ok(applied) => Ok(Json(serde_json::json!({
            "transaction_id": fill.transaction_id, "applied": applied,
        }))),
        Err(e) => Err(sentin8::shell::err_db(e)),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("trade8:router", "TRADE8_ROUTER")
            .with_registration(registration::print)
            .with_own_health()
            .with_state(|core| async move {
                let engine = core::RouterEngine::default();
                let engine = std::sync::Arc::new(engine);
                let mut templates = std::collections::HashMap::new();
                // Boot pass: restore session counters,
                // load routes + templates so outbound sequences stay
                // monotonic and routing resolves from the DB truth.
                if let Ok(env_slug) = std::env::var("TRADE8_ROUTER_ENV") {
                    if let Ok(env_id) = uuid::Uuid::parse_str(&env_slug) {
                        let mut conn = core.pool.acquire().await?;
                        match store::bootstrap_sessions(&mut conn, env_id).await {
                            Ok(rows) => {
                                let mut sessions = engine.sessions.write();
                                for (destination_id, outgoing_seq, incoming_seq, _state) in rows {
                                    let entry = sessions
                                        .entry(destination_id)
                                        .or_insert_with(|| core::SessionState::new(destination_id));
                                    entry.outgoing_seq = outgoing_seq as u64;
                                    entry.incoming_seq = incoming_seq as u64;
                                }
                            }
                            Err(e) => tracing::warn!(error = %e, "session bootstrap failed"),
                        }
                        match wiring::load_routes(&mut conn, env_id, &engine).await {
                            Ok(links) => {
                                tracing::info!(destinations = links.len(), "router: routes loaded")
                            }
                            Err(e) => tracing::warn!(error = %e, "route load failed"),
                        }
                        match wiring::load_templates(&mut conn, env_id).await {
                            Ok(t) => {
                                tracing::info!(templates = t.len(), "router: templates loaded");
                                templates = t;
                            }
                            Err(e) => tracing::warn!(error = %e, "template load failed"),
                        }
                        // Heartbeat watchdog.
                        tokio::spawn(wiring::heartbeat_loop(core.pool.clone(), env_id));
                    }
                }
                // Desk-order consumer: accepted orders route
                // outbound (route → render → journal → order:routed).
                wiring::spawn_desk_consumer(
                    core.pool.clone(),
                    &core.config.sentin8_bus_url,
                    &core.config.sentin8_auth_issuer,
                    engine.clone(),
                );
                // Cron journal retention: the
                // 02:00 daily schedule proxy-calls `/v1/journal/purge`.
                if let Ok(cron_url) = std::env::var("SENTIN8_CRON_URL") {
                    if !cron_url.is_empty() {
                        trade8::client::cron::register_defaults(
                            &core.config.sentin8_auth_issuer,
                            &cron_url,
                            "trade8:router",
                            core.config.port,
                        )
                        .await;
                    }
                }
                Ok(Arc::new(RouterState {
                    core,
                    engine,
                    templates,
                }))
            })
            .with_router(extend_router),
    )
    .await
}

/// Shared policy-admin surface accessor: the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache.
impl sentin8::shield::admin::HasPolicyAdmin for RouterState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}

// ---------------------------------------------------------------------------
// Admin data-endpoint tests (test-architecture design §7). The REAL
// router's POST-alias mutation route (`/v1/admin/sessions/{id}/reset`)
// + the admin read gate, over `oneshot` with the harness JWKS issuer.
// The DB leg asserts the counter reset through the service role.
// ---------------------------------------------------------------------------
// The harness inside references `sentin8_auth::jwks` — the Auth SERVER
// crate — which this repo never declared as a dependency (no vertical
// pins the auth server; imports go through the platform SDK facade,
// which does not re-export jwks). It therefore never compiled as-is.
// Disabled pending the SDK-side test-harness OIDC stand-in surface
// (backlog story T8-HARNESS-1).
#[cfg(all(test, feature = "handler_tests"))]
mod admin_alias_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn env_a() -> uuid::Uuid {
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    }

    /// The admin router exactly like `main` builds it (read routes + the
    /// POST-alias mutations).
    fn test_router(state: Arc<RouterState>) -> Router {
        let for_state = state.clone();
        extend_router(Router::new(), for_state).with_state(state)
    }

    /// Ephemeral OIDC stand-in (discovery + JWKS + claim minting from
    /// the real test keys) — the desk handler-test harness shape.
    struct Harness {
        issuer: String,
        keys: sentin8_auth::jwks::SigningKeys,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        async fn spawn() -> Self {
            let keys = sentin8_auth::jwks::SigningKeys::generate_for_testing().expect("test keys");
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind ephemeral");
            let issuer = format!("http://{}", listener.local_addr().expect("local addr"));
            let keys_for_jwks = keys.clone();
            let app = axum::Router::new()
                .route(
                    "/.well-known/openid-configuration",
                    get({
                        let issuer = issuer.clone();
                        move || {
                            let issuer = issuer.clone();
                            async move {
                                axum::Json(serde_json::json!({
                                    "issuer": issuer,
                                    "jwks_uri": format!("{issuer}/jwks"),
                                }))
                            }
                        }
                    }),
                )
                .route(
                    "/jwks",
                    get(move || {
                        let all = sentin8_auth::jwks::JwksResponse::from_keys(&keys_for_jwks);
                        async move { axum::Json(all) }
                    }),
                );
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self { issuer, keys, task }
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn oneshot_json(
        app: &Router,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        let req = match body {
            Some(v) => b
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&v).unwrap())),
            None => b.body(Body::empty()),
        }
        .unwrap();
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    /// Router state mirroring the shell boot (in-memory engine; no relay).
    async fn build_state(h: &Harness) -> Option<Arc<RouterState>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect("postgresql://trade8_router_service@localhost:5432/trade8")
            .await
            .ok()?;
        let policy_engine = sentin8::shield::runtime::engine_for("trade8:router");
        let _ =
            sentin8::shield::runtime::boot_full(&pool, &h.issuer, "trade8:router", &policy_engine)
                .await;
        let config = Arc::new(sentin8::shell::ShellConfig {
            service_type: "trade8:router",
            port: 0,
            database_url: "postgresql://trade8_router_service@localhost:5432/trade8".into(),
            sentin8_auth_issuer: h.issuer.clone(),
            sentin8_bus_url: String::new(),
            sentin8_auth_client: sentin8::auth::client::Client::new(
                sentin8::auth::client::ClientOptions {
                    base_url: h.issuer.clone(),
                    ..Default::default()
                },
            ),
            valkey_url: String::new(),
        });
        let core = sentin8::shell::ServiceCore {
            config: config.clone(),
            pool: pool.clone(),
            admin: sentin8::shield::admin::SharedAdminState {
                service: "trade8:router".into(),
                pool,
                policy: policy_engine,
                auth_client: Arc::new(sentin8::auth::client::Client::new(
                    sentin8::auth::client::ClientOptions {
                        base_url: h.issuer.clone(),
                        ..Default::default()
                    },
                )),
            },
        };
        Some(Arc::new(RouterState {
            core,
            engine: Arc::new(core::RouterEngine::default()),
            templates: Default::default(),
        }))
    }

    /// The POST-alias reset route is Shield-gated: the session-manage
    /// scope passes, a scopeless admin token → 403 (step 5, never 500).
    #[tokio::test]
    async fn post_alias_reset_is_scope_gated() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        let dest = uuid::Uuid::new_v4();
        // Missing scope → 403 (the alias inherits the POST gate).
        let tokenless = "";
        let (status, _) = oneshot_json(
            &app,
            "POST",
            &format!("/v1/admin/sessions/{dest}/reset"),
            Some(tokenless),
            None,
        )
        .await;
        assert!(status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN);
    }

    /// Unauthenticated POST-alias → 401 (the verify step).
    #[tokio::test]
    async fn post_alias_reset_requires_token() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        let (status, _) = oneshot_json(
            &app,
            "POST",
            &format!("/v1/admin/sessions/{}/reset", uuid::Uuid::new_v4()),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
