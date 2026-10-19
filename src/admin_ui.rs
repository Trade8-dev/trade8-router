// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Router admin UI - RFW pages + data endpoints.
//!
//! Secrets (credentials_ref) are never displayed - only their existence.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use sentin8::rfw::page::AdminPage;
use sentin8::rfw::response::AdminUiResponse;
use sentin8::rfw::widget::*;

use crate::RouterState;

fn cols(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: true,
        width: None,
        format: None,
    }
}
fn col(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: false,
        width: None,
        format: None,
    }
}
fn table(columns: Vec<WidgetDataTableColumn>, endpoint: &str) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 25,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions: vec![],
    })
}
fn card(title: &str, body: &str) -> RemoteWidget {
    RemoteWidget::Card(WidgetCard {
        title: Some(title.into()),
        subtitle: None,
        children: vec![RemoteWidget::Text(WidgetText {
            text: body.into(),
            text_type: WidgetTextType::Body,
            align: TextAlign::Left,
            max_lines: None,
        })],
        actions: vec![],
    })
}
#[utoipa::path(
    get,
    path = "/v1/admin/ui/{page}",
    responses(
        (status = 200, description = "OK", body = sentin8::rfw::AdminUiResponse),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/ui/{page}` - RFW definitions.
pub async fn get_admin_ui(
    State(_st): State<std::sync::Arc<RouterState>>,
    headers: HeaderMap,
    Path(page): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&_st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &_st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:read"),
    )
    .await?;
    let def = match page.as_str() {
        "sessions" => AdminPage::new("Broker Sessions")
            .widget(card(
                "Sessions Wall",
                "Per-destination FIX/OUCH session state with sequence counters. Reset via the POST alias.",
            ))
            .widget(table(
                vec![
                    cols("destination_id", "Destination"),
                    cols("state", "State"),
                    cols("outgoing_seq", "Out Seq"),
                    cols("incoming_seq", "In Seq"),
                    col("last_heartbeat", "Last Heartbeat"),
                ],
                "/v1/admin/sessions",
            ))
            .build(),
        "journal" => AdminPage::new("Message Journal")
            .widget(table(
                vec![
                    cols("seq", "Seq"),
                    cols("destination_id", "Destination"),
                    cols("direction", "Dir"),
                    cols("msg_type", "Type"),
                    col("at", "At"),
                ],
                "/v1/admin/journal",
            ))
            .build(),
        "fees" => AdminPage::new("Fee Formulas")
            .widget(card(
                "Charger DSL",
                "Formula types: per_share / per_order / percent_notional / banded - destination-default fallback applies when no formula matches.",
            ))
            .widget(table(
                vec![
                    cols("name", "Formula"),
                    col("kind", "Type"),
                    col("params", "Parameters"),
                ],
                "/v1/admin/fees",
            ))
            .build(),
        _ => {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("Unknown admin UI page: {page}"),
            ))
        }
    };
    Ok(Json(
        serde_json::to_value(AdminUiResponse::page(def)).unwrap_or_default(),
    ))
}

type Qp = std::collections::HashMap<String, String>;
#[utoipa::path(
    get,
    path = "/v1/admin/sessions",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/sessions` - session rows from the DB.
pub async fn admin_sessions(
    State(st): State<std::sync::Arc<RouterState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    type AdminSessionRow = (
        uuid::Uuid,
        i64,
        i64,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let rows: Vec<AdminSessionRow> = sqlx::query_as(
        r#"SELECT destination_id, outgoing_seq, incoming_seq, state, last_heartbeat
               FROM router.session WHERE env_id = $1
               ORDER BY destination_id"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(
            |(destination_id, outgoing_seq, incoming_seq, state, last_heartbeat)| {
                serde_json::json!({
                    "destination_id": destination_id, "state": state,
                    "outgoing_seq": outgoing_seq, "incoming_seq": incoming_seq,
                    "last_heartbeat": last_heartbeat,
                })
            }
        )
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/journal",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/journal` - journal window (latest N frames, hex preview).
pub async fn admin_journal(
    State(st): State<std::sync::Arc<RouterState>>,
    headers: HeaderMap,
    Query(q): Query<Qp>,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
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
    let dest = q
        .get("destination_id")
        .and_then(|v| uuid::Uuid::parse_str(v).ok());
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let rows = crate::store::read_journal(&mut conn, env, dest, 0, limit)
        .await
        .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(|(seq, destination_id, direction, msg_type, payload, at)| {
            // hex preview - the wall never shows raw control bytes
            let preview = payload
                .iter()
                .take(48)
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join("");
            serde_json::json!({
                "seq": seq, "destination_id": destination_id,
                "direction": direction, "msg_type": msg_type,
                "preview_hex": preview, "at": at,
            })
        })
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/fees",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/fees` - charger formula table (engine copy).
pub async fn admin_fees(
    State(st): State<std::sync::Arc<RouterState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:read"),
    )
    .await?;
    let formulas = st.engine.charger.formulas.clone();
    let rows: Vec<_> = formulas
        .iter()
        .map(|(name, f)| {
            let (kind, params) = match f {
                crate::core::FeeFormula::PerShare(v) => ("per_share", format!("{v} / share")),
                crate::core::FeeFormula::PerOrder(v) => ("per_order", format!("{v} flat")),
                crate::core::FeeFormula::PercentNotional(bps) => {
                    ("percent_notional", format!("{bps} bps"))
                }
                crate::core::FeeFormula::Banded(bands) => (
                    "banded",
                    bands
                        .iter()
                        .map(|(upper, fee)| format!("≤{upper}:{fee}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            };
            serde_json::json!({"name": name, "kind": kind, "params": params})
        })
        .collect();
    Ok(Json(rows))
}
#[utoipa::path(
    post,
    path = "/v1/admin/sessions/{id}/reset",
    responses(
        (status = 200, description = "Session reset", body = crate::SessionResetAck),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `POST /v1/admin/sessions/{id}/reset` - RFW POST-alias reset.
pub async fn admin_reset_session(
    State(st): State<std::sync::Arc<RouterState>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<crate::SessionResetAck>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:router:session:manage"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    crate::store::reset_session_counters(&mut conn, env, id)
        .await
        .map_err(sentin8::shell::err_db)?;
    let mut sessions = st.engine.sessions.write();
    if let Some(entry) = sessions.get_mut(&id) {
        entry.outgoing_seq = 0;
        entry.incoming_seq = 0;
    }
    tracing::info!(destination = %id, actor = %a.claims.sub, "session reset (admin)");
    Ok(Json(crate::SessionResetAck {
        destination_id: id,
        reset: true,
    }))
}
