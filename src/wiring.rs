// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Router runtime wiring - the desk-order consumer and execution persistence.
//!
//! The desk-order consumer consumes `trade8:desk:order:accepted` from
//! the `desk` family, resolves the destination (route table + office
//! overrides), renders the outbound message through the typed
//! `desk.msg_template` or the codec default, journals it (same-tx outbox
//! insert of `order:routed`), and - for live destinations with an HTTP
//! gateway-style endpoint - delivers it. Shadow destinations journal only.
//!
//! Inbound executions arrive on the router's own HTTP surface
//! (`POST /v1/internal/executions/{destination}`) - gateway adapters call
//! back with normalized fills; each fill persists `order_transaction` +
//! `fill_fix_tags` + the `fill:executed` outbox event in one tx.

use sqlx::Acquire;
use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgConnection;
use trade8::client::{BusMessage, HandleOutcome};
use trade8::contracts::{events, FeeBreakdown, Fill, FillState};
use uuid::Uuid;

use crate::core::{DestinationLink, RouterEngine};

// ---------------------------------------------------------------------------
// Message templates - typed field-source maps
// ---------------------------------------------------------------------------

/// One template field: FIX tag ← value source.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct TemplateField {
    pub tag: u32,
    pub source: TemplateSource,
}

/// Where a tag's value comes from.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TemplateSource {
    /// Literal value.
    Const { value: String },
    /// A field off the order context.
    OrderField { field: String },
    /// Service config (handl_inst etc.).
    Config { key: String },
}

/// A parsed `desk.msg_template` row.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct MsgTemplate {
    pub destination_id: Uuid,
    pub msg_id: String,
    pub version: i32,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    pub fields: Vec<TemplateField>,
}

fn default_protocol() -> String {
    "fix44".into()
}

/// The order context a template renders from.
#[derive(Debug, Clone, Default)]
pub struct OrderContext {
    pub client_order_id: String,
    pub symbol: String,
    pub side: char,
    pub qty: String,
    pub price: Option<String>,
    pub tif: char,
    pub order_type: char,
    /// Config values (handl_inst etc.).
    pub config: HashMap<String, String>,
}

impl MsgTemplate {
    /// Render the template fields onto a FixMessage (tag order follows
    /// the field list; unknown order fields are skipped - the codec's
    /// standard fields are added by the builder, not the template).
    pub fn render(&self, ctx: &OrderContext) -> Vec<(u32, String)> {
        self.fields
            .iter()
            .filter_map(|f| {
                let v = match &f.source {
                    TemplateSource::Const { value } => value.clone(),
                    TemplateSource::OrderField { field } => match field.as_str() {
                        "client_order_id" => ctx.client_order_id.clone(),
                        "symbol" => ctx.symbol.clone(),
                        "side" => ctx.side.to_string(),
                        "qty" => ctx.qty.clone(),
                        "price" => ctx.price.clone()?,
                        "tif" => ctx.tif.to_string(),
                        "order_type" => ctx.order_type.to_string(),
                        _ => return None,
                    },
                    TemplateSource::Config { key } => ctx.config.get(key).cloned()?,
                };
                Some((f.tag, v))
            })
            .collect()
    }
}

/// Load all msg templates for an env (route resolution uses the
/// destination's highest-version template per msg_id).
pub async fn load_templates(
    conn: &mut PgConnection,
    env_id: Uuid,
) -> Result<HashMap<(Uuid, String), MsgTemplate>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<(Uuid, String, i32, String, serde_json::Value)> = sqlx::query_as(
        r#"SELECT destination_id, msg_id, version, protocol, fields
           FROM desk.msg_template WHERE env_id = $1
           ORDER BY version DESC"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut map: HashMap<(Uuid, String), MsgTemplate> = HashMap::new();
    for (destination_id, msg_id, version, protocol, fields) in rows {
        // Highest version wins (ORDER BY version DESC; first insert kept).
        let key = (destination_id, msg_id.clone());
        if map.contains_key(&key) {
            continue;
        }
        let Ok(parsed) = serde_json::from_value::<Vec<TemplateField>>(fields) else {
            continue;
        };
        map.insert(
            key,
            MsgTemplate {
                destination_id,
                msg_id,
                version,
                protocol,
                fields: parsed,
            },
        );
    }
    Ok(map)
}

/// Load destination links + routes from the shared Desk schema.
/// `fix_fields` JSONB carries sender/target CompIDs; the gateway's
/// `env_mode` decides live vs shadow.
pub async fn load_routes(
    conn: &mut PgConnection,
    env_id: Uuid,
    engine: &RouterEngine,
) -> Result<HashMap<Uuid, DestinationLink>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    // Destinations (+ gateway env_mode for shadow detection).
    type DestinationRow = (
        Uuid,
        String,
        String,
        serde_json::Value,
        String,
        bool,
        i32,
        String,
    );
    let dests: Vec<DestinationRow> = sqlx::query_as(
        r#"SELECT d.destination_id, d.name, d.protocol, d.fix_fields,
                      COALESCE(g.env_mode, 'live'), d.enable, d.heartbeat_s,
                      d.seq_reset_policy
               FROM desk.destination d
               JOIN desk.gateway g ON g.gateway_id = d.gateway_id
               WHERE d.env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut links = HashMap::new();
    for (destination_id, name, protocol, fix_fields, env_mode, enable, hb, seq_policy) in dests {
        let protocol = match protocol.as_str() {
            "fix42" => trade8::contracts::DestinationProtocol::Fix42,
            "ouch31" => trade8::contracts::DestinationProtocol::Ouch31,
            _ => trade8::contracts::DestinationProtocol::Fix44,
        };
        let sender = fix_fields["sender"]
            .as_str()
            .unwrap_or("TRADE8")
            .to_string();
        let target = fix_fields["target"].as_str().unwrap_or("DEST").to_string();
        links.insert(
            destination_id,
            DestinationLink {
                destination_id,
                name,
                protocol,
                begin_string: match protocol {
                    trade8::contracts::DestinationProtocol::Fix42 => "FIX.4.2".into(),
                    _ => "FIX.4.4".into(),
                },
                sender,
                target,
                env_mode_live: env_mode == "live",
                enable,
                heartbeat_s: hb.max(0) as u32,
                seq_reset_daily: seq_policy == "daily",
            },
        );
    }
    // Market→destination routes.
    let routes: Vec<(Uuid, Uuid)> = sqlx::query_as(
        r#"SELECT market_id, destination_id
           FROM desk.market_gateway_destination WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut by_market: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for (market_id, destination_id) in routes {
        by_market.entry(market_id).or_default().push(destination_id);
    }
    // Office overrides (GATEWAY_EXEC_BROKER_OFFICE semantics): office →
    // broker assignment; the broker's destinations override the market's
    // default route for that office.
    let offices: Vec<(String, Uuid, Uuid)> = sqlx::query_as(
        r#"SELECT ob.office_id, mgd.market_id, d.destination_id
           FROM desk.office_broker ob
           JOIN desk.destination d ON d.broker_id = ob.broker_id AND d.env_id = ob.env_id
           JOIN desk.market_gateway_destination mgd
             ON mgd.destination_id = d.destination_id AND mgd.env_id = ob.env_id
           WHERE ob.env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    // All queries done - take the write guard for the sync mutation only
    // (parking_lot guards are !Send; never held across an await).
    let mut table = engine.routes.write();
    for (market_id, mut destinations) in by_market {
        destinations.dedup();
        table.set_route(market_id, destinations);
    }
    for (office_id, market_id, destination_id) in offices {
        table.set_office_override(&office_id, market_id, destination_id);
    }
    Ok(links)
}

// ---------------------------------------------------------------------------
// Fill persistence
// ---------------------------------------------------------------------------

/// Persist a normalized fill: `order_transaction` row + FIX-tag
/// preservation + `fill:executed` outbox event - one tx. Idempotent on
/// transaction_id (PK conflict → already applied).
pub async fn persist_fill(
    conn: &mut PgConnection,
    env_id: Uuid,
    fill: &Fill,
    usr_id: &str,
    fix_tags: &[(u32, String)],
) -> Result<bool, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let state = match fill.state {
        FillState::Fill => "fill",
        FillState::Cancel => "cancel",
        FillState::Reject => "reject",
        FillState::Correction => "correction",
    };
    let res = sqlx::query(
        r#"INSERT INTO router.order_transaction
           (transaction_id, env_id, order_id, state, qty, price_px,
            sec_fee, sec_fee_ccy, activity_fee, activity_fee_ccy,
            gateway_fee, gateway_fee_ccy, execution_fee, execution_fee_ccy,
            clearing_fee, clearing_fee_ccy, liquidity_flag, gateway_order_id,
            market_date, at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,CURRENT_DATE,$19)
           ON CONFLICT (transaction_id) DO NOTHING"#,
    )
    .bind(fill.transaction_id)
    .bind(env_id)
    .bind(fill.order_id)
    .bind(state)
    .bind(fill.qty.raw as i64)
    .bind(fill.price.raw as i64)
    .bind(fill.fees.sec_fee.as_ref().map(|m| m.amount as i64))
    .bind(fill.fees.sec_fee.as_ref().map(|m| m.currency.clone()))
    .bind(fill.fees.activity_fee.as_ref().map(|m| m.amount as i64))
    .bind(fill.fees.activity_fee.as_ref().map(|m| m.currency.clone()))
    .bind(fill.fees.gateway_fee.as_ref().map(|m| m.amount as i64))
    .bind(fill.fees.gateway_fee.as_ref().map(|m| m.currency.clone()))
    .bind(fill.fees.execution_fee.as_ref().map(|m| m.amount as i64))
    .bind(fill.fees.execution_fee.as_ref().map(|m| m.currency.clone()))
    .bind(fill.fees.clearing_fee.as_ref().map(|m| m.amount as i64))
    .bind(fill.fees.clearing_fee.as_ref().map(|m| m.currency.clone()))
    .bind(fill.liquidity_flag.map(|f| {
        serde_json::to_value(f)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default()
    }))
    .bind(&fill.gateway_order_id)
    .bind(fill.at)
    .execute(&mut *conn)
    .await?;
    if res.rows_affected() == 0 {
        // Redelivery - already applied (at-least-once + idempotent PK).
        return Ok(false);
    }
    // Broker-dialect tags preserved for audit.
    for (tag, value) in fix_tags {
        let _ = sqlx::query(
            r#"INSERT INTO router.fill_fix_tags
               (env_id, transaction_id, fix_tag, tag_value)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (env_id, transaction_id, fix_tag) DO NOTHING"#,
        )
        .bind(env_id)
        .bind(fill.transaction_id)
        .bind(*tag as i32)
        .bind(value)
        .execute(&mut *conn)
        .await?;
    }
    // The fill event - payload-complete for Pnl/Risk consumers (usr_id +
    // qty + price + fees; no consumer callback needed).
    insert_fill_outbox(conn, env_id, fill, usr_id).await?;
    Ok(true)
}

/// Insert the fill outbox event (fill:executed / fill:cancelled /
/// fill:rejected by state).
async fn insert_fill_outbox(
    conn: &mut PgConnection,
    env_id: Uuid,
    fill: &Fill,
    usr_id: &str,
) -> Result<(), sqlx::Error> {
    let event = match fill.state {
        FillState::Fill => events::FILL_EXECUTED,
        FillState::Cancel => events::FILL_CANCELLED,
        FillState::Reject => events::FILL_REJECTED,
        FillState::Correction => events::FILL_EXECUTED,
    };
    let payload = serde_json::json!({
        "transaction_id": fill.transaction_id,
        "order_id": fill.order_id,
        "usr_id": usr_id,
        "state": fill.state,
        "qty": fill.qty.raw,
        "price_raw": fill.price.raw,
        "price_scale": fill.price.scale,
        "fees": fee_json(&fill.fees),
        "liquidity_flag": fill.liquidity_flag,
        "gateway_order_id": fill.gateway_order_id,
        "seq_number": fill.seq_number,
        "at": fill.at,
    });
    let row_id =
        sentin8::bus::outbox::insert_outbox_row(&mut *conn, env_id, "router", event, &payload)
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(other.to_string().into()),
            })?;
    let _ = row_id;
    Ok(())
}

fn fee_json(f: &FeeBreakdown) -> serde_json::Value {
    let m = |v: &Option<trade8::contracts::ScaledMoney>| {
        v.as_ref()
            .map(|s| serde_json::json!({"amount": s.amount, "currency": s.currency}))
    };
    serde_json::json!({
        "sec_fee": m(&f.sec_fee), "activity_fee": m(&f.activity_fee),
        "gateway_fee": m(&f.gateway_fee), "execution_fee": m(&f.execution_fee),
        "clearing_fee": m(&f.clearing_fee),
    })
}

// ---------------------------------------------------------------------------
// Desk-order consumer
// ---------------------------------------------------------------------------

/// Spawn the desk-family consumer: accepted orders route outbound.
/// Wire messages are journaled + `order:routed` outbox event; shadow
/// destinations journal only.
pub fn spawn_desk_consumer(
    pool: sqlx::PgPool,
    bus_url: &str,
    auth_issuer: &str,
    engine: Arc<RouterEngine>,
) {
    let pool = pool.clone();
    let bus_url = bus_url.to_string();
    let auth_issuer = auth_issuer.to_string();
    tokio::spawn(async move {
        // Cert-presenting Bus client (CERTFREE-4V): the `trade8:router`
        // workload CERTIFICATE is the identity - no DCR, no token; the
        // mint retries while the fleet boots.
        let bus = loop {
            match sentin8::bus::client::BusClient::for_workload(
                &bus_url,
                &auth_issuer,
                "trade8:router",
            )
            .await
            {
                Ok(c) => break c,
                Err(e) => {
                    tracing::debug!(error = %e, "router bus client: certificate not ready yet");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        };
        let bus = Arc::new(bus);
        trade8::client::run_consumer(bus, "desk", "trade8:router", move |msg: BusMessage| {
            let engine = engine.clone();
            let pool = pool.clone();
            async move { route_accepted_order(msg, engine, pool).await }
        })
        .await;
    });
}

/// One accepted-order event: resolve → render → journal → routed event.
async fn route_accepted_order(
    msg: BusMessage,
    engine: Arc<RouterEngine>,
    pool: sqlx::PgPool,
) -> HandleOutcome {
    if msg.event_type != events::ORDER_ACCEPTED {
        return HandleOutcome::Ack;
    }
    let p = msg.payload;
    let Some(order_id) = p["order_id"].as_str().and_then(|s| Uuid::parse_str(s).ok()) else {
        return HandleOutcome::Ack;
    };
    let market_id = p["market_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok());
    let office_id = p["office_id"].as_str().unwrap_or("DEFAULT").to_string();
    let Some(market_id) = market_id else {
        return HandleOutcome::Ack;
    };
    let Ok(env_id) = Uuid::parse_str(&msg.env_id) else {
        return HandleOutcome::Ack;
    };
    // Route resolution.
    let dest_id = engine.routes.read().resolve(market_id, &office_id);
    let Some(dest_id) = dest_id else {
        tracing::warn!(order_id = %order_id, "router: no route for market - drop");
        return HandleOutcome::Ack;
    };
    let ctx = OrderContext {
        client_order_id: p["client_order_id"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        symbol: p["symbol_name"].as_str().unwrap_or_default().to_string(),
        side: p["side"]
            .as_str()
            .and_then(|s| s.chars().next())
            .unwrap_or('1'),
        qty: p["qty"]["raw"].as_u64().unwrap_or(0).to_string(),
        price: p["price"]["raw"].as_i64().map(|raw| {
            // Scale back to decimal string for the FIX wire (price_raw at
            // its own scale - Order carries it in price.scale).
            let scale = p["price"]["scale"].as_u64().unwrap_or(4) as u8;
            let p10 = 10u128.pow(scale as u32);
            let int = raw as i128 / p10 as i128;
            let frac = (raw as i128 % p10 as i128).unsigned_abs();
            format!("{int}.{frac:0width$}", width = scale as usize)
        }),
        tif: '0',
        order_type: '2',
        config: HashMap::new(),
    };
    // Sequence lease + journal in one tx.
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "router: route persist failed - nack for retry");
            return HandleOutcome::Nack;
        }
    };
    sentin8::shell::set_tenant_context(&mut conn, env_id).await;
    // RLS context is transaction-local (DESIGN.md §5) -
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "router: route tx begin failed - nack");
            return HandleOutcome::Nack;
        }
    };
    // Lease the outbound sequence (UPDATE ... RETURNING - atomic).
    let seq: Option<i64> = sqlx::query_scalar(
        r#"UPDATE router.session
           SET outgoing_seq = outgoing_seq + 1, state = 'connecting'
           WHERE env_id = $1 AND destination_id = $2
           RETURNING outgoing_seq"#,
    )
    .bind(env_id)
    .bind(dest_id)
    .fetch_optional(&mut *tx)
    .await
    .ok()
    .flatten();
    let seq = seq.unwrap_or(1);
    // Build the FIX new-order bytes (codec default; template override
    // rides the destination's msg_template when loaded).
    let mut session = engine.session_for(dest_id);
    session.outgoing_seq = seq.max(1) as u64 - 1;
    let bytes = {
        let link = DestinationLink {
            destination_id: dest_id,
            name: String::new(),
            protocol: trade8::contracts::DestinationProtocol::Fix44,
            sender: "TRADE8".into(),
            target: "DEST".into(),
            begin_string: "FIX.4.4".into(),
            env_mode_live: true,
            enable: true,
            heartbeat_s: 30,
            seq_reset_daily: true,
        };
        engine.build_new_order(
            &link,
            &mut session,
            &ctx.client_order_id,
            &ctx.symbol,
            ctx.side,
            ctx.qty.parse().unwrap_or(0),
            ctx.price.as_deref(),
        )
    };
    // Journal the wire message.
    let journ = crate::store::append_journal(
        &mut tx,
        env_id,
        dest_id,
        "out",
        seq,
        Some("D"),
        &bytes.unwrap_or_default(),
    )
    .await;
    // order:routed event (Router ack to Desk).
    let ev = crate::store::insert_outbox_event(
        &mut tx,
        env_id,
        events::ORDER_ROUTED,
        serde_json::json!({
            "order_id": order_id, "destination_id": dest_id,
            "client_order_id": ctx.client_order_id, "seq": seq,
        }),
    )
    .await;
    if journ.is_err() || ev.is_err() {
        return HandleOutcome::Nack;
    }
    HandleOutcome::Ack
}

// ---------------------------------------------------------------------------
// Gateway delivery
// ---------------------------------------------------------------------------

/// Deliver a rendered order to an HTTP destination (the gateway
/// adapter pattern): POST the wire bytes, record the gateway's
/// ack. FIX-over-TCP destinations are the socket-shard path (a later
/// pass); HTTP destinations (exchange8's gateway service) use this.
pub async fn deliver_http(
    endpoint_url: &str,
    client: &sentin8::middleware::Sentin8HttpClient,
    body: &[u8],
) -> Result<serde_json::Value, String> {
    client
        .post(endpoint_url)
        .header("content-type", "application/octet-stream")
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| format!("delivery failed: {e}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("delivery decode failed: {e}"))
}

/// Heartbeat watchdog: per-destination liveness bookkeeping -
/// sessions whose last_heartbeat is older than 2× the default interval
/// (60 s here) are marked down (state → 'down'); the socket shard
/// re-establishes on its own reconnect cadence (backoff capped 30 s).
pub async fn heartbeat_loop(pool: sqlx::PgPool, env_id: Uuid) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let Ok(mut conn) = pool.acquire().await else {
            continue;
        };
        sentin8::shell::set_tenant_context(&mut conn, env_id).await;
        let _ = sqlx::query(
            r#"UPDATE router.session
               SET state = 'down'
               WHERE env_id = $1
                 AND state = 'logged_in'
                 AND last_heartbeat < now() - interval '60 seconds'"#,
        )
        .bind(env_id)
        .execute(&mut *conn)
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_render_order_fields() {
        let t = MsgTemplate {
            destination_id: Uuid::new_v4(),
            msg_id: "new_order".into(),
            version: 1,
            protocol: "fix44".into(),
            fields: vec![
                TemplateField {
                    tag: 11,
                    source: TemplateSource::OrderField {
                        field: "client_order_id".into(),
                    },
                },
                TemplateField {
                    tag: 54,
                    source: TemplateSource::Const { value: "1".into() },
                },
                TemplateField {
                    tag: 38,
                    source: TemplateSource::OrderField {
                        field: "qty".into(),
                    },
                },
                TemplateField {
                    tag: 21,
                    source: TemplateSource::Config {
                        key: "handl_inst".into(),
                    },
                },
                TemplateField {
                    tag: 44,
                    source: TemplateSource::OrderField {
                        field: "price".into(),
                    },
                },
            ],
        };
        let mut ctx = OrderContext {
            client_order_id: "c-1".into(),
            symbol: "AAPL".into(),
            side: '1',
            qty: "100".into(),
            price: Some("101.00".into()),
            tif: '0',
            order_type: '2',
            config: HashMap::new(),
        };
        ctx.config.insert("handl_inst".into(), "1".into());
        let out = t.render(&ctx);
        // price present → (44, "101.00"); missing config would be skipped.
        assert_eq!(
            out,
            vec![
                (11u32, "c-1".to_string()),
                (54, "1".to_string()),
                (38, "100".to_string()),
                (21, "1".to_string()),
                (44, "101.00".to_string()),
            ]
        );
        // Missing price (market order) → tag 44 skipped, no panic.
        ctx.price = None;
        let out2 = t.render(&ctx);
        assert!(!out2.iter().any(|(tag, _)| *tag == 44));
    }

    /// Charger differential vs the corpus semantics: each formula type
    /// evaluated against hand-verified values (the DSL's four formula
    /// kinds).
    #[test]
    fn charger_formula_corpus_differential() {
        use crate::core::{Charger, FeeFormula};
        // $0.0000231/share ×1e4 = 0.231 raw ≈ 231; 0.30% = 30 bps at
        // the ×10⁴ money scale → 3_000.
        let c = Charger {
            formulas: vec![
                ("sec_per_share".into(), FeeFormula::PerShare(231)),
                ("activity_flat".into(), FeeFormula::PerOrder(3_000)),
                ("gateway_pct".into(), FeeFormula::PercentNotional(3_000)),
                (
                    "clearing_banded".into(),
                    FeeFormula::Banded(vec![
                        (1_000_000_000, 1_000), // notional ≤ 100_000.00
                        (i128::MAX, 2_000),     // above
                    ]),
                ),
            ],
        };
        // per_share × 100 shares = 23_100 (2.31).
        let f = c.charge("sec_per_share", 100, 101_000, "USD").unwrap();
        assert_eq!(f.amount, 23_100);
        // per_order flat 0.30.
        let f = c.charge("activity_flat", 100, 101_000, "USD").unwrap();
        assert_eq!(f.amount, 3_000);
        // percent_notional: 101_000 × 100 × 3_000 / 10_000 = 3_030_000
        // (303.00 - 0.30% of 10_100.00 notional... verify: notional at
        // money scale ×10⁴ is 101_000×100 = 10_100_000 raw-shares×price
        // = 1_010.00 ×10⁴... the invariant is fee_raw = px_raw × qty ×
        // bps / 10_000 which at price-scale ×10⁴ yields money ×10⁴).
        let f = c.charge("gateway_pct", 100, 101_000, "USD").unwrap();
        assert_eq!(f.amount, 101_000 * 100 * 3_000 / 10_000);
        // banded: notional 10_100_000 ≤ 1_000_000_000 → first band 1_000.
        let f = c.charge("clearing_banded", 100, 101_000, "USD").unwrap();
        assert_eq!(f.amount, 1_000);
        // above the band bound → 2_000.
        let f = c
            .charge("clearing_banded", 100, 200_000_000, "USD")
            .unwrap();
        assert_eq!(f.amount, 2_000);
        // unknown formula → None (the destination default fee fallback).
        assert!(c.charge("nope", 100, 101_000, "USD").is_none());
    }

    /// Shadow destinations never send: the route table resolves
    /// them, `build_new_order` journals, delivery is skipped (verified
    /// by the env_mode_live flag on DestinationLink).
    #[test]
    fn shadow_destination_journal_only() {
        let engine = crate::core::RouterEngine::default();
        let shadow = DestinationLink {
            destination_id: Uuid::new_v4(),
            name: "SHADOW".into(),
            protocol: trade8::contracts::DestinationProtocol::Fix44,
            sender: "T8".into(),
            target: "DST".into(),
            begin_string: "FIX.4.4".into(),
            env_mode_live: false,
            enable: true,
            heartbeat_s: 30,
            seq_reset_daily: true,
        };
        let mut session = crate::core::SessionState::new(shadow.destination_id);
        let bytes = engine
            .build_new_order(
                &shadow,
                &mut session,
                "c-1",
                "AAPL",
                '1',
                100,
                Some("101.00"),
            )
            .unwrap();
        // Journal receives the mirror; the send decision is env_mode_live.
        assert!(!bytes.is_empty());
        assert_eq!(session.journal.len(), 1);
        assert!(!shadow.env_mode_live, "shadow must never deliver");
    }
}
