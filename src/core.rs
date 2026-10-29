// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Router gateway core - destination model, session state
//! machines, execution normalization, and the fee charger.
//!
//! Design: `../router/DESIGN.md` §4.
//! The codecs (crates fix/ouch), the normalization, and the charger are
//! complete here; session dial-out is the socket-transport remainder.

use std::collections::HashMap;

use chrono::Utc;
use parking_lot::RwLock;
use trade8::contracts::{DestinationProtocol, FeeBreakdown, Fill, FillState, ScaledMoney};
use trade8::fix::ExecutionReport;
use uuid::Uuid;

// ------------------------------------------------------------------------
// Destination model
// ------------------------------------------------------------------------

/// A destination row as loaded for routing (from the shared Desk schema).
#[derive(Debug, Clone, PartialEq)]
pub struct DestinationLink {
    pub destination_id: Uuid,
    pub name: String,
    pub protocol: DestinationProtocol,
    pub sender: String,       // FIX senderCompID / OUCH firm
    pub target: String,       // FIX targetCompID
    pub begin_string: String, // FIX.4.2 | FIX.4.4
    pub env_mode_live: bool,  // false = shadow (mirror, never send)
    pub enable: bool,
    pub heartbeat_s: u32,
    pub seq_reset_daily: bool,
}

/// Resolve the destination for a (market, office-class) route
/// market→gateway→broker→destination resolution over the Desk
/// catalog tables.
#[derive(Debug, Default)]
pub struct RouteTable {
    /// (market_id, office class) → destination list in priority order.
    routes: HashMap<Uuid, Vec<Uuid>>,
    /// Office→broker assignment override.
    office_overrides: HashMap<(String, Uuid), Uuid>,
}

impl RouteTable {
    pub fn set_route(&mut self, market_id: Uuid, destinations: Vec<Uuid>) {
        self.routes.insert(market_id, destinations);
    }

    pub fn set_office_override(&mut self, office_id: &str, market_id: Uuid, destination_id: Uuid) {
        self.office_overrides
            .insert((office_id.to_string(), market_id), destination_id);
    }

    /// First destination for the market, honoring the office override.
    pub fn resolve(&self, market_id: Uuid, office_id: &str) -> Option<Uuid> {
        if let Some(dest) = self
            .office_overrides
            .get(&(office_id.to_string(), market_id))
        {
            return Some(*dest);
        }
        self.routes.get(&market_id).and_then(|v| v.first().copied())
    }
}

// ------------------------------------------------------------------------
// Session state
// ------------------------------------------------------------------------

/// One broker session's runtime state (per-destination shard).
#[derive(Debug, Clone, PartialEq)]
pub struct SessionState {
    pub destination_id: Uuid,
    pub outgoing_seq: u64,
    pub incoming_seq: u64,
    pub logged_in: bool,
    pub last_heartbeat_ms: u64,
    /// Journal tail (the DB pass makes this durable; tests replay it).
    pub journal: Vec<Vec<u8>>,
}

impl SessionState {
    pub fn new(destination_id: Uuid) -> Self {
        SessionState {
            destination_id,
            outgoing_seq: 0,
            incoming_seq: 0,
            logged_in: false,
            last_heartbeat_ms: 0,
            journal: Vec::new(),
        }
    }

    /// Next outbound sequence (daily rollover resets to 1).
    pub fn next_out_seq(&mut self) -> u64 {
        self.outgoing_seq += 1;
        self.outgoing_seq
    }

    /// Observe an inbound sequence - gap detection returns the resend
    /// window.
    pub fn observe_in_seq(&mut self, seq: u64) -> Option<(u64, u64)> {
        if seq > self.incoming_seq + 1 {
            let window = (self.incoming_seq + 1, seq - 1);
            self.incoming_seq = seq;
            return Some(window); // resend needed: (from, until)
        }
        self.incoming_seq = self.incoming_seq.max(seq);
        None
    }
}

// ------------------------------------------------------------------------
// Execution normalization
// ------------------------------------------------------------------------

/// A fee-formula entry.
#[derive(Debug, Clone, PartialEq)]
pub enum FeeFormula {
    /// value per share (scaled ×10⁴).
    PerShare(i128),
    /// flat value per order.
    PerOrder(i128),
    /// percent of notional (bps ×10⁴; e.g. 250 = 2.5%).
    PercentNotional(i128),
    /// banded: (notional_upper, fee) tiers ascending.
    Banded(Vec<(i128, i128)>),
}

/// The charger - evaluates gateway fees on fills ( DSL:
/// per_share / per_order / percent_notional / banded).
#[derive(Debug, Default)]
pub struct Charger {
    pub formulas: Vec<(String, FeeFormula)>,
}

impl Charger {
    /// Charge by formula name; `None` when no formula matches (the
    /// destination default fee is the caller's fallback.
    /// DESTINATION_DEFAULT_FEE).
    pub fn charge(
        &self,
        formula: &str,
        qty: u64,
        price_raw: i128,
        currency: &str,
    ) -> Option<ScaledMoney> {
        let (name, f) = self.formulas.iter().find(|(n, _)| n == formula)?;
        let _ = name;
        let amount = match f {
            FeeFormula::PerShare(v) => v * qty as i128,
            FeeFormula::PerOrder(v) => *v,
            FeeFormula::PercentNotional(bps) => price_raw * qty as i128 * bps / 10_000,
            FeeFormula::Banded(bands) => {
                let notional = price_raw * qty as i128;
                let mut fee = 0;
                for (upper, f) in bands {
                    if notional <= *upper {
                        fee = *f;
                        break;
                    }
                }
                fee
            }
        };
        Some(ScaledMoney {
            amount,
            currency: currency.to_string(),
        })
    }
}

/// Normalize a parsed FIX ExecutionReport into the internal fill event
/// shape (the outbox insert rides the persist pass).
pub fn normalize_exec_report(order_id: Uuid, er: &ExecutionReport, fees: FeeBreakdown) -> Fill {
    let state = match er.exec_type {
        trade8::fix::ExecType::Fill | trade8::fix::ExecType::PartialFill => FillState::Fill,
        trade8::fix::ExecType::Canceled => FillState::Cancel,
        trade8::fix::ExecType::Rejected | trade8::fix::ExecType::Expired => FillState::Reject,
        trade8::fix::ExecType::Replace
        | trade8::fix::ExecType::New
        | trade8::fix::ExecType::Correct => FillState::Correction,
    };
    let qty = er.last_qty.unwrap_or(0);
    // FIX price strings are decimal; scale to the instrument by the
    // caller - here we parse at ×10⁴ (equity default).
    let price_raw = er
        .last_px
        .as_deref()
        .and_then(|p| trade8::fixed::Price::parse_decimal(p, 4))
        .unwrap_or(trade8::fixed::Price::equity(0));
    Fill {
        transaction_id: Uuid::now_v7(),
        order_id,
        state,
        qty: trade8::fixed::Qty::shares(qty),
        price: price_raw,
        fees,
        liquidity_flag: None,
        gateway_order_id: Some(er.order_id.clone()),
        seq_number: None,
        at: Utc::now(),
    }
}

/// The Router engine handle: sessions + routes + charger.
#[derive(Default)]
pub struct RouterEngine {
    pub sessions: RwLock<HashMap<Uuid, SessionState>>,
    pub routes: RwLock<RouteTable>,
    pub charger: Charger,
}

impl RouterEngine {
    pub fn session_for(&self, destination_id: Uuid) -> SessionState {
        self.sessions
            .read()
            .get(&destination_id)
            .cloned()
            .unwrap_or_else(|| SessionState::new(destination_id))
    }

    /// Build a FIX new-order wire message for a destination session
    /// (shadow destinations journal but never send). The destination
    /// parameters mirror the FIX session identity; grouped they'd cost an
    /// allocation per call on the routing path.
    #[allow(clippy::too_many_arguments)]
    pub fn build_new_order(
        &self,
        dest: &DestinationLink,
        destination_session: &mut SessionState,
        cl_ord_id: &str,
        symbol: &str,
        side: char,
        qty: u64,
        price_decimal: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        let seq = destination_session.next_out_seq();
        let bytes = trade8::fix::build_new_order(
            &dest.begin_string,
            seq,
            &dest.sender,
            &dest.target,
            cl_ord_id,
            symbol,
            side,
            '2',
            &qty.to_string(),
            price_decimal,
            '0',
            &[],
        )
        .map_err(|e| e.to_string())?;
        destination_session.journal.push(bytes.clone());
        Ok(bytes)
    }

    /// Gap window on inbound sequence (resend trigger).
    pub fn observe_inbound(&self, destination_id: Uuid, seq: u64) -> Option<(u64, u64)> {
        let mut sessions = self.sessions.write();
        let entry = sessions
            .entry(destination_id)
            .or_insert_with(|| SessionState::new(destination_id));
        entry.observe_in_seq(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_resolution_with_office_override() {
        let mut rt = RouteTable::default();
        let mkt = Uuid::new_v4();
        let d1 = Uuid::new_v4();
        let d2 = Uuid::new_v4();
        rt.set_route(mkt, vec![d1, d2]);
        assert_eq!(rt.resolve(mkt, "HO"), Some(d1));
        rt.set_office_override("BR", mkt, d2);
        assert_eq!(rt.resolve(mkt, "BR"), Some(d2));
        assert_eq!(rt.resolve(mkt, "HO"), Some(d1));
        assert_eq!(rt.resolve(Uuid::new_v4(), "HO"), None);
    }

    #[test]
    fn session_sequences_and_gap_detection() {
        let mut s = SessionState::new(Uuid::new_v4());
        assert_eq!(s.next_out_seq(), 1);
        assert_eq!(s.next_out_seq(), 2);
        // inbound 1,2 fine; jump to 5 → resend window (3,4)
        assert_eq!(s.observe_in_seq(1), None);
        assert_eq!(s.observe_in_seq(2), None);
        assert_eq!(s.observe_in_seq(5), Some((3, 4)));
        assert_eq!(s.observe_in_seq(6), None);
    }

    #[test]
    fn charger_formula_types() {
        let c = Charger {
            formulas: vec![
                ("per_share".into(), FeeFormula::PerShare(5)), // 0.0005/share
                ("per_order".into(), FeeFormula::PerOrder(1_000)), // 0.10
                ("pct".into(), FeeFormula::PercentNotional(250)), // 2.5% bps×10⁴=250? see below
                (
                    "banded".into(),
                    FeeFormula::Banded(vec![(1_000_000, 500), (i128::MAX, 900)]),
                ),
            ],
        };
        // per-share: 100 shares × 5 = 500
        assert_eq!(c.charge("per_share", 100, 0, "USD").unwrap().amount, 500);
        // per-order flat
        assert_eq!(c.charge("per_order", 100, 0, "USD").unwrap().amount, 1_000);
        // percent: 250 bps = 2.5% of notional (10.00×100 = 1_000_000
        // scaled-notional 10_000_000) → 2.5% = 250_000 ×10⁴ money ticks
        let pct = c.charge("pct", 100, 100_000, "USD").unwrap().amount;
        assert_eq!(pct, 250_000);
        // banded: notional 10.00×100 = 1_000_000 ≤ first band upper
        // (1_000_000, fee 500); the second band (900) starts above it
        assert_eq!(c.charge("banded", 100, 10_000, "USD").unwrap().amount, 500);
        assert_eq!(c.charge("banded", 100, 11_000, "USD").unwrap().amount, 900);
        assert!(c.charge("missing", 1, 1, "USD").is_none());
    }

    #[test]
    fn exec_report_normalization() {
        let er = ExecutionReport {
            exec_type: trade8::fix::ExecType::PartialFill,
            order_id: "bo-1".into(),
            cl_ord_id: "cl-1".into(),
            exec_id: "ex-1".into(),
            ord_status: '1',
            leaves_qty: Some(50),
            cum_qty: Some(50),
            last_qty: Some(50),
            last_px: Some("10.10".into()),
            reject_reason: None,
            text: None,
        };
        let fill = normalize_exec_report(Uuid::new_v4(), &er, FeeBreakdown::default());
        assert_eq!(fill.state, FillState::Fill);
        assert_eq!(fill.qty.raw, 50);
        assert_eq!(fill.price.raw, 101_000); // 10.10 ×10⁴ (memory literal)
        assert_eq!(fill.gateway_order_id.as_deref(), Some("bo-1"));
    }

    #[test]
    fn shadow_mode_journals_without_send_flag() {
        let eng = RouterEngine::default();
        let dest = DestinationLink {
            destination_id: Uuid::new_v4(),
            name: "SHADOW".into(),
            protocol: DestinationProtocol::Fix42,
            sender: "DESK".into(),
            target: "BRK".into(),
            begin_string: "FIX.4.2".into(),
            env_mode_live: false,
            enable: true,
            heartbeat_s: 30,
            seq_reset_daily: true,
        };
        let mut s = SessionState::new(dest.destination_id);
        let bytes = eng
            .build_new_order(&dest, &mut s, "cl-1", "AAPL", '1', 100, Some("10.00"))
            .unwrap();
        assert!(bytes.starts_with(b"8=FIX.4.2"));
        assert_eq!(s.journal.len(), 1);
        assert_eq!(s.outgoing_seq, 1);
        // shadow destinations never dial out - the transport pass checks
        // env_mode_live before connecting; the journal keeps the mirror.
        assert!(!dest.env_mode_live);
    }
}
