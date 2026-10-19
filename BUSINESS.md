# Trade8:Router - Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Trade8:Router **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Connect the trading desk to **every broker and venue** the firm uses,
  through one managed service - FIX 4.2/4.4 and OUCH 3.1 today, each as
  data-driven destinations rather than bespoke integrations.
- Guarantee that **every wire message and every fill is durably recorded**
  and attributable (sequence numbers, preserved broker tags, fee breakdown) -
  the compliance backbone of client and regulator inquiries.
- Turn raw broker reports into **clean internal fill events** (with fees,
  liquidity, and corrected quantities) so P&L, Risk, and reporting all work
  from one normalized truth.
- Protect the brokers: shadow destinations, atomic sequence management, and
  heartbeats keep sessions sane and testable without risking real orders.

## 2. Business Scope (What the Service Provides)

| Capability | Description |
|---|---|
| Order routing | Accepted Desk orders resolved to destinations and rendered outbound |
| FIX client sessions | FIX 4.2/4.4 logon/heartbeat/sequence management (session machines + codecs) |
| OUCH client sessions | OUCH 3.1 order entry (codes + inbound normalization) |
| Message templates | Typed, versioned outbound field maps per destination (admin-managed data) |
| Route resolution | Market → gateway → destination with office-level broker overrides |
| Fee charging | Per-share/per-order/percent/banded formulas evaluated on fills |
| Execution normalization | Broker reports → internal fill events (executed/cancelled/rejected/correction) |
| Fill custody | Sole writer of fills + preserved broker FIX tags |
| Wire journal | Every outbound/inbound message stored, retention-purged |
| Session ops wall | Live session states, sequence resets, heartbeat status |
| Shadow mode | Full render/journal pipeline without sending (testing/parity) |
| Admin workspace | RFW pages: sessions, journal, fees |

### What the service does NOT own (business boundary)

| Concern | Owned by |
|---|---|
| Routing catalogs (destinations, gateways, templates, office-broker map) | Trade8:Desk |
| Order acceptance and pre-trade authorization | Trade8:Desk / Trade8:Risk |
| Position/P&L effects of fills | Trade8:Pnl |
| Fee catalog rows | Trade8:Desk (Router evaluates the formulas) |
| Market data | Trade8:Feed |
| Scheduling (purge) | Sentin8:Cron |
| Venue-side matching/auction/clearing | exchange8 (when deployed) |

## 3. Business Capabilities

| Area | Capability | Business rule |
|---|---|---|
| Routing | Destination resolution | A market's orders flow to the first configured destination unless an office-level broker override applies |
| Routing | Protocol adaptation | The destination's protocol (fix42/fix44/ouch31) selects the codec; templates override field layouts per broker |
| Sequences | Contiguous order ids | Outbound sequences are allocated atomically per destination; daily reset follows the destination's policy |
| Sessions | Liveness | Stale heartbeats mark sessions down; resets and logons are operator actions, audited |
| Fees | Formula evaluation | Fees compute from formulas (per share, per order, percent of notional, banded) at fill time; defaults apply when no formula matches |
| Fills | Normalization completeness | Every broker execution becomes exactly one internal event with fees, liquidity flag, and preserved broker tags |
| Fills | Corrections | Brokers' corrected reports update the fill (correction maps into the executed event with corrected quantities) |
| Journal | Completeness + retention | Outbound and inbound wire bytes are journaled with sequence and message type; older than retention is purged on schedule |
| Shadow | Zero-impact testing | Shadow destinations process the full path (render, journal, sequences) but never transmit |

## 4. Business Events (Outbound)

| Event | Fires when |
|---|---|
| `trade8:router:fill:executed` | A broker reports an execution (or a correction) for a routed order |
| `trade8:router:fill:cancelled` | A broker reports a cancellation of a resting order |
| `trade8:router:fill:rejected` | A broker rejects an order (mapped through the reject-code registry) |
| `trade8:router:session:status` | A destination session changes state (down/connecting/logged_in) |
| `trade8:desk:order:routed` | A Desk order is leased, rendered and journaled toward its destination |

**Consumed:** `trade8:desk:order:accepted` (the routing trigger).

## 5. Business Rules & Invariants

1. An accepted desk order is always journaled before (or atomically with)
   its route acknowledgment - no unrecorded sends.
2. Two orders to the same destination can never get the same sequence
   number (atomic lease).
3. Shadow destinations never send real orders - production parity testing
   is impossible without explicit live mode.
4. Every fill carries its complete fee breakdown and broker FIX tags;
   corrections update rather than duplicate.
5. Broker credentials are stored as secret references and never rendered in
   any admin surface or journal.
6. Fills are the single source for PnL/Risk/reporting - no other service
   writes execution records.
7. The journal is retention-purged on schedule, never ad hoc.
8. A rejected/failed send never consumes the order: the consumer retries
   (Nack) and Desk's idempotency keeps the order unique.

## 6. Target-User Roles & Use Cases

| Persona | Use cases |
|---|---|
| Trading supervisor | Watch the session wall; reset a stuck sequence; confirm broker connectivity before market open |
| Back office / catalog admin | Manage destinations, gateways, templates, fee schedules (Desk-owned catalogs) |
| Compliance | Browse the wire journal, attribute fills to brokers via preserved tags |
| Support desk | Trace an order from desk acceptance to broker acknowledgment and fills |
| Desk operations | Configure office-level broker routing overrides |
| Downstream systems | Consume normalized fills (PnL, Risk) |

## 7. KPIs / Success Criteria

- Zero lost or double-sequenced outbound messages (lease atomicity).
- Every broker report reflected as an internal event within the
  fill-processing budget; fills persist with complete fee data.
- Session wall accuracy: watchdog marks stale sessions down within one
  watchdog interval.
- 100% of fills attributable (exec id, broker tags, destination).
- Journal purge never removes un-purged young rows; retention enforced.
- Shadow-mode runs must never emit to a live endpoint (test-enforced).

## 8. Dependencies & Reliance on Other Services

| Dependency | Why the business relies on it |
|---|---|
| Trade8:Desk | Routing catalogs + the accepted-order stream + reject codes + fee catalog |
| Sentin8:Bus | Event transport (orders in, fills out) |
| Sentin8:Cron | Journal purge scheduling |
| Sentin8:Auth + Shield | Ops authorization for session/journal surfaces |
| Sentin8:Sys | Audit ingestion |
| Brokers / exchange8 | The counterparties on the wire |

## 9. Acceptance Criteria

1. An accepted desk order reaches its destination's journal with a
   contiguous sequence and a routed acknowledgment.
2. Every broker execution appears in the blotter (via PnL/Risk) with fees -
   within the trading-day budget.
3. An operator can reset a session and see it re-establish (state wall).
4. Compliance can reconstruct any fill's original broker wire tags.
5. Shadow-mode destinations prove a broker cutover without sending orders.
6. Fee schedules are admin-editable without deployments.

## 10. Open Business Questions

- Which brokers require the native socket transport first (vs HTTP
  adapters), and what is the broker-certification checklist per venue?
- Should fill corrections support negative adjustments (true reversals)
  in the business rules, or is re-execution sufficient?
- What retention period do regulators require for the wire journal
  (30-day default today)?
- Does the business want broker rejects auto-converted into trader
  notifications (via Post), and in which escalation ladder?

---

**Design attribution:** trade8:router and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
