// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Router SQL integration tests (test-architecture-design §5.4).
//!
//! Real `trade8_router_service` NOBYPASSRLS role: journal
//! append/read/purge, session-counter upsert/reset, the outbound-seq
//! lease atomicity (the wiring's UPDATE…RETURNING), and the RLS matrix.
//!
//! Design: `../../DESIGN.md §9 (testing)` §5.4.

#[path = "../src/store.rs"]
#[allow(dead_code)]
mod store;

use sqlx::postgres::PgPoolOptions;
use sqlx::Acquire;
use trade8::contracts::events;
use uuid::Uuid;

const ROUTER_URL: &str = "postgresql://trade8_router_service@localhost:5432/trade8";
const PROBE_URL: &str = "postgresql://trade8_probe@localhost:5432/trade8";

fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}
fn env_b() -> Uuid {
    Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
}

async fn router_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(ROUTER_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn probe_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(PROBE_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn cleanup(conn: &mut sqlx::PgConnection, env: Uuid) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let _ = sqlx::query(r#"DELETE FROM router.message WHERE env_id=$1 AND seq >= 900000000"#)
        .bind(env)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM router.session WHERE env_id=$1"#)
        .bind(env)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM router.outbox WHERE env_id=$1 AND stream_family='router' AND created_at > now() - interval '1 minute'"#)
        .bind(env)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
}

/// A high seq base so test rows never collide with real traffic.
const SEQ_BASE: i64 = 900_000_000;

// ---------------------------------------------------------------------------
// RLS matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rls_matrix_no_context_returns_zero() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM router.message")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(n, 0, "no tenant context must see zero rows");
}

#[tokio::test]
async fn rls_matrix_cross_env_returns_zero() {
    let Some(mut conn) = router_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let dest = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::append_journal(
            &mut tx,
            env_a(),
            dest,
            "out",
            SEQ_BASE,
            Some("D"),
            b"8=FIX.4.4\x0135=D\x01",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_b()).await;
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM router.message WHERE destination_id = $1 AND seq = $2",
    )
    .bind(dest)
    .bind(SEQ_BASE)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(n, 0, "env-b must not see env-a's journal rows");
    cleanup(&mut conn, env_a()).await;
}

// ---------------------------------------------------------------------------
// Journal append / read / purge (design §5.4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn journal_append_read_filter() {
    let Some(mut conn) = router_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let dest1 = Uuid::new_v4();
    let dest2 = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::append_journal(&mut tx, env_a(), dest1, "out", SEQ_BASE, Some("D"), b"one")
            .await
            .unwrap();
        store::append_journal(
            &mut tx,
            env_a(),
            dest2,
            "out",
            SEQ_BASE + 1,
            Some("D"),
            b"two",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    // Destination filter returns only that destination's rows.
    let rows = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::read_journal(&mut tx, env_a(), Some(dest1), SEQ_BASE, 10)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, SEQ_BASE);
    assert_eq!(rows[0].4, b"one".to_vec());
    // No filter returns both, ordered by seq (scoped to the test's own
    // destinations — the table may hold rows from other runs/services).
    let all = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::read_journal(&mut tx, env_a(), None, SEQ_BASE, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.1 == dest1 || r.1 == dest2)
            .collect::<Vec<_>>();
        tx.rollback().await.unwrap();
        v
    };
    let seqs: Vec<i64> = all.iter().map(|r| r.0).collect();
    assert_eq!(
        seqs_of(&seqs),
        vec![SEQ_BASE, SEQ_BASE + 1],
        "ordered by seq"
    );
    cleanup(&mut conn, env_a()).await;
}

fn seqs_of(v: &[i64]) -> Vec<i64> {
    v.to_vec()
}

#[tokio::test]
async fn journal_purge_by_age() {
    let Some(mut conn) = router_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let dest = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::append_journal(&mut tx, env_a(), dest, "out", SEQ_BASE, Some("D"), b"old")
            .await
            .unwrap();
        store::append_journal(
            &mut tx,
            env_a(),
            dest,
            "out",
            SEQ_BASE + 1,
            Some("D"),
            b"new",
        )
        .await
        .unwrap();
        // Backdate the first row beyond the 30d retention.
        sqlx::query(
            r#"UPDATE router.message SET at = now() - interval '40 days'
               WHERE env_id=$1 AND destination_id=$2 AND seq=$3"#,
        )
        .bind(env_a())
        .bind(dest)
        .bind(SEQ_BASE)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let purged = {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let n = store::purge_journal(&mut tx, env_a(), 30).await.unwrap();
        tx.commit().await.unwrap();
        n
    };
    assert!(purged >= 1, "the backdated row must purge");
    let remaining = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::read_journal(&mut tx, env_a(), Some(dest), SEQ_BASE, 10)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert_eq!(remaining.len(), 1, "only the fresh row survives");
    assert_eq!(remaining[0].4, b"new".to_vec());
    cleanup(&mut conn, env_a()).await;
}

// ---------------------------------------------------------------------------
// Session counters (design §5.4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_counters_upsert_and_reset() {
    let Some(mut conn) = router_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let dest = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_session(&mut tx, env_a(), dest, 10, 5, "logged_in")
            .await
            .unwrap();
        store::persist_session(&mut tx, env_a(), dest, 11, 6, "logged_in")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            r#"SELECT outgoing_seq, incoming_seq, state FROM router.session
               WHERE env_id=$1 AND destination_id=$2"#,
        )
        .bind(env_a())
        .bind(dest)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1, "upsert must collapse to one row");
        assert_eq!(
            (rows[0].0, rows[0].1, rows[0].2.as_str()),
            (11, 6, "logged_in")
        );
        let events_: i64 = sqlx::query_scalar(
            r#"SELECT count(*) FROM router.outbox
               WHERE env_id=$1 AND event_type=$2"#,
        )
        .bind(env_a())
        .bind(events::SESSION_STATUS)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert!(events_ >= 2, "each persist must emit session:status");
        tx.commit().await.unwrap();
    }
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::reset_session_counters(&mut tx, env_a(), dest)
            .await
            .unwrap();
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            r#"SELECT outgoing_seq, incoming_seq, state FROM router.session
               WHERE env_id=$1 AND destination_id=$2"#,
        )
        .bind(env_a())
        .bind(dest)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].0, rows[0].1, rows[0].2.as_str()), (0, 0, "down"));
        tx.commit().await.unwrap();
    }
    cleanup(&mut conn, env_a()).await;
}

/// The wiring's outbound-seq lease (UPDATE…RETURNING) hands out disjoint
/// contiguous blocks — the platform's atomic-lease convention.
#[tokio::test]
async fn outbound_seq_lease_is_atomic() {
    let Some(mut conn) = router_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let dest = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_session(&mut tx, env_a(), dest, 100, 0, "logged_in")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let s1: Option<i64> = sqlx::query_scalar(
        r#"UPDATE router.session
           SET outgoing_seq = outgoing_seq + 1, state = 'connecting'
           WHERE env_id = $1 AND destination_id = $2
           RETURNING outgoing_seq"#,
    )
    .bind(env_a())
    .bind(dest)
    .fetch_optional(&mut *tx)
    .await
    .unwrap();
    let s2: Option<i64> = sqlx::query_scalar(
        r#"UPDATE router.session
           SET outgoing_seq = outgoing_seq + 1, state = 'connecting'
           WHERE env_id = $1 AND destination_id = $2
           RETURNING outgoing_seq"#,
    )
    .bind(env_a())
    .bind(dest)
    .fetch_optional(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap(); // lease test must not consume real seqs
    assert_eq!(s1, Some(101), "lease continues from the persisted counter");
    assert_eq!(s2, Some(102), "sequential leases are contiguous");
    cleanup(&mut conn, env_a()).await;
}
