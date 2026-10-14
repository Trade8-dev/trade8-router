// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Router DB persistence — message journal + session
//! counters with `session:status` outbox events and Cron-driven retention.

use sqlx::PgConnection;
use trade8::contracts::events;
use uuid::Uuid;

/// Append a journal row (outbound/inbound frame bytes).
pub async fn append_journal(
    conn: &mut PgConnection,
    env_id: Uuid,
    destination_id: Uuid,
    direction: &str,
    seq: i64,
    msg_type: Option<&str>,
    payload: &[u8],
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"INSERT INTO router.message
           (env_id, destination_id, direction, seq, msg_type, payload)
           VALUES ($1,$2,$3,$4,$5,$6)"#,
    )
    .bind(env_id)
    .bind(destination_id)
    .bind(direction)
    .bind(seq)
    .bind(msg_type)
    .bind(payload)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// One `router.message` window row (seq, destination, direction,
/// msg_type, payload, at).
pub type JournalRow = (
    i64,
    Uuid,
    String,
    Option<String>,
    Vec<u8>,
    chrono::DateTime<chrono::Utc>,
);

/// Read a journal window (resend recovery; the admin wall's browser).
pub async fn read_journal(
    conn: &mut PgConnection,
    env_id: Uuid,
    destination_id: Option<Uuid>,
    from_seq: i64,
    limit: i64,
) -> Result<Vec<JournalRow>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<JournalRow> = sqlx::query_as(
        r#"SELECT seq, destination_id, direction, msg_type, payload, at
               FROM router.message
               WHERE env_id = $1
                 AND ($2::uuid IS NULL OR destination_id = $2)
                 AND seq >= $3
               ORDER BY seq LIMIT $4"#,
    )
    .bind(env_id)
    .bind(destination_id)
    .bind(from_seq)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows)
}

/// Retention purge (Cron proxy-back target; 30d default).
pub async fn purge_journal(
    conn: &mut PgConnection,
    env_id: Uuid,
    days: i32,
) -> Result<u64, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let res = sqlx::query(
        r#"DELETE FROM router.message
           WHERE env_id = $1 AND at < now() - ($2 || ' days')::interval"#,
    )
    .bind(env_id)
    .bind(days.to_string())
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected())
}

/// Persist session counters + `session:status` outbox event.
pub async fn persist_session(
    conn: &mut PgConnection,
    env_id: Uuid,
    destination_id: Uuid,
    outgoing_seq: i64,
    incoming_seq: i64,
    state: &str,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"INSERT INTO router.session
           (env_id, destination_id, outgoing_seq, incoming_seq, state, last_heartbeat)
           VALUES ($1,$2,$3,$4,$5, now())
           ON CONFLICT (env_id, destination_id) DO UPDATE SET
             outgoing_seq = EXCLUDED.outgoing_seq,
             incoming_seq = EXCLUDED.incoming_seq,
             state = EXCLUDED.state,
             last_heartbeat = now()"#,
    )
    .bind(env_id)
    .bind(destination_id)
    .bind(outgoing_seq)
    .bind(incoming_seq)
    .bind(state)
    .execute(&mut *conn)
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::SESSION_STATUS,
        serde_json::json!({
            "destination_id": destination_id, "state": state,
            "outgoing_seq": outgoing_seq, "incoming_seq": incoming_seq,
        }),
    )
    .await
}

/// Reset counters (the `:reset` op — audited at the handler).
pub async fn reset_session_counters(
    conn: &mut PgConnection,
    env_id: Uuid,
    destination_id: Uuid,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"INSERT INTO router.session
           (env_id, destination_id, outgoing_seq, incoming_seq, state)
           VALUES ($1,$2,0,0,'down')
           ON CONFLICT (env_id, destination_id) DO UPDATE SET
             outgoing_seq = 0, incoming_seq = 0, state = 'down'"#,
    )
    .bind(env_id)
    .bind(destination_id)
    .execute(&mut *conn)
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::SESSION_STATUS,
        serde_json::json!({"destination_id": destination_id, "action": "reset"}),
    )
    .await
}

/// Load session counters at boot (resend/recovery continuity).
pub async fn bootstrap_sessions(
    conn: &mut PgConnection,
    env_id: Uuid,
) -> Result<Vec<(Uuid, i64, i64, String)>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<(Uuid, i64, i64, String)> = sqlx::query_as(
        r#"SELECT destination_id, outgoing_seq, incoming_seq, state
           FROM router.session WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows)
}

/// Outbox insert — drained by the shell's boot-step-8b relay to Bus.
pub async fn insert_outbox_event(
    conn: &mut PgConnection,
    env_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let row_id =
        sentin8::bus::outbox::insert_outbox_row(&mut *conn, env_id, "router", event_type, &payload)
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(other.to_string().into()),
            })?;
    let _ = row_id;
    Ok(())
}
