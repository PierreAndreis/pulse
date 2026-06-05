//! Node interest registry — the routing table for the bus.
//!
//! Each node records which tables it currently has live subscriptions on. A
//! publisher consults this to NOTIFY only the nodes that could match a change,
//! instead of broadcasting every change to every node. Rows carry an `updated_at`
//! heartbeat; stale rows (a crashed node, or a table a node no longer watches)
//! expire after a TTL, so the registry is self-healing — staleness only ever
//! causes a few *extra* notifies, never a missed one (as long as a node registers
//! a newly-watched table synchronously before relying on the bus for it).

use sqlx::postgres::PgPool;

use crate::BusError;

/// Create the interest table if absent. Idempotent; safe to call on every boot.
pub async fn ensure_interest_table(pool: &PgPool) -> Result<(), BusError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _pulse_node_interest (
            node_id    text        NOT NULL,
            table_name text        NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (node_id, table_name)
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Upsert this node's interest in `tables`, refreshing the heartbeat. Called both
/// synchronously when a node first watches a table and periodically to keep the
/// rows alive. Empty `tables` is a no-op.
pub async fn register_interest(
    pool: &PgPool,
    node_id: &str,
    tables: &[String],
) -> Result<(), BusError> {
    if tables.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO _pulse_node_interest (node_id, table_name, updated_at)
         SELECT $1, unnest($2::text[]), now()
         ON CONFLICT (node_id, table_name) DO UPDATE SET updated_at = now()",
    )
    .bind(node_id)
    .bind(tables)
    .execute(pool)
    .await?;
    Ok(())
}

/// Distinct node ids (excluding `self_node`) that currently have live interest in
/// any of `tables` — i.e. whose heartbeat is within `ttl_secs`. These are exactly
/// the nodes a change touching `tables` must be routed to.
pub async fn interested_nodes(
    pool: &PgPool,
    tables: &[String],
    self_node: &str,
    ttl_secs: i64,
) -> Result<Vec<String>, BusError> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT node_id FROM _pulse_node_interest
         WHERE table_name = ANY($1)
           AND node_id <> $2
           AND updated_at > now() - ($3::int * interval '1 second')",
    )
    .bind(tables)
    .bind(self_node)
    .bind(ttl_secs as i32)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

/// Delete interest rows whose heartbeat is older than `ttl_secs` (crashed nodes /
/// abandoned tables). Any node may run this; it's cheap and idempotent.
pub async fn prune_interest(pool: &PgPool, ttl_secs: i64) -> Result<u64, BusError> {
    let res = sqlx::query(
        "DELETE FROM _pulse_node_interest
         WHERE updated_at < now() - ($1::int * interval '1 second')",
    )
    .bind(ttl_secs as i32)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Drop all of a node's interest rows (graceful shutdown).
pub async fn clear_interest(pool: &PgPool, node_id: &str) -> Result<(), BusError> {
    sqlx::query("DELETE FROM _pulse_node_interest WHERE node_id = $1")
        .bind(node_id)
        .execute(pool)
        .await?;
    Ok(())
}
