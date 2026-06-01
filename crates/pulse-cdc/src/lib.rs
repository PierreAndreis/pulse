//! Cross-node change bus.
//!
//! The reactive layer is single-process by itself: each engine node only knows
//! about subscriptions in its own memory. To scale horizontally we need a write
//! on *any* node to invalidate matching subscriptions on *every* node. Only the
//! committed `ChangeSet` should cross process boundaries — each node then runs
//! its own local `apply_change_set` (matching + re-exec + push).
//!
//! This module is that bus, implemented over Postgres `LISTEN/NOTIFY` (pure
//! Postgres — no extra infrastructure, consistent with bring-your-own-Postgres).
//! It is the same shape a WAL/`pgoutput` consumer will take: a change source that
//! yields `ChangeSet`s feeding `apply_change_set`. When logical-replication CDC
//! lands it replaces the *publish* step (so writes made outside Pulse also flow),
//! leaving the receive side unchanged.
//!
//! Each node tags its notifications with a `node_id`; the listener drops messages
//! it originated (the origin node already applied them locally), so enabling the
//! bus is additive — single-node behavior is unchanged.

use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::mpsc;

use pulse_core::ChangeSet;

/// The NOTIFY channel all nodes publish/subscribe on.
pub const CHANNEL: &str = "pulse_changes";

/// Postgres NOTIFY payloads are capped at 8000 bytes; stay safely under.
const MAX_PAYLOAD: usize = 7800;

/// A decoded bus event handed to the local reactor.
#[derive(Debug, Clone)]
pub enum BusEvent {
    /// A concrete change-set from another node — apply it precisely.
    Changes(ChangeSet),
    /// The originating change-set was too large to ship; re-evaluate everything
    /// (rare, safe over-approximation).
    Resync,
}

/// Wire form of a bus message.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Wire {
    Changes { origin: String, data: ChangeSet },
    Resync { origin: String },
}

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Publish a change-set to every node (including the origin). Small change-sets
/// ship in full; oversized ones degrade to a `Resync` marker.
pub async fn publish(pool: &PgPool, node_id: &str, cs: &ChangeSet) -> Result<(), BusError> {
    let full = Wire::Changes {
        origin: node_id.to_string(),
        data: cs.clone(),
    };
    let payload = serde_json::to_string(&full)?;
    let payload = if payload.len() <= MAX_PAYLOAD {
        payload
    } else {
        serde_json::to_string(&Wire::Resync {
            origin: node_id.to_string(),
        })?
    };
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(payload)
        .execute(pool)
        .await?;
    Ok(())
}

/// Start listening for bus events from *other* nodes. Returns a receiver that
/// yields foreign events only (self-originated messages are dropped, since the
/// origin already applied them locally). The listener runs until the receiver is
/// dropped or the connection fails.
pub async fn start_listener(
    database_url: &str,
    node_id: String,
) -> Result<mpsc::Receiver<BusEvent>, BusError> {
    let mut listener = PgListener::connect(database_url).await?;
    listener.listen(CHANNEL).await?;
    let (tx, rx) = mpsc::channel(1024);

    tokio::spawn(async move {
        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let payload = notification.payload();
                    match serde_json::from_str::<Wire>(payload) {
                        Ok(Wire::Changes { origin, data }) => {
                            if origin == node_id {
                                continue; // already applied locally
                            }
                            if tx.send(BusEvent::Changes(data)).await.is_err() {
                                break; // receiver gone
                            }
                        }
                        Ok(Wire::Resync { origin }) => {
                            if origin == node_id {
                                continue;
                            }
                            if tx.send(BusEvent::Resync).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(target: "pulse::cdc", "unparseable bus payload: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(target: "pulse::cdc", "bus listener error: {e}");
                    break;
                }
            }
        }
    });

    Ok(rx)
}
