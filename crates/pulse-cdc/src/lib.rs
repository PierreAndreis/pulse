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
    /// The originating change-set was too large to ship precisely, but the set of
    /// tables it touched still fit — re-evaluate only subscriptions on those tables
    /// (coarse, but scoped to the affected tables instead of everything).
    ResyncTables(Vec<String>),
    /// Even the touched-table list didn't fit (pathologically wide write) →
    /// re-evaluate everything (rare, safe over-approximation).
    Resync,
}

/// Wire form of a bus message.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Wire {
    Changes { origin: String, data: ChangeSet },
    ResyncTables { origin: String, tables: Vec<String> },
    Resync { origin: String },
}

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Encode a change-set for the wire, degrading gracefully as it grows:
///   1. small enough → ship the full change-set (precise),
///   2. too big, but the distinct touched-table list fits → ship `ResyncTables`
///      so peers invalidate only those tables (scoped over-approximation),
///   3. even the table list is too big → ship `Resync` (global re-eval).
fn encode_payload(node_id: &str, cs: &ChangeSet) -> Result<String, BusError> {
    let full = Wire::Changes {
        origin: node_id.to_string(),
        data: cs.clone(),
    };
    let payload = serde_json::to_string(&full)?;
    if payload.len() <= MAX_PAYLOAD {
        return Ok(payload);
    }

    let mut tables: Vec<String> = cs.changes.iter().map(|c| c.table.0.clone()).collect();
    tables.sort();
    tables.dedup();
    let scoped = serde_json::to_string(&Wire::ResyncTables {
        origin: node_id.to_string(),
        tables,
    })?;
    if scoped.len() <= MAX_PAYLOAD {
        return Ok(scoped);
    }

    Ok(serde_json::to_string(&Wire::Resync {
        origin: node_id.to_string(),
    })?)
}

/// Classify a decoded wire message against the local node id. Self-originated
/// messages return `None` (the origin already applied them locally); foreign
/// messages return the `BusEvent` to hand to the local reactor.
fn classify(wire: Wire, self_node_id: &str) -> Option<BusEvent> {
    match wire {
        Wire::Changes { origin, data } => {
            if origin == self_node_id {
                None // already applied locally
            } else {
                Some(BusEvent::Changes(data))
            }
        }
        Wire::ResyncTables { origin, tables } => {
            if origin == self_node_id {
                None
            } else {
                Some(BusEvent::ResyncTables(tables))
            }
        }
        Wire::Resync { origin } => {
            if origin == self_node_id {
                None
            } else {
                Some(BusEvent::Resync)
            }
        }
    }
}

/// Publish a change-set to every node (including the origin). Small change-sets
/// ship in full; oversized ones degrade to a scoped `ResyncTables` (or a global
/// `Resync` if even the table list overflows) — see [`encode_payload`].
pub async fn publish(pool: &PgPool, node_id: &str, cs: &ChangeSet) -> Result<(), BusError> {
    let payload = encode_payload(node_id, cs)?;
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
                        Ok(wire) => match classify(wire, &node_id) {
                            Some(event) => {
                                if tx.send(event).await.is_err() {
                                    break; // receiver gone
                                }
                            }
                            None => continue, // self-originated; already applied locally
                        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{Change, ChangeOp, KeyValue, Lsn, PrimaryKey, TableId};

    fn lsn() -> Lsn {
        "0/1000".parse().unwrap()
    }

    fn small_changeset() -> ChangeSet {
        let mut cs = ChangeSet::new(lsn());
        cs.push(Change::point(
            TableId::new("public.messages"),
            PrimaryKey::single(KeyValue::Int(1)),
            ChangeOp::Insert,
        ));
        cs
    }

    #[test]
    fn wire_round_trips() {
        let wire = Wire::Changes {
            origin: "node-a".to_string(),
            data: small_changeset(),
        };
        let json = serde_json::to_string(&wire).unwrap();
        // lowercase kind tag from serde rename_all = "lowercase"
        assert!(json.contains("\"kind\":\"changes\""), "json was: {json}");

        let back: Wire = serde_json::from_str(&json).unwrap();
        match back {
            Wire::Changes { origin, data } => {
                assert_eq!(origin, "node-a");
                assert_eq!(data, small_changeset());
            }
            _ => panic!("expected Changes variant"),
        }
    }

    #[test]
    fn resync_wire_round_trips_lowercase_tag() {
        let wire = Wire::Resync {
            origin: "node-z".to_string(),
        };
        let json = serde_json::to_string(&wire).unwrap();
        assert!(json.contains("\"kind\":\"resync\""), "json was: {json}");
        let back: Wire = serde_json::from_str(&json).unwrap();
        match back {
            Wire::Resync { origin } => assert_eq!(origin, "node-z"),
            _ => panic!("expected Resync variant"),
        }
    }

    #[test]
    fn classify_drops_self_origin() {
        let wire = Wire::Changes {
            origin: "self".to_string(),
            data: small_changeset(),
        };
        assert!(classify(wire, "self").is_none());

        let resync = Wire::Resync {
            origin: "self".to_string(),
        };
        assert!(classify(resync, "self").is_none());
    }

    #[test]
    fn classify_keeps_foreign() {
        let wire = Wire::Changes {
            origin: "other".to_string(),
            data: small_changeset(),
        };
        match classify(wire, "self") {
            Some(BusEvent::Changes(cs)) => assert_eq!(cs, small_changeset()),
            other => panic!("expected foreign Changes, got {other:?}"),
        }

        let resync = Wire::Resync {
            origin: "other".to_string(),
        };
        match classify(resync, "self") {
            Some(BusEvent::Resync) => {}
            other => panic!("expected foreign Resync, got {other:?}"),
        }
    }

    #[test]
    fn classify_keeps_foreign_resync_tables() {
        let wire = Wire::ResyncTables {
            origin: "other".to_string(),
            tables: vec!["public.messages".to_string()],
        };
        match classify(wire, "self") {
            Some(BusEvent::ResyncTables(t)) => assert_eq!(t, vec!["public.messages"]),
            other => panic!("expected foreign ResyncTables, got {other:?}"),
        }
        // self-originated is dropped
        let mine = Wire::ResyncTables {
            origin: "self".to_string(),
            tables: vec!["x".to_string()],
        };
        assert!(classify(mine, "self").is_none());
    }

    #[test]
    fn oversize_payload_degrades_to_scoped_table_resync() {
        // Under the guard: a small change-set ships in full.
        let small = encode_payload("n1", &small_changeset()).unwrap();
        assert!(small.len() <= MAX_PAYLOAD);
        assert!(small.contains("\"kind\":\"changes\""), "json was: {small}");

        // Over the guard but few distinct tables: degrade to ResyncTables carrying
        // the touched tables (NOT a global Resync) — this is the blast-radius win.
        let mut big = ChangeSet::new(lsn());
        for i in 0..2000 {
            big.push(Change::point(
                TableId::new("public.messages"),
                PrimaryKey::single(KeyValue::Text(format!("row-{i}"))),
                ChangeOp::Insert,
            ));
        }
        let full = serde_json::to_string(&Wire::Changes {
            origin: "n1".to_string(),
            data: big.clone(),
        })
        .unwrap();
        assert!(
            full.len() > MAX_PAYLOAD,
            "fixture too small: {}",
            full.len()
        );

        let encoded = encode_payload("n1", &big).unwrap();
        assert!(encoded.len() <= MAX_PAYLOAD);
        let back: Wire = serde_json::from_str(&encoded).unwrap();
        match back {
            Wire::ResyncTables { origin, tables } => {
                assert_eq!(origin, "n1");
                assert_eq!(tables, vec!["public.messages"], "dedup to the one table");
            }
            _ => panic!("expected ResyncTables, got a different wire variant"),
        }
    }

    #[test]
    fn pathologically_wide_write_falls_back_to_global_resync() {
        // Thousands of DISTINCT tables → even the table list exceeds the cap →
        // fall all the way back to a global Resync.
        let mut big = ChangeSet::new(lsn());
        for i in 0..3000 {
            big.push(Change::point(
                TableId::new(format!("public.table_with_a_longish_name_{i:06}")),
                PrimaryKey::single(KeyValue::Int(i)),
                ChangeOp::Insert,
            ));
        }
        let encoded = encode_payload("n1", &big).unwrap();
        assert!(encoded.len() <= MAX_PAYLOAD);
        let back: Wire = serde_json::from_str(&encoded).unwrap();
        assert!(
            matches!(back, Wire::Resync { .. }),
            "a write spanning thousands of tables must fall back to global Resync"
        );
    }
}
