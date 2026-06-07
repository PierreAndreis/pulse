//! The engine **Coordinator** — the orchestration that used to live inside the
//! `pulse-server` axum handlers and `main()`.
//!
//! It owns the request and cross-node flows (run a procedure, propagate a
//! mutation, register a subscription, apply a remote invalidation, beat the
//! interest heartbeat) over four seams it does *not* depend on concretely:
//!
//! - [`Executor`] — run a procedure (value + read-set + write-set), over the worker.
//! - [`Reactor`] — the subscription manager (re-exported from `pulse-reactor`).
//! - [`Publisher`] — publish a committed change-set to other nodes, over the bus.
//! - [`InterestRegistry`] — refresh/prune this node's cross-node interest.
//!
//! Keeping the crate to `pulse-core` + `pulse-reactor` is the whole point: the
//! orchestration becomes testable in-memory (an in-memory reactor plus fakes for
//! the three host seams) with no Postgres and no worker process.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::Value;

use pulse_core::{Change, ChangeSet, Lsn, TableId};
pub use pulse_reactor::{Reactor, SsePush, Subscription};
use tokio::sync::mpsc;

/// What a procedure run yields. Richer than the reactor's `ReExecutor` (which
/// drops the write-set): the RPC path needs `changes` to propagate a mutation.
pub struct ExecOutcome {
    pub value: Value,
    pub read_set: pulse_core::ReadSet,
    /// The mutation's captured write-set (empty for reads).
    pub changes: Vec<Change>,
}

/// A procedure failure, mirroring the worker's error envelope without binding the
/// engine to the JS-runtime crate. `code` drives the HTTP status at the edge.
#[derive(Debug, Clone)]
pub struct ExecError {
    pub code: String,
    pub data: Value,
    pub message: Option<String>,
}

impl ExecError {
    pub fn not_found(path: &[String]) -> Self {
        ExecError {
            code: "NOT_FOUND".to_string(),
            data: Value::Null,
            message: Some(format!("no procedure at `{}`", path.join("."))),
        }
    }
}

/// Runs procedures for the coordinator. Host adapts it over its function runtime.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Whether a procedure exists at `path` — drives the NOT_FOUND pre-check
    /// without executing.
    fn contains(&self, path: &[String]) -> bool;

    async fn execute(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
        mutation_id: Option<String>,
    ) -> Result<ExecOutcome, ExecError>;
}

/// Publishes a committed change-set to other nodes. Best-effort: a publish error
/// is the adapter's concern (logged), never surfaced — local delivery already
/// happened, and a missed cross-node message is recovered by resync.
#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, change_set: &ChangeSet);
}

/// Keeps this node's cross-node interest fresh on the heartbeat. `refresh`
/// (re)registers interest in the tables this node currently watches; `prune`
/// drops dead nodes. The TTL is the adapter's; [`clamp_heartbeat`] keeps the
/// beat well inside it.
#[async_trait]
pub trait InterestRegistry: Send + Sync {
    async fn refresh(&self, tables: Vec<TableId>);
    async fn prune(&self);
}

/// A cross-node invalidation, lifted off the wire format so the engine doesn't
/// depend on the bus crate. The host's listener translates its bus events into
/// this and hands it to [`Coordinator::apply_invalidation`].
pub enum Invalidation {
    /// A precise committed change-set.
    Changes(ChangeSet),
    /// Re-evaluate every subscription on these tables (coarse, payload too big to
    /// carry precisely but the touched tables are known).
    Tables(HashSet<TableId>),
    /// Re-evaluate everything (coarsest fallback).
    All,
}

/// Cross-node "network layer" counters + latency decomposition, owned here so
/// `apply_invalidation` is the single writer. `busLagMs` = commit→deliver (bus
/// propagation); `applyMs` = deliver→applied (match + re-exec + push).
#[derive(Default)]
pub struct BusMetrics {
    pub events: AtomicU64,
    pub changes: AtomicU64,
    pub lag_us_sum: AtomicU64,
    pub apply_us_sum: AtomicU64,
    pub resyncs: AtomicU64,
}

/// A successful RPC: the value to return now, plus (for a mutation) the stamped
/// change-set to propagate off the response path via [`Coordinator::propagate`].
#[derive(Debug)]
pub struct RpcOk {
    pub value: Value,
    pub propagate: Option<ChangeSet>,
}

/// The heartbeat must refresh interest well inside the TTL, or a live node's
/// interest lapses and it silently misses invalidations — clamp the configured
/// beat to TTL/3 (min 1 ms).
pub fn clamp_heartbeat(ttl_secs: i64, heartbeat_ms: u64) -> u64 {
    let max = (ttl_secs.max(1) as u64) * 1000 / 3;
    if heartbeat_ms > max {
        max.max(1)
    } else {
        heartbeat_ms
    }
}

/// The deep module: all of the engine's request + cross-node orchestration behind
/// a small interface, sitting over the four host seams.
pub struct Coordinator {
    reactor: Arc<dyn Reactor>,
    executor: Arc<dyn Executor>,
    publisher: Arc<dyn Publisher>,
    interest: Arc<dyn InterestRegistry>,
    /// Latest sampled WAL position (written by the host's WAL-sampler timer). A
    /// mutation stamps its change-set's commit watermark from this instead of a
    /// per-write WAL round-trip — WAL only advances, so it stays monotonic.
    wal_lsn: Arc<AtomicU64>,
    metrics: Arc<BusMetrics>,
}

impl Coordinator {
    pub fn new(
        reactor: Arc<dyn Reactor>,
        executor: Arc<dyn Executor>,
        publisher: Arc<dyn Publisher>,
        interest: Arc<dyn InterestRegistry>,
        wal_lsn: Arc<AtomicU64>,
    ) -> Self {
        Coordinator {
            reactor,
            executor,
            publisher,
            interest,
            wal_lsn,
            metrics: Arc::new(BusMetrics::default()),
        }
    }

    pub fn metrics(&self) -> &BusMetrics {
        &self.metrics
    }

    /// Run a procedure. On a mutation that produced writes, the result carries a
    /// `propagate` change-set already stamped with the sampled commit watermark;
    /// the caller spawns [`Coordinator::propagate`] for it off the response path.
    pub async fn handle_rpc(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
        mutation_id: Option<String>,
    ) -> Result<RpcOk, ExecError> {
        if !self.executor.contains(&path) {
            return Err(ExecError::not_found(&path));
        }
        let res = self
            .executor
            .execute(path, input, headers, mutation_id)
            .await?;
        let propagate = if res.changes.is_empty() {
            None
        } else {
            // Stamp from the sampled WAL position — no per-write WAL round-trip.
            let commit_lsn = Lsn(self.wal_lsn.load(Relaxed));
            Some(ChangeSet {
                commit_lsn,
                changes: res.changes,
            })
        };
        Ok(RpcOk {
            value: res.value,
            propagate,
        })
    }

    /// Fold a committed mutation into local subscribers and publish it to other
    /// nodes — the local apply running in parallel with the routed publish, the
    /// shape the host runs off the RPC's critical path (`tokio::spawn`).
    pub async fn propagate(&self, change_set: ChangeSet) {
        let reactor = self.reactor.clone();
        let local = {
            let cs = change_set.clone();
            tokio::spawn(async move { reactor.apply_change_set(cs).await })
        };
        self.publisher.publish(&change_set).await;
        let _ = local.await;
    }

    /// Register a reactive subscription: run it for the initial value + read-set,
    /// add it (which announces fresh interest), push the initial result, then a
    /// catch-up re-exec to close the subscribe-vs-remote-write race. Order matters.
    pub async fn handle_subscribe(
        &self,
        client_id: String,
        sub: String,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
    ) -> Result<(), ExecError> {
        let res = self
            .executor
            .execute(path.clone(), input.clone(), headers.clone(), None)
            .await?;
        self.reactor
            .add_subscription(Subscription {
                client_id: client_id.clone(),
                sub: sub.clone(),
                path,
                input,
                headers,
                read_set: res.read_set,
                last: Some(res.value.clone()),
                last_lsn: Lsn::ZERO,
            })
            .await;
        // Initial push reflects no committed change yet → LSN zero.
        self.reactor
            .push(&client_id, &sub, &res.value, Lsn::ZERO)
            .await;
        // Catch-up: interest is registered now, so re-evaluate once to pick up any
        // remote write between the initial snapshot and interest registration. A
        // dedup'd no-op when nothing was missed (the common case).
        self.reactor.refresh_subscription(&client_id, &sub).await;
        Ok(())
    }

    pub async fn register_client(&self, client_id: String) -> mpsc::Receiver<SsePush> {
        self.reactor.register_client(client_id).await
    }

    pub async fn handle_unsubscribe(&self, client_id: &str, sub: &str) {
        self.reactor.remove_subscription(client_id, sub).await;
    }

    /// Apply a cross-node invalidation and account for it. `lag_us` is the
    /// commit→deliver propagation sample (computed at the host edge, where the
    /// wall clock lives); `Some` only for a precise change-set.
    pub async fn apply_invalidation(&self, inv: Invalidation, lag_us: Option<u64>) {
        self.metrics.events.fetch_add(1, Relaxed);
        let started = Instant::now();
        match inv {
            Invalidation::Changes(cs) => {
                if let Some(lag) = lag_us {
                    self.metrics.lag_us_sum.fetch_add(lag, Relaxed);
                    self.metrics.changes.fetch_add(1, Relaxed);
                }
                self.reactor.apply_change_set(cs).await;
            }
            Invalidation::Tables(tables) => {
                self.metrics.resyncs.fetch_add(1, Relaxed);
                self.reactor.invalidate_tables(tables).await;
            }
            Invalidation::All => {
                self.metrics.resyncs.fetch_add(1, Relaxed);
                self.reactor.invalidate_all().await;
            }
        }
        self.metrics
            .apply_us_sum
            .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
    }

    /// Keep this node's interest rows fresh and prune dead nodes' — the heartbeat
    /// body. Refreshes exactly the tables the reactor currently watches.
    pub async fn on_heartbeat(&self) {
        let tables = self.reactor.interest_tables().await;
        self.interest.refresh(tables).await;
        self.interest.prune().await;
    }
}

#[cfg(test)]
mod tests;
