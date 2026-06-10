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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use pulse_core::{Change, ChangeSet, Lsn, TableId};
pub use pulse_reactor::{Reactor, Resume, SsePush, Subscription};
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
    /// WAL changes dropped because they echo an in-engine write already applied
    /// locally (Mode B: the engine keeps the synchronous fast-path; the WAL
    /// stream carries only out-of-band writes, plus echoes we filter here).
    pub deduped: AtomicU64,
}

/// How long an in-engine change stays in the dedup window — long enough to cover
/// the WAL poll round-trip the echo arrives on, short enough to stay tiny. A
/// false miss only costs a wasted re-exec; a false dedup (an out-of-band write
/// byte-identical to a just-applied in-engine one) produces the same result the
/// local apply already pushed, so it is harmless either way.
const DEDUP_TTL: Duration = Duration::from_secs(5);

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
    /// Recently applied in-engine changes, for deduping their WAL echo (Mode B).
    applied: Mutex<VecDeque<(Change, Instant)>>,
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
            applied: Mutex::new(VecDeque::new()),
        }
    }

    pub fn metrics(&self) -> &BusMetrics {
        &self.metrics
    }

    /// Count of subscription updates served by incremental view maintenance (no
    /// worker re-exec) — surfaced on `/metrics` for IVM hit-rate observability.
    pub fn ivm_applied(&self) -> u64 {
        self.reactor.ivm_applied()
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
        // Mode B: remember these changes so their WAL echo (the same commit,
        // seen again when the slot is polled) is deduped in `apply_wal` rather
        // than re-applied. Recorded before the local apply so the marker is in
        // place no matter how fast the echo arrives.
        self.remember_applied(&change_set.changes);
        let reactor = self.reactor.clone();
        let local = {
            let cs = change_set.clone();
            tokio::spawn(async move { reactor.apply_change_set(cs).await })
        };
        self.publisher.publish(&change_set).await;
        let _ = local.await;
    }

    /// Record in-engine-applied changes in the dedup window (pruning expired).
    fn remember_applied(&self, changes: &[Change]) {
        let mut applied = self.applied.lock().unwrap();
        let now = Instant::now();
        applied.retain(|(_, t)| now.duration_since(*t) < DEDUP_TTL);
        for c in changes {
            applied.push_back((c.clone(), now));
        }
    }

    /// Apply a change-set sourced from the WAL/CDC consumer. Out-of-band writes
    /// (raw SQL, other services) invalidate subscriptions like any other change;
    /// echoes of this node's own in-engine writes are dropped (Mode B), since
    /// `propagate` already applied them synchronously. Because both paths build
    /// row images through the same `text_to_key_value`, an echo is byte-equal to
    /// its recorded in-engine `Change`.
    pub async fn apply_wal(&self, change_set: ChangeSet, lag_us: Option<u64>) {
        let kept: Vec<Change> = {
            let mut applied = self.applied.lock().unwrap();
            let now = Instant::now();
            applied.retain(|(_, t)| now.duration_since(*t) < DEDUP_TTL);
            change_set
                .changes
                .into_iter()
                .filter(|c| match applied.iter().position(|(ac, _)| ac == c) {
                    Some(pos) => {
                        applied.remove(pos); // consume the marker — one echo per write
                        self.metrics.deduped.fetch_add(1, Relaxed);
                        false
                    }
                    None => true,
                })
                .collect()
        };
        if kept.is_empty() {
            return;
        }
        self.apply_invalidation(
            Invalidation::Changes(ChangeSet {
                commit_lsn: change_set.commit_lsn,
                changes: kept,
            }),
            lag_us,
        )
        .await;
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

    /// Reconnect a client, optionally resuming from a `Last-Event-ID`. The
    /// returned receiver already has any replayed events (or a `resync` control
    /// frame) enqueued ahead of live delivery.
    pub async fn register_client_resume(
        &self,
        client_id: String,
        last_event_id: Option<u64>,
    ) -> (mpsc::Receiver<SsePush>, Resume) {
        self.reactor
            .register_client_resume(client_id, last_event_id)
            .await
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
