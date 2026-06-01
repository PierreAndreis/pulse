//! The reactor: registers each reactive query's read-set, intersects incoming
//! `ChangeSet`s against the registry, suppresses redundant pushes, and drives
//! re-execution + SSE fan-out.
//!
//! Re-execution is injected via the [`ReExecutor`] trait so this crate stays
//! free of the JS-runtime dependency, and invalidation flows through the single
//! [`Reactor::apply_change_set`] entry point — the seam a future WAL/CDC consumer
//! (or a cross-node bus) plugs into without a second matching path.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};

use pulse_core::{ChangeSet, Lsn, ReadSet};

/// One server→client SSE event: a monotonic per-client `id` (for `Last-Event-ID`
/// resume) and the JSON body.
#[derive(Clone)]
pub struct SsePush {
    pub id: u64,
    pub body: String,
}

#[derive(Clone)]
pub struct Subscription {
    pub client_id: String,
    pub sub: String,
    pub path: Vec<String>,
    pub input: Value,
    pub headers: HashMap<String, String>,
    pub read_set: ReadSet,
    /// Last value pushed, for redundant-push suppression.
    pub last: Option<Value>,
}

/// Re-executes a procedure for invalidation. Implemented by the host over its
/// function runtime (e.g. `pulse-jsruntime::Worker`).
#[async_trait]
pub trait ReExecutor: Send + Sync {
    async fn exec(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
    ) -> Result<Value, String>;
}

/// The reactor surface used by the HTTP layer.
#[async_trait]
pub trait Reactor: Send + Sync {
    async fn register_client(&self, client_id: String) -> mpsc::Receiver<SsePush>;
    async fn remove_client(&self, client_id: &str);
    async fn add_subscription(&self, sub: Subscription);
    async fn remove_subscription(&self, client_id: &str, sub: &str);
    /// Push a value to one subscription (used for the initial subscribe result).
    async fn push(&self, client_id: &str, sub: &str, value: &Value, commit_lsn: Lsn) -> bool;
    /// The single invalidation entry point: match → dedup → re-exec → diff → push.
    async fn apply_change_set(&self, change_set: ChangeSet);
    /// Re-evaluate every subscription regardless of read-set. Safe (over-broad)
    /// fallback used when a precise change-set can't be delivered (e.g. an
    /// oversized cross-node bus payload).
    async fn invalidate_all(&self);
}

struct Client {
    tx: mpsc::Sender<SsePush>,
    seq: u64,
}

fn sub_key(client_id: &str, sub: &str) -> String {
    format!("{client_id}\u{0}{sub}")
}

/// In-process reactor: dashmap-free `Mutex<HashMap>` registry plus an injected
/// re-executor. Single-node; the `Reactor` trait is the seam for a distributed
/// implementation later.
pub struct InMemoryReactor {
    clients: Mutex<HashMap<String, Client>>,
    subs: Mutex<HashMap<String, Subscription>>,
    reexec: Arc<dyn ReExecutor>,
}

impl InMemoryReactor {
    pub fn new(reexec: Arc<dyn ReExecutor>) -> Self {
        InMemoryReactor {
            clients: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            reexec,
        }
    }

    /// Assign the next per-client seq and enqueue a push. False if disconnected.
    ///
    /// The bounded-channel `send().await` is performed OUTSIDE the clients lock:
    /// we bump the seq and clone the sender under the lock, then release it before
    /// awaiting. Otherwise one stalled consumer (a full buffer) would park while
    /// holding the global lock and wedge every other client's push + register/
    /// remove — head-of-line blocking the whole reactor on the slowest browser.
    async fn send(&self, client_id: &str, sub: &str, value: &Value, commit_lsn: Lsn) -> bool {
        let (tx, id) = {
            let mut clients = self.clients.lock().await;
            let Some(client) = clients.get_mut(client_id) else {
                return false;
            };
            client.seq += 1;
            (client.tx.clone(), client.seq)
        };
        let body = json!({
            "sub": sub,
            "id": id.to_string(),
            "seq": id,
            "commitLsn": commit_lsn.to_string(),
            "data": value,
        })
        .to_string();
        tx.send(SsePush { id, body }).await.is_ok()
    }

    /// Re-execute the given subscriptions and push any changed result, stamped
    /// with `commit_lsn`. Shared by precise (`apply_change_set`) and coarse
    /// (`invalidate_all`) invalidation.
    ///
    /// The expensive part of each re-exec (a worker round-trip + Postgres query)
    /// runs CONCURRENTLY across the dirty set — a single write can fan out to many
    /// subscriptions, and running them serially would make the tail of the wave
    /// wait behind every prior round-trip. The cheap, state-touching follow-up
    /// (`record_value` + `send`) is applied per-sub as each result arrives.
    async fn reexec_and_push(&self, dirty: Vec<Subscription>, commit_lsn: Lsn) {
        let mut set = tokio::task::JoinSet::new();
        for sub in dirty {
            let reexec = self.reexec.clone();
            set.spawn(async move {
                let result = reexec
                    .exec(sub.path.clone(), sub.input.clone(), sub.headers.clone())
                    .await;
                (sub, result)
            });
        }
        while let Some(joined) = set.join_next().await {
            let Ok((sub, result)) = joined else { continue }; // task panic → skip
            match result {
                Ok(value) => {
                    if !self.record_value(&sub.client_id, &sub.sub, &value).await {
                        continue; // unchanged result → no redundant push
                    }
                    if !self
                        .send(&sub.client_id, &sub.sub, &value, commit_lsn)
                        .await
                    {
                        self.remove_client(&sub.client_id).await;
                    }
                }
                Err(code) => {
                    tracing::warn!("re-exec of subscription {} failed: {}", sub.sub, code);
                }
            }
        }
    }

    /// True if `value` differs from the subscription's last pushed value (and
    /// records it). Skips byte-identical recomputations.
    async fn record_value(&self, client_id: &str, sub: &str, value: &Value) -> bool {
        let mut subs = self.subs.lock().await;
        match subs.get_mut(&sub_key(client_id, sub)) {
            Some(s) => {
                if s.last.as_ref() == Some(value) {
                    false
                } else {
                    s.last = Some(value.clone());
                    true
                }
            }
            None => false,
        }
    }
}

#[async_trait]
impl Reactor for InMemoryReactor {
    async fn register_client(&self, client_id: String) -> mpsc::Receiver<SsePush> {
        let (tx, rx) = mpsc::channel(256);
        self.clients
            .lock()
            .await
            .insert(client_id, Client { tx, seq: 0 });
        rx
    }

    async fn remove_client(&self, client_id: &str) {
        self.clients.lock().await.remove(client_id);
        self.subs
            .lock()
            .await
            .retain(|_, s| s.client_id != client_id);
    }

    async fn add_subscription(&self, sub: Subscription) {
        let key = sub_key(&sub.client_id, &sub.sub);
        self.subs.lock().await.insert(key, sub);
    }

    async fn remove_subscription(&self, client_id: &str, sub: &str) {
        self.subs.lock().await.remove(&sub_key(client_id, sub));
    }

    async fn push(&self, client_id: &str, sub: &str, value: &Value, commit_lsn: Lsn) -> bool {
        self.send(client_id, sub, value, commit_lsn).await
    }

    async fn apply_change_set(&self, change_set: ChangeSet) {
        // Match (dedup: a multi-row tx re-runs each sub at most once).
        let dirty: Vec<Subscription> = {
            let subs = self.subs.lock().await;
            subs.values()
                .filter(|s| s.read_set.matches(&change_set))
                .cloned()
                .collect()
        };
        self.reexec_and_push(dirty, change_set.commit_lsn).await;
    }

    async fn invalidate_all(&self) {
        let all: Vec<Subscription> = self.subs.lock().await.values().cloned().collect();
        self.reexec_and_push(all, Lsn::ZERO).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{
        Change, ChangeOp, ChangeSet, Cond, Filter, FilterOp, KeyValue, PrimaryKey, ReadSet, TableId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Re-executor that records how many times it ran and returns a fixed value.
    struct CountingReExec {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ReExecutor for CountingReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            _input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<Value, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Return a fresh value each call so the diff never suppresses the push.
            Ok(json!({ "n": self.calls.load(Ordering::SeqCst) }))
        }
    }

    fn channel_filter(value: &str) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "channelId".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Text(value.into()),
                }],
            },
        );
        rs
    }

    fn insert_into(channel: &str) -> Change {
        let mut new = HashMap::new();
        new.insert("channelId".to_string(), KeyValue::Text(channel.into()));
        Change {
            table: TableId::new("messages"),
            key: PrimaryKey::single(KeyValue::Int(1)),
            op: ChangeOp::Insert,
            new: Some(new),
            old: None,
        }
    }

    async fn sub(reactor: &InMemoryReactor, client: &str, channel: &str) {
        reactor
            .add_subscription(Subscription {
                client_id: client.into(),
                sub: format!("messages.list::{channel}"),
                path: vec!["messages".into(), "list".into()],
                input: json!({ "channelId": channel }),
                headers: HashMap::new(),
                read_set: channel_filter(channel),
                last: None,
            })
            .await;
    }

    /// An injected ChangeSet drives matching + re-execution via the trait alone,
    /// re-running only the matching subscription.
    #[tokio::test]
    async fn apply_change_set_reexecutes_only_matching_subs() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());

        let mut rx_a = reactor.register_client("a".into()).await;
        let mut rx_b = reactor.register_client("b".into()).await;
        sub(&reactor, "a", "A").await;
        sub(&reactor, "b", "B").await;

        // A change into channel A must re-run exactly one subscription (A's).
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;

        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            1,
            "only the matching sub re-executes"
        );
        assert!(rx_a.try_recv().is_ok(), "channel-A client received a push");
        assert!(
            rx_b.try_recv().is_err(),
            "channel-B client received nothing"
        );
    }

    /// Re-executor that blocks until N calls are concurrently in flight (a
    /// barrier). If the reactor re-executes serially, the first call waits on the
    /// barrier forever and the others never start → the whole apply_change_set
    /// hangs. Parallel re-exec lets all N rendezvous and complete.
    struct BarrierReExec {
        barrier: Arc<tokio::sync::Barrier>,
    }
    #[async_trait]
    impl ReExecutor for BarrierReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<Value, String> {
            self.barrier.wait().await; // only releases once all N are here
            Ok(input)
        }
    }

    /// Matching subscriptions must re-execute concurrently, not one-at-a-time —
    /// otherwise a fan-out wave's tail waits behind every prior worker round-trip.
    #[tokio::test]
    async fn matching_subs_reexecute_in_parallel() {
        const N: usize = 8;
        let reactor = InMemoryReactor::new(Arc::new(BarrierReExec {
            barrier: Arc::new(tokio::sync::Barrier::new(N)),
        }));

        // N subscriptions on the SAME channel — one change fans out to all N.
        for i in 0..N {
            reactor.register_client(format!("c{i}")).await;
            sub(&reactor, &format!("c{i}"), "A").await;
        }

        let applied = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reactor.apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            }),
        )
        .await;

        assert!(
            applied.is_ok(),
            "re-exec must run in parallel; serial execution deadlocks on the barrier"
        );
    }

    /// A re-executor that returns a unique value each call (so every push goes
    /// through), used to drive many pushes.
    struct UniqueReExec {
        n: AtomicUsize,
    }
    #[async_trait]
    impl ReExecutor for UniqueReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            _input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<Value, String> {
            Ok(json!({ "n": self.n.fetch_add(1, Ordering::SeqCst) }))
        }
    }

    /// A client whose SSE buffer is full (a stalled/slow browser) must not hold
    /// the global clients lock. If `send` awaits the bounded channel WHILE holding
    /// that lock, a full buffer parks every other client lookup — including
    /// register/remove — wedging the whole reactor on one slow consumer.
    #[tokio::test]
    async fn a_stalled_client_does_not_hold_the_clients_lock() {
        let reactor = Arc::new(InMemoryReactor::new(Arc::new(UniqueReExec {
            n: AtomicUsize::new(0),
        })));
        // "slow" never drains its receiver; keep rx alive so the channel stays open.
        let _rx_slow = reactor.register_client("slow".into()).await;

        // Spawn a flood of pushes to "slow". The channel cap is 256, so after it
        // fills, the next push parks inside send().await.
        let r2 = reactor.clone();
        tokio::spawn(async move {
            for _ in 0..512 {
                r2.push(
                    "slow",
                    "messages.list::A",
                    &json!({}),
                    pulse_core::Lsn::ZERO,
                )
                .await;
            }
        });
        // Let the flood fill the buffer and park.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Another operation that needs the clients lock must still complete fast.
        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reactor.register_client("other".into()),
        )
        .await;

        assert!(
            ok.is_ok(),
            "register_client wedged behind a stalled client's push — the clients \
             lock must not be held across the channel send().await"
        );
    }
}
