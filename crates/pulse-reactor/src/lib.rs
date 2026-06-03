//! The reactor: registers each reactive query's read-set, intersects incoming
//! `ChangeSet`s against the registry, suppresses redundant pushes, and drives
//! re-execution + SSE fan-out.
//!
//! Re-execution is injected via the [`ReExecutor`] trait so this crate stays
//! free of the JS-runtime dependency, and invalidation flows through the single
//! [`Reactor::apply_change_set`] entry point — the seam a future WAL/CDC consumer
//! (or a cross-node bus) plugs into without a second matching path.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};

use pulse_core::{ChangeSet, Lsn, ReadSet, TableId};

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
    /// Re-run a procedure, returning its result AND the fresh read-set captured
    /// during that run. The reactor stores the new read-set so a query's
    /// dependencies stay current as the data it reads changes (e.g. a join that
    /// starts referencing a newly-inserted row).
    async fn exec(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
    ) -> Result<(Value, ReadSet), String>;
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
    /// Re-evaluate every subscription referencing any of `tables`, regardless of
    /// row-level predicates. Scoped over-approximation used when a precise
    /// change-set can't be delivered but the touched tables are known (an oversized
    /// cross-node payload that still carried its table list).
    async fn invalidate_tables(&self, tables: HashSet<TableId>);
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

/// The subscription registry plus a coarse table→subscriptions index. The index
/// lets `apply_change_set` consider only the subscriptions that reference a table
/// the change actually touched, instead of scanning every subscription — so
/// matching cost scales with the number of subs on the affected tables, not the
/// global subscription count. The precise `ReadSet::matches` still runs on each
/// candidate, so the index never changes *which* subs match, only how few we test.
#[derive(Default)]
struct Registry {
    subs: HashMap<String, Subscription>,
    by_table: HashMap<TableId, HashSet<String>>,
}

impl Registry {
    fn index(&mut self, key: &str, rs: &ReadSet) {
        for t in rs.referenced_tables() {
            self.by_table.entry(t).or_default().insert(key.to_string());
        }
    }

    fn deindex(&mut self, key: &str, rs: &ReadSet) {
        for t in rs.referenced_tables() {
            if let Some(set) = self.by_table.get_mut(&t) {
                set.remove(key);
                if set.is_empty() {
                    self.by_table.remove(&t);
                }
            }
        }
    }

    fn insert(&mut self, key: String, sub: Subscription) {
        if let Some(old) = self.subs.get(&key) {
            let old_rs = old.read_set.clone();
            self.deindex(&key, &old_rs);
        }
        self.index(&key, &sub.read_set);
        self.subs.insert(key, sub);
    }

    fn remove(&mut self, key: &str) {
        if let Some(old) = self.subs.remove(key) {
            self.deindex(key, &old.read_set);
        }
    }

    /// Replace a subscription's read-set, reindexing if the set of referenced
    /// tables changed (a re-exec can shift dependencies — e.g. a join that begins
    /// reading a newly-inserted row's table). Missing this would let a later
    /// change to the new table be silently skipped for this subscription.
    fn set_read_set(&mut self, key: &str, rs: ReadSet) {
        let changed = match self.subs.get(key) {
            Some(s) => s.read_set.referenced_tables() != rs.referenced_tables(),
            None => return,
        };
        if changed {
            let old = self.subs[key].read_set.clone();
            self.deindex(key, &old);
            self.index(key, &rs);
        }
        if let Some(s) = self.subs.get_mut(key) {
            s.read_set = rs;
        }
    }

    /// Subscriptions referencing any of `tables` (deduplicated), cloned for
    /// lock-free re-execution. This is the index lookup that replaces a full scan.
    fn candidates(&self, tables: &HashSet<TableId>) -> Vec<Subscription> {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut out = Vec::new();
        for t in tables {
            let Some(keys) = self.by_table.get(t) else {
                continue;
            };
            for k in keys {
                if seen.insert(k.as_str()) {
                    if let Some(s) = self.subs.get(k) {
                        out.push(s.clone());
                    }
                }
            }
        }
        out
    }
}

/// In-process reactor: a `Mutex<Registry>` (subscriptions + table index) plus an
/// injected re-executor. Single-node; the `Reactor` trait is the seam for a
/// distributed implementation later.
pub struct InMemoryReactor {
    clients: Mutex<HashMap<String, Client>>,
    reg: Mutex<Registry>,
    reexec: Arc<dyn ReExecutor>,
}

impl InMemoryReactor {
    pub fn new(reexec: Arc<dyn ReExecutor>) -> Self {
        InMemoryReactor {
            clients: Mutex::new(HashMap::new()),
            reg: Mutex::new(Registry::default()),
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
        // Coalesce subscriptions that would compute the identical result (same
        // path + input + headers) into one re-execution, fanning the result to
        // every subscriber. A reactive query is a deterministic function of those
        // inputs at a given commit, so this is exact — and it collapses N tabs of
        // the same view (or many clients on the same public query) from N worker
        // round-trips + N Postgres queries down to one.
        let mut groups: HashMap<String, Vec<Subscription>> = HashMap::new();
        for sub in dirty {
            let key = Self::dedup_key_of(&sub.path, &sub.input, &sub.headers);
            groups.entry(key).or_default().push(sub);
        }
        let mut set = tokio::task::JoinSet::new();
        for (_key, subs) in groups {
            let reexec = self.reexec.clone();
            let (path, input, headers) = (
                subs[0].path.clone(),
                subs[0].input.clone(),
                subs[0].headers.clone(),
            );
            set.spawn(async move {
                let result = reexec.exec(path, input, headers).await;
                (subs, result)
            });
        }
        while let Some(joined) = set.join_next().await {
            let Ok((subs, result)) = joined else { continue }; // task panic → skip
            match result {
                Ok((value, read_set)) => {
                    for sub in subs {
                        if !self
                            .record_value(&sub.client_id, &sub.sub, &value, read_set.clone())
                            .await
                        {
                            continue; // unchanged for this subscriber → no redundant push
                        }
                        if !self
                            .send(&sub.client_id, &sub.sub, &value, commit_lsn)
                            .await
                        {
                            self.remove_client(&sub.client_id).await;
                        }
                    }
                }
                Err(code) => {
                    let label = subs.first().map(|s| s.sub.as_str()).unwrap_or("?");
                    tracing::warn!("re-exec of subscription {} failed: {}", label, code);
                }
            }
        }
    }

    /// Canonical key grouping subscriptions whose result must be identical: same
    /// procedure path, input, and headers (auth). Headers are sorted so order
    /// doesn't matter.
    fn dedup_key_of(path: &[String], input: &Value, headers: &HashMap<String, String>) -> String {
        let mut hs: Vec<(&String, &String)> = headers.iter().collect();
        hs.sort();
        json!({ "p": path, "i": input, "h": hs }).to_string()
    }

    /// Test-only: the matching half of `apply_change_set` in isolation (index
    /// lookup + precise `matches`), without re-exec/push side effects. Returns how
    /// many subscriptions a change set invalidates — used to benchmark the hot path.
    #[cfg(test)]
    async fn count_matches(&self, change_set: &ChangeSet) -> usize {
        let changed: HashSet<TableId> =
            change_set.changes.iter().map(|c| c.table.clone()).collect();
        let reg = self.reg.lock().await;
        reg.candidates(&changed)
            .into_iter()
            .filter(|s| s.read_set.matches(change_set))
            .count()
    }

    /// Refresh the subscription's read-set from the latest run (so dependencies
    /// track the data), and report whether `value` differs from the last pushed
    /// value (recording it). Skips byte-identical recomputations.
    async fn record_value(
        &self,
        client_id: &str,
        sub: &str,
        value: &Value,
        read_set: ReadSet,
    ) -> bool {
        let key = sub_key(client_id, sub);
        let mut reg = self.reg.lock().await;
        // Dependencies may have shifted since the last run → reindex if so.
        reg.set_read_set(&key, read_set);
        match reg.subs.get_mut(&key) {
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
        let mut reg = self.reg.lock().await;
        let keys: Vec<String> = reg
            .subs
            .iter()
            .filter(|(_, s)| s.client_id == client_id)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            reg.remove(&k);
        }
    }

    async fn add_subscription(&self, sub: Subscription) {
        let key = sub_key(&sub.client_id, &sub.sub);
        self.reg.lock().await.insert(key, sub);
    }

    async fn remove_subscription(&self, client_id: &str, sub: &str) {
        self.reg.lock().await.remove(&sub_key(client_id, sub));
    }

    async fn push(&self, client_id: &str, sub: &str, value: &Value, commit_lsn: Lsn) -> bool {
        self.send(client_id, sub, value, commit_lsn).await
    }

    async fn apply_change_set(&self, change_set: ChangeSet) {
        // Only subscriptions referencing a table this change touched can match, so
        // the index narrows the candidate set first; the precise `matches` then
        // runs on those (dedup: a multi-row tx re-runs each sub at most once).
        let changed: HashSet<TableId> =
            change_set.changes.iter().map(|c| c.table.clone()).collect();
        let dirty: Vec<Subscription> = {
            let reg = self.reg.lock().await;
            reg.candidates(&changed)
                .into_iter()
                .filter(|s| s.read_set.matches(&change_set))
                .collect()
        };
        self.reexec_and_push(dirty, change_set.commit_lsn).await;
    }

    async fn invalidate_tables(&self, tables: HashSet<TableId>) {
        // Re-exec every subscription on the affected tables (no row-level matching —
        // we don't have the rows). The index makes this scoped to those tables.
        let dirty: Vec<Subscription> = self.reg.lock().await.candidates(&tables);
        self.reexec_and_push(dirty, Lsn::ZERO).await;
    }

    async fn invalidate_all(&self) {
        let all: Vec<Subscription> = self.reg.lock().await.subs.values().cloned().collect();
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
        ) -> Result<(Value, ReadSet), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Return a fresh value each call so the diff never suppresses the push.
            Ok((
                json!({ "n": self.calls.load(Ordering::SeqCst) }),
                ReadSet::new(),
            ))
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
                read_cols: None,
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
        ) -> Result<(Value, ReadSet), String> {
            self.barrier.wait().await; // only releases once all N are here
            Ok((input, ReadSet::new()))
        }
    }

    /// Many clients on the SAME query (same path+input+headers) must share ONE
    /// re-execution, not one per client — the fix for per-client fan-out cost.
    #[tokio::test]
    async fn identical_subscriptions_share_one_reexec() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());

        let mut rxs = Vec::new();
        for c in ["a", "b", "c"] {
            rxs.push(reactor.register_client(c.into()).await);
            sub(&reactor, c, "A").await; // identical path + input + headers
        }

        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;

        // One execution shared across all three subscribers …
        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            1,
            "identical subs must coalesce"
        );
        // … and every subscriber still receives its push.
        for rx in &mut rxs {
            assert!(
                rx.try_recv().is_ok(),
                "each subscriber must get the fanned-out result"
            );
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

        // N subscriptions on DISTINCT channels — a change per channel fans out to
        // N *distinct* re-execs (identical subs would coalesce into one, so they
        // must differ to exercise parallelism).
        let mut changes = Vec::new();
        for i in 0..N {
            reactor.register_client(format!("c{i}")).await;
            sub(&reactor, &format!("c{i}"), &format!("A{i}")).await;
            changes.push(insert_into(&format!("A{i}")));
        }

        let applied = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reactor.apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes,
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
        ) -> Result<(Value, ReadSet), String> {
            Ok((
                json!({ "n": self.n.fetch_add(1, Ordering::SeqCst) }),
                ReadSet::new(),
            ))
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

    /// Re-executor that ALWAYS returns the same fixed value, regardless of how
    /// many times it runs. Unlike `CountingReExec` (fresh value each call), this
    /// lets the redundant-push suppression branch in `record_value` actually fire.
    struct ConstReExec {
        calls: AtomicUsize,
        // Returned (refreshed) on every re-exec so the subscription keeps matching
        // across applies; only the value stays constant — which is what exercises
        // the redundant-push suppression branch.
        read_set: ReadSet,
    }
    #[async_trait]
    impl ReExecutor for ConstReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            _input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<(Value, ReadSet), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok((json!({ "fixed": true }), self.read_set.clone()))
        }
    }

    /// A re-exec that yields a byte-identical result to the last pushed value must
    /// be suppressed: the first apply pushes, an identical second apply pushes
    /// nothing.
    #[tokio::test]
    async fn unchanged_result_suppresses_push() {
        let reexec = Arc::new(ConstReExec {
            calls: AtomicUsize::new(0),
            read_set: channel_filter("A"),
        });
        let reactor = InMemoryReactor::new(reexec.clone());

        let mut rx_a = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await;

        // First change → re-exec runs, value is new (last was None) → pushes.
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;
        assert!(rx_a.try_recv().is_ok(), "first apply pushes");

        // Second identical change → re-exec runs again and returns the SAME value,
        // so record_value returns false and no push is enqueued.
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;
        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            2,
            "re-exec runs on both applies"
        );
        assert!(
            rx_a.try_recv().is_err(),
            "identical second result is suppressed — no redundant push"
        );
    }

    /// A multi-row transaction (several changes into the same channel) must
    /// re-run a matching subscription at most once, with exactly one resulting
    /// push.
    #[tokio::test]
    async fn multi_row_tx_reexecutes_sub_once() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());

        let mut rx_a = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await;

        // Three changes, all into channel A, in one ChangeSet.
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A"), insert_into("A"), insert_into("A")],
            })
            .await;

        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            1,
            "a multi-row tx re-runs the matching sub exactly once"
        );
        assert!(rx_a.try_recv().is_ok(), "exactly one push arrives");
        assert!(rx_a.try_recv().is_err(), "and no second push");
    }

    /// `invalidate_all` re-evaluates every subscription regardless of read-set
    /// and stamps pushes with `Lsn::ZERO`.
    #[tokio::test]
    async fn invalidate_all_reexecutes_every_sub() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());

        let mut rx_a = reactor.register_client("a".into()).await;
        let mut rx_b = reactor.register_client("b".into()).await;
        sub(&reactor, "a", "A").await;
        sub(&reactor, "b", "B").await;

        reactor.invalidate_all().await;

        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            2,
            "every subscription is re-evaluated"
        );
        let push_a = rx_a.try_recv().expect("client A received a push");
        let push_b = rx_b.try_recv().expect("client B received a push");
        assert!(
            push_a.body.contains("\"commitLsn\":\"0/0\""),
            "invalidate_all stamps Lsn::ZERO"
        );
        assert!(
            push_b.body.contains("\"commitLsn\":\"0/0\""),
            "invalidate_all stamps Lsn::ZERO"
        );
    }

    /// `record_value` for a subscription that no longer exists returns false and
    /// does not panic (the sub was removed between match and the per-result
    /// follow-up).
    #[tokio::test]
    async fn record_value_for_unknown_sub_is_false() {
        let reactor = InMemoryReactor::new(Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        }));

        let recorded = reactor
            .record_value(
                "nobody",
                "messages.list::ghost",
                &json!({ "x": 1 }),
                ReadSet::new(),
            )
            .await;

        assert!(!recorded, "recording for an unknown sub is false");
    }

    /// A re-executor that re-points the subscription's read-set at a *different*
    /// table (`users`) on first run — simulating a dynamic dependency shift (e.g.
    /// a join that begins reading a new table).
    struct RemapReExec {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl ReExecutor for RemapReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            _input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<(Value, ReadSet), String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let mut rs = ReadSet::new();
            rs.add_table(TableId::new("users")); // now depends on `users`, not `messages`
            Ok((json!({ "n": n }), rs))
        }
    }

    fn insert_on(table: &str) -> Change {
        Change {
            table: TableId::new(table),
            key: PrimaryKey::single(KeyValue::Int(1)),
            op: ChangeOp::Insert,
            new: Some(HashMap::new()),
            old: None,
        }
    }

    /// The table index must follow a read-set that shifts at runtime: after a
    /// re-exec re-points a sub from `messages` to `users`, a later change to
    /// `users` must invalidate it, and a change to `messages` must no longer.
    #[tokio::test]
    async fn read_set_change_reindexes_the_subscription() {
        let reexec = Arc::new(RemapReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await; // initial read-set references `messages`

        // A change to `messages` matches → re-exec → read-set re-points to `users`.
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;
        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
        assert!(rx.try_recv().is_ok());

        // Now a change to `users` must invalidate it (proves the index gained `users`).
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_on("users")],
            })
            .await;
        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            2,
            "a change to the newly-referenced table must re-exec"
        );

        // …and a change to `messages` must NOT (proves the index dropped `messages`).
        reactor
            .apply_change_set(ChangeSet {
                commit_lsn: pulse_core::Lsn::ZERO,
                changes: vec![insert_into("A")],
            })
            .await;
        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            2,
            "a change to the no-longer-referenced table must be pruned"
        );
    }

    /// A coarse table-scoped invalidation (the oversized-bus-payload path) must
    /// re-exec only subscriptions on the named tables, leaving other tables alone.
    #[tokio::test]
    async fn invalidate_tables_is_scoped_to_named_tables() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await; // read-set references `messages`

        // A second sub on an unrelated table (`users`).
        let mut users_rs = ReadSet::new();
        users_rs.add_table(TableId::new("users"));
        reactor
            .add_subscription(Subscription {
                client_id: "a".into(),
                sub: "users::list".into(),
                path: vec!["users".into(), "list".into()],
                input: json!({}),
                headers: HashMap::new(),
                read_set: users_rs,
                last: None,
            })
            .await;

        reactor
            .invalidate_tables(HashSet::from([TableId::new("messages")]))
            .await;

        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            1,
            "only the `messages` sub re-execs; the `users` sub is untouched"
        );
    }

    /// Register `hot` subs on the changed table and `idle` subs on an unrelated
    /// table, all on one client.
    async fn seed_subs(reactor: &InMemoryReactor, hot: usize, idle: usize) {
        reactor.register_client("c".into()).await;
        for i in 0..hot {
            sub(reactor, "c", &format!("HOT{i}")).await; // read-set references `messages`
        }
        for i in 0..idle {
            let mut rs = ReadSet::new();
            rs.add_table(TableId::new("other"));
            reactor
                .add_subscription(Subscription {
                    client_id: "c".into(),
                    sub: format!("other::{i}"),
                    path: vec!["other".into()],
                    input: json!({ "i": i }),
                    headers: HashMap::new(),
                    read_set: rs,
                    last: None,
                })
                .await;
        }
    }

    /// Benchmark: matching cost scales with subscriptions on the *changed* table,
    /// not the global subscription count. With the index, piling on idle subs (on
    /// other tables) leaves a change's matching cost flat; the pre-index full scan
    /// was O(total subs).
    ///   cargo test -p pulse-reactor --release -- --ignored --nocapture bench_matching
    #[tokio::test]
    #[ignore = "benchmark; run explicitly with --ignored --nocapture"]
    async fn bench_matching_scales_with_changed_table() {
        use std::time::Instant;

        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let cs = ChangeSet {
            commit_lsn: pulse_core::Lsn::ZERO,
            changes: vec![insert_into("HOT0")],
        };
        const ITERS: usize = 2_000;

        // Fixed small hot set, growing idle set → cost should stay flat.
        println!("-- 10 hot subs, growing idle set (matching should stay flat) --");
        for &idle in &[0usize, 1_000, 10_000, 100_000] {
            let reactor = InMemoryReactor::new(reexec.clone());
            seed_subs(&reactor, 10, idle).await;
            let start = Instant::now();
            let mut hits = 0;
            for _ in 0..ITERS {
                hits = reactor.count_matches(&cs).await;
            }
            let per = start.elapsed().as_nanos() as f64 / ITERS as f64;
            println!(
                "  idle={idle:>7} total={:>7}  match={per:>8.0} ns/change  (matched={hits})",
                idle + 10
            );
        }

        // Growing hot set → cost should scale with it (this is the real work).
        println!("-- growing hot set, no idle (matching scales with the hot table) --");
        for &hot in &[1usize, 100, 1_000, 10_000] {
            let reactor = InMemoryReactor::new(reexec.clone());
            seed_subs(&reactor, hot, 0).await;
            let start = Instant::now();
            for _ in 0..ITERS {
                reactor.count_matches(&cs).await;
            }
            let per = start.elapsed().as_nanos() as f64 / ITERS as f64;
            println!("  hot={hot:>7}  match={per:>8.0} ns/change");
        }
    }
}
