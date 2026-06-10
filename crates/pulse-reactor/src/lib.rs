//! The reactor: registers each reactive query's read-set, intersects incoming
//! `ChangeSet`s against the registry, suppresses redundant pushes, and drives
//! re-execution + SSE fan-out.
//!
//! Re-execution is injected via the [`ReExecutor`] trait so this crate stays
//! free of the JS-runtime dependency, and invalidation flows through the single
//! [`Reactor::apply_change_set`] entry point — the seam a future WAL/CDC consumer
//! (or a cross-node bus) plugs into without a second matching path.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};

use pulse_core::{AggFunc, ChangeSet, KeyValue, Lsn, PrimaryKey, ReadSet, RowValues, TableId};

/// One server→client SSE event: a monotonic per-client `id` (for `Last-Event-ID`
/// resume) and the JSON body.
#[derive(Clone)]
pub struct SsePush {
    pub id: u64,
    pub body: String,
}

/// Outcome of a reconnect via [`Reactor::register_client_resume`].
#[derive(Debug, PartialEq, Eq)]
pub enum Resume {
    /// A fresh stream (no `Last-Event-ID` presented).
    Fresh,
    /// The buffer covered the gap: events after the resume point were replayed
    /// onto the new stream ahead of live delivery.
    Resumed,
    /// The gap rolled past the buffer (or the client is unknown): a `resync`
    /// control frame was enqueued and the client must re-subscribe.
    Resync,
}

/// A `resync` control frame body — tells the client to re-subscribe (its missed
/// events are no longer buffered).
fn resync_frame(id: u64) -> String {
    json!({ "id": id.to_string(), "seq": id, "type": "resync" }).to_string()
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
    /// Highest commit LSN emitted to this subscriber. The emitted `commitLsn` is
    /// clamped to be monotonically non-decreasing per subscription, so a client's
    /// watermark never goes backwards even if invalidations arrive out of order.
    #[doc(hidden)]
    pub last_lsn: Lsn,
}

/// The minimal slice of a subscription needed to re-execute and push: identity +
/// the (path, input, headers) that determine its result. Re-execution never reads
/// `read_set` or `last` (the matcher already ran; `last` is re-read under lock in
/// `record_value`), so the dirty set carries only these light fields — we don't
/// clone the read-set or the retained last-value for every invalidated sub.
#[derive(Clone)]
struct ReExecJob {
    client_id: String,
    sub: String,
    path: Vec<String>,
    input: Value,
    headers: HashMap<String, String>,
}

impl ReExecJob {
    fn of(s: &Subscription) -> Self {
        ReExecJob {
            client_id: s.client_id.clone(),
            sub: s.sub.clone(),
            path: s.path.clone(),
            input: s.input.clone(),
            headers: s.headers.clone(),
        }
    }
}

/// Notified when this node begins watching tables it wasn't watching before, so
/// the host can register cross-node interest (the bus routing table) *before* the
/// subscription relies on the bus for those tables — closing the subscribe-time
/// race where a remote write could otherwise be missed. Implemented by the host
/// over the change bus (e.g. `pulse_cdc::register_interest`).
#[async_trait]
pub trait InterestSink: Send + Sync {
    async fn on_tables_added(&self, tables: Vec<TableId>);
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
    /// Reconnect a client, optionally resuming from a `Last-Event-ID`. Returns a
    /// fresh receiver plus whether the gap was replayed ([`Resume::Resumed`]),
    /// needs a full re-subscribe ([`Resume::Resync`]), or is a fresh stream
    /// ([`Resume::Fresh`]). Replayed events are enqueued ahead of any live push.
    async fn register_client_resume(
        &self,
        client_id: String,
        last_event_id: Option<u64>,
    ) -> (mpsc::Receiver<SsePush>, Resume);
    async fn remove_client(&self, client_id: &str);
    async fn add_subscription(&self, sub: Subscription);
    async fn remove_subscription(&self, client_id: &str, sub: &str);
    /// Push a value to one subscription (used for the initial subscribe result).
    async fn push(&self, client_id: &str, sub: &str, value: &Value, commit_lsn: Lsn) -> bool;
    /// The single invalidation entry point: match → dedup → re-exec → diff → push.
    async fn apply_change_set(&self, change_set: ChangeSet);
    /// Re-execute one subscription now and push only if its result changed. Used as
    /// a catch-up right after a subscription registers cross-node interest, to pick
    /// up any remote write that landed between the initial query snapshot and the
    /// interest registration (closing the routed-bus subscribe race). A no-op push
    /// when nothing changed (the common case), thanks to last-value dedup.
    async fn refresh_subscription(&self, client_id: &str, sub: &str);
    /// Re-evaluate every subscription referencing any of `tables`, regardless of
    /// row-level predicates. Scoped over-approximation used when a precise
    /// change-set can't be delivered but the touched tables are known (an oversized
    /// cross-node payload that still carried its table list).
    async fn invalidate_tables(&self, tables: HashSet<TableId>);
    /// Re-evaluate every subscription regardless of read-set. Safe (over-broad)
    /// fallback used when a precise change-set can't be delivered (e.g. an
    /// oversized cross-node bus payload).
    async fn invalidate_all(&self);
    /// Count of subscription updates served incrementally (IVM) rather than by a
    /// worker re-exec — an observability signal exposed on `/metrics`.
    fn ivm_applied(&self) -> u64;
    /// The tables this node currently has at least one subscription on — the set to
    /// (re)register as cross-node interest on the bus heartbeat.
    async fn interest_tables(&self) -> Vec<TableId>;
}

struct Client {
    tx: mpsc::Sender<SsePush>,
    seq: u64,
    /// Bounded tail of recent pushes (newest at the back) for `Last-Event-ID`
    /// resume. Capacity matches the SSE channel, so a covered reconnect's replay
    /// always fits. Holds a contiguous id range `[seq-len+1 ..= seq]`.
    buffer: VecDeque<SsePush>,
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
    /// Index `key` under every table its read-set references. Returns the tables
    /// that became newly-watched by this node (their bucket was empty before), so
    /// the caller can register fresh cross-node interest for them.
    fn index(&mut self, key: &str, rs: &ReadSet) -> Vec<TableId> {
        let mut newly_watched = Vec::new();
        for t in rs.referenced_tables() {
            let bucket = self.by_table.entry(t.clone()).or_default();
            if bucket.is_empty() {
                newly_watched.push(t);
            }
            bucket.insert(key.to_string());
        }
        newly_watched
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

    fn insert(&mut self, key: String, sub: Subscription) -> Vec<TableId> {
        if let Some(old) = self.subs.get(&key) {
            let old_rs = old.read_set.clone();
            self.deindex(&key, &old_rs);
        }
        let newly_watched = self.index(&key, &sub.read_set);
        self.subs.insert(key, sub);
        newly_watched
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
    fn set_read_set(&mut self, key: &str, rs: ReadSet) -> Vec<TableId> {
        let changed = match self.subs.get(key) {
            Some(s) => s.read_set.referenced_tables() != rs.referenced_tables(),
            None => return Vec::new(),
        };
        let mut newly_watched = Vec::new();
        if changed {
            let old = self.subs[key].read_set.clone();
            self.deindex(key, &old);
            newly_watched = self.index(key, &rs);
        }
        if let Some(s) = self.subs.get_mut(key) {
            s.read_set = rs;
        }
        newly_watched
    }

    /// Visit each subscription referencing any of `tables`, once, calling `f` with
    /// a reference. The index lookup that replaces a full scan; matching/cloning
    /// decisions are left to the caller so we never clone subs we won't re-exec.
    fn for_each_candidate(&self, tables: &HashSet<TableId>, mut f: impl FnMut(&Subscription)) {
        let mut seen: HashSet<&str> = HashSet::new();
        for t in tables {
            let Some(keys) = self.by_table.get(t) else {
                continue;
            };
            for k in keys {
                if seen.insert(k.as_str()) {
                    if let Some(s) = self.subs.get(k) {
                        f(s);
                    }
                }
            }
        }
    }

    /// Re-exec jobs for every subscription on `tables`, no row-level matching (the
    /// coarse table-scoped invalidation path).
    fn table_jobs(&self, tables: &HashSet<TableId>) -> Vec<ReExecJob> {
        let mut out = Vec::new();
        self.for_each_candidate(tables, |s| out.push(ReExecJob::of(s)));
        out
    }

    /// Re-exec jobs for every subscription (the global-resync path).
    fn all_jobs(&self) -> Vec<ReExecJob> {
        self.subs.values().map(ReExecJob::of).collect()
    }
}

/// In-process reactor: a `Mutex<Registry>` (subscriptions + table index) plus an
/// injected re-executor. Single-node; the `Reactor` trait is the seam for a
/// distributed implementation later.
/// Default per-client SSE buffer (events queued before a slow client is dropped).
pub const DEFAULT_SSE_BUFFER: usize = 256;

pub struct InMemoryReactor {
    clients: Mutex<HashMap<String, Client>>,
    reg: Mutex<Registry>,
    reexec: Arc<dyn ReExecutor>,
    interest: Option<Arc<dyn InterestSink>>,
    /// Per-client SSE channel capacity — a bigger buffer tolerates burstier/slower
    /// clients at the cost of memory; a full buffer drops the client.
    sse_buffer: usize,
    /// Count of subscription updates served by incremental view maintenance (an
    /// aggregate maintained from the change delta) instead of a worker re-exec —
    /// an observability signal for the IVM hit-rate.
    ivm_applied: AtomicU64,
}

impl InMemoryReactor {
    pub fn new(reexec: Arc<dyn ReExecutor>) -> Self {
        InMemoryReactor {
            clients: Mutex::new(HashMap::new()),
            reg: Mutex::new(Registry::default()),
            reexec,
            interest: None,
            sse_buffer: DEFAULT_SSE_BUFFER,
            ivm_applied: AtomicU64::new(0),
        }
    }

    /// Attach an interest sink (cross-node bus routing). Builder-style; call before
    /// wrapping the reactor in an `Arc`.
    pub fn with_interest(mut self, sink: Arc<dyn InterestSink>) -> Self {
        self.interest = Some(sink);
        self
    }

    /// Set the per-client SSE buffer capacity (knob; defaults to [`DEFAULT_SSE_BUFFER`]).
    /// Builder-style; call before wrapping in an `Arc`. A zero is bumped to 1.
    pub fn with_sse_buffer(mut self, n: usize) -> Self {
        self.sse_buffer = n.max(1);
        self
    }

    /// Hand newly-watched tables to the interest sink (if any). Called after the
    /// registry lock is released so the sink's I/O never runs under the lock.
    async fn announce_interest(&self, tables: Vec<TableId>) {
        if tables.is_empty() {
            return;
        }
        if let Some(sink) = &self.interest {
            sink.on_tables_added(tables).await;
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
        let (tx, push) = {
            let mut clients = self.clients.lock().await;
            let Some(client) = clients.get_mut(client_id) else {
                return false;
            };
            client.seq += 1;
            let id = client.seq;
            // Body built under the lock (cheap CPU); only the bounded-channel
            // `send().await` stays outside it (the no-head-of-line invariant).
            let body = json!({
                "sub": sub,
                "id": id.to_string(),
                "seq": id,
                "commitLsn": commit_lsn.to_string(),
                "data": value,
            })
            .to_string();
            let push = SsePush { id, body };
            client.buffer.push_back(push.clone());
            while client.buffer.len() > self.sse_buffer {
                client.buffer.pop_front();
            }
            (client.tx.clone(), push)
        };
        tx.send(push).await.is_ok()
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
    async fn reexec_and_push(&self, dirty: Vec<ReExecJob>, commit_lsn: Lsn) {
        // Coalesce subscriptions that would compute the identical result (same
        // path + input + headers) into one re-execution, fanning the result to
        // every subscriber. A reactive query is a deterministic function of those
        // inputs at a given commit, so this is exact — and it collapses N tabs of
        // the same view (or many clients on the same public query) from N worker
        // round-trips + N Postgres queries down to one.
        let mut groups: HashMap<String, Vec<ReExecJob>> = HashMap::new();
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
                        let Some(effective_lsn) = self
                            .record_value(
                                &sub.client_id,
                                &sub.sub,
                                &value,
                                read_set.clone(),
                                commit_lsn,
                            )
                            .await
                        else {
                            continue; // unchanged for this subscriber → no redundant push
                        };
                        if !self
                            .send(&sub.client_id, &sub.sub, &value, effective_lsn)
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
        let mut n = 0;
        reg.for_each_candidate(&changed, |s| {
            if s.read_set.matches(change_set) {
                n += 1;
            }
        });
        n
    }

    /// Refresh the subscription's read-set from the latest run (so dependencies
    /// track the data) and advance its commit watermark. Returns `Some(effective_lsn)`
    /// — the monotonic LSN to stamp on the push — when `value` differs from the last
    /// pushed value, or `None` to suppress a byte-identical recomputation.
    ///
    /// `effective_lsn = max(commit_lsn, last_lsn)`: the watermark only ever advances,
    /// so a later-but-lower-LSN invalidation (out-of-order delivery, or a coarse
    /// resync stamped LSN zero) never regresses the client's commit position.
    async fn record_value(
        &self,
        client_id: &str,
        sub: &str,
        value: &Value,
        read_set: ReadSet,
        commit_lsn: Lsn,
    ) -> Option<Lsn> {
        let key = sub_key(client_id, sub);
        let (result, newly_watched) = {
            let mut reg = self.reg.lock().await;
            // Dependencies may have shifted since the last run → reindex if so.
            let newly_watched = reg.set_read_set(&key, read_set);
            let result = reg.subs.get_mut(&key).map(|s| {
                let effective = commit_lsn.max(s.last_lsn);
                s.last_lsn = effective;
                if s.last.as_ref() == Some(value) {
                    None
                } else {
                    s.last = Some(value.clone());
                    Some(effective)
                }
            });
            (result, newly_watched)
        };
        // A re-exec that began reading a new table must register interest in it too
        // (dynamic dependencies, e.g. a join reaching a new row's table).
        self.announce_interest(newly_watched).await;
        result.flatten()
    }
}

/// Maintain an aggregate subscription's scalar incrementally from a `ChangeSet`,
/// returning the new value — or `None` to fall back to a full re-execution. IVM
/// is opportunistic; `None` is always safe (the re-exec path is exact). Slice 1
/// supports bare `count(*)`; every other shape falls back.
fn try_ivm(sub: &Subscription, cs: &ChangeSet) -> Option<Value> {
    // Only a clean single scalar aggregate over one table is IVM-able: no coarse
    // table-wildcard, no point-key, exactly one filter on one table.
    let rs = &sub.read_set;
    if !rs.tables.is_empty() || !rs.keys.is_empty() || rs.filters.len() != 1 {
        return None;
    }
    let (table, filters) = rs.filters.iter().next()?;
    if filters.len() != 1 {
        return None;
    }
    let filter = &filters[0];
    let agg = filter.aggregate.as_ref()?;
    let last = sub.last.as_ref()?;

    // Coalesce multiple changes to the same row in this commit into ONE net
    // transition — first pre-image, last post-image. A row inserted-then-deleted in
    // the same tx is a no-op, not a transient member; an updated-twice row collapses
    // to its endpoints. count/sum sum deltas so they tolerate intermediate states,
    // but min/max would otherwise latch onto a phantom extreme that never persisted.
    let mut net: HashMap<&PrimaryKey, (Option<&RowValues>, Option<&RowValues>)> = HashMap::new();
    for c in &cs.changes {
        if &c.table != table {
            continue;
        }
        let slot = net
            .entry(&c.key)
            .or_insert((c.old.as_ref(), c.new.as_ref()));
        slot.1 = c.new.as_ref(); // keep the last post-image; .0 stays the first pre-image
    }
    let nets: Vec<(Option<&RowValues>, Option<&RowValues>)> = net.into_values().collect();

    match agg.func {
        // bare count(*): +1 per row entering the filter, −1 per row leaving it.
        AggFunc::Count if agg.field.is_none() && !agg.distinct => {
            let mut delta: i64 = 0;
            for (old, new) in &nets {
                let (old_m, new_m) = filter.membership_images(*old, *new);
                delta += new_m as i64 - old_m as i64;
            }
            Some(json!(last.as_i64()? + delta))
        }
        // sum(field): +new on enter, −old on leave, (new−old) on a stay where the
        // value changed. Maintained only for INTEGER fields — float/jsonb columns
        // aren't carried in the change image (so `int()` returns None → fall back),
        // which also keeps the arithmetic exact (no f64 drift). An empty set's sum
        // is SQL NULL, so a non-numeric `last` falls back too.
        AggFunc::Sum if agg.field.is_some() && !agg.distinct => {
            let field = agg.field.as_ref().unwrap();
            let base = last.as_f64()?; // Null (empty set) / non-numeric → fall back
            let int = |row: Option<&RowValues>| -> Option<i64> {
                match row?.get(field)? {
                    KeyValue::Int(n) => Some(*n),
                    _ => None,
                }
            };
            let mut delta: i64 = 0;
            for (old, new) in &nets {
                match filter.membership_images(*old, *new) {
                    (false, true) => delta += int(*new)?,             // entered
                    (true, false) => delta -= int(*old)?,             // left
                    (true, true) => delta += int(*new)? - int(*old)?, // stayed
                    (false, false) => {}                              // outside
                }
            }
            // A computed sum of exactly 0 is ambiguous: the set may be empty (SQL
            // `sum` → NULL) or its values may cancel to 0, and without a row count we
            // can't tell — so fall back and let the re-exec decide. Any *non-zero*
            // result proves a non-empty set, so it's safe to push (matches the
            // re-exec's bare-number shape).
            let result = base + delta as f64;
            if result == 0.0 {
                return None;
            }
            Some(json!(result))
        }
        // max(field) / min(field): a *rising* extreme (a row entering or growing
        // to/above the current extreme) is cheap to maintain from the delta — the
        // monotonic high-water-mark case stays fully incremental. But *removing* or
        // *lowering* the row that holds the extreme needs the next-best value, which
        // isn't in the change image, so we fall back.
        //
        // Integer fields only: the change image carries no floats (KeyValue must stay
        // Hash+Eq for primary keys), so a float/absent field → None → fall back. The
        // engine reads scalar aggregates as f64 (fetch_scalar_opt_f64), so `last` and
        // the output are f64 even for integer columns — matching sum and the re-exec
        // shape; an empty set's extreme is SQL NULL → non-numeric `last` falls back.
        AggFunc::Max | AggFunc::Min if agg.field.is_some() && !agg.distinct => {
            let is_max = matches!(agg.func, AggFunc::Max);
            let field = agg.field.as_ref().unwrap();
            let cur = last.as_f64()?; // Null (empty set) / non-numeric → fall back
            let val = |row: Option<&RowValues>| -> Option<f64> {
                match row?.get(field)? {
                    KeyValue::Int(n) => Some(*n as f64),
                    _ => None,
                }
            };
            // `toward(cur, v)` keeps the value closer to the extreme (the larger for
            // max, the smaller for min); `holds(v)` is true when v reaches the extreme.
            let toward = |a: f64, b: f64| if is_max { a.max(b) } else { a.min(b) };
            let holds = |v: f64| if is_max { v >= cur } else { v <= cur };

            let mut best: Option<f64> = None; // extreme among rows in-set after the change
            let mut lost = false; // the row holding `cur` left or moved off the extreme
            for (old, new) in &nets {
                match filter.membership_images(*old, *new) {
                    (false, true) => {
                        let v = val(*new)?;
                        best = Some(best.map_or(v, |b| toward(b, v)));
                    }
                    (true, false) => {
                        if holds(val(*old)?) {
                            lost = true;
                        }
                    }
                    (true, true) => {
                        let (o, n) = (val(*old)?, val(*new)?);
                        best = Some(best.map_or(n, |b| toward(b, n))); // n is still in-set
                        if holds(o) && !holds(n) {
                            lost = true;
                        }
                    }
                    (false, false) => {}
                }
            }
            let new_extreme = if !lost {
                // `cur` still has a holder → extreme is `cur` pulled toward by entrants.
                toward(cur, best.unwrap_or(cur))
            } else {
                // The holder of `cur` is gone; only a row that reaches `cur` can prove
                // the new extreme — otherwise the next-best is unknown → fall back.
                match best {
                    Some(b) if holds(b) => b,
                    _ => return None,
                }
            };
            Some(json!(new_extreme))
        }
        // avg / count(field) / count(distinct) → re-exec (later slices).
        _ => None,
    }
}

#[async_trait]
impl Reactor for InMemoryReactor {
    async fn register_client(&self, client_id: String) -> mpsc::Receiver<SsePush> {
        let (tx, rx) = mpsc::channel(self.sse_buffer);
        self.clients.lock().await.insert(
            client_id,
            Client {
                tx,
                seq: 0,
                buffer: VecDeque::new(),
            },
        );
        rx
    }

    async fn register_client_resume(
        &self,
        client_id: String,
        last_event_id: Option<u64>,
    ) -> (mpsc::Receiver<SsePush>, Resume) {
        let (tx, rx) = mpsc::channel(self.sse_buffer);
        let Some(n) = last_event_id else {
            // Fresh stream — no resume point.
            self.clients.lock().await.insert(
                client_id,
                Client {
                    tx,
                    seq: 0,
                    buffer: VecDeque::new(),
                },
            );
            return (rx, Resume::Fresh);
        };
        let mut clients = self.clients.lock().await;
        match clients.get_mut(&client_id) {
            Some(client) => {
                client.tx = tx.clone();
                // The buffer holds a contiguous id range, so the gap after `n` is
                // covered iff the oldest buffered id is ≤ n+1.
                let covered = match client.buffer.front() {
                    Some(oldest) => n + 1 >= oldest.id,
                    None => n == client.seq, // empty buffer: only if fully caught up
                };
                if covered {
                    // Replay every buffered event after the resume point, in order,
                    // ahead of any live push (try_send: capacity ≥ buffer len).
                    for p in client.buffer.iter().filter(|p| p.id > n) {
                        let _ = tx.try_send(p.clone());
                    }
                    (rx, Resume::Resumed)
                } else {
                    client.seq += 1;
                    let id = client.seq;
                    let _ = tx.try_send(SsePush {
                        id,
                        body: resync_frame(id),
                    });
                    (rx, Resume::Resync)
                }
            }
            None => {
                // Unknown client presenting a resume point (server restarted, or a
                // stale id) — nothing to replay; tell it to re-subscribe.
                let _ = tx.try_send(SsePush {
                    id: 1,
                    body: resync_frame(1),
                });
                clients.insert(
                    client_id,
                    Client {
                        tx,
                        seq: 1,
                        buffer: VecDeque::new(),
                    },
                );
                (rx, Resume::Resync)
            }
        }
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
        let newly_watched = self.reg.lock().await.insert(key, sub);
        self.announce_interest(newly_watched).await;
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
        // Partition matched subs: those whose result can be maintained from the
        // change delta (incremental view maintenance — no worker round-trip) vs.
        // the rest, which re-execute. IVM is opportunistic; `try_ivm` returns
        // `None` for anything it can't maintain precisely, so correctness is the
        // re-exec path's.
        let (ivm, dirty) = {
            let reg = self.reg.lock().await;
            let mut ivm: Vec<(String, String, ReadSet, Value)> = Vec::new();
            let mut dirty = Vec::new();
            reg.for_each_candidate(&changed, |s| {
                if !s.read_set.matches(&change_set) {
                    return;
                }
                match try_ivm(s, &change_set) {
                    Some(value) => ivm.push((
                        s.client_id.clone(),
                        s.sub.clone(),
                        s.read_set.clone(),
                        value,
                    )),
                    None => dirty.push(ReExecJob::of(s)),
                }
            });
            (ivm, dirty)
        };
        self.ivm_applied.fetch_add(ivm.len() as u64, Relaxed);
        // IVM pushes reuse record_value (diff-suppression + watermark clamp) and
        // send, exactly like a re-exec push — minus the worker call.
        for (client_id, sub, read_set, value) in ivm {
            if let Some(lsn) = self
                .record_value(&client_id, &sub, &value, read_set, change_set.commit_lsn)
                .await
            {
                if !self.send(&client_id, &sub, &value, lsn).await {
                    self.remove_client(&client_id).await;
                }
            }
        }
        self.reexec_and_push(dirty, change_set.commit_lsn).await;
    }

    async fn refresh_subscription(&self, client_id: &str, sub: &str) {
        let job = {
            let reg = self.reg.lock().await;
            reg.subs.get(&sub_key(client_id, sub)).map(ReExecJob::of)
        };
        if let Some(job) = job {
            self.reexec_and_push(vec![job], Lsn::ZERO).await;
        }
    }

    async fn invalidate_tables(&self, tables: HashSet<TableId>) {
        // Re-exec every subscription on the affected tables (no row-level matching —
        // we don't have the rows). The index makes this scoped to those tables.
        let dirty = self.reg.lock().await.table_jobs(&tables);
        self.reexec_and_push(dirty, Lsn::ZERO).await;
    }

    async fn invalidate_all(&self) {
        let all = self.reg.lock().await.all_jobs();
        self.reexec_and_push(all, Lsn::ZERO).await;
    }

    fn ivm_applied(&self) -> u64 {
        self.ivm_applied.load(Relaxed)
    }

    async fn interest_tables(&self) -> Vec<TableId> {
        self.reg.lock().await.by_table.keys().cloned().collect()
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
                aggregate: None,
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
                last_lsn: Lsn::ZERO,
            })
            .await;
    }

    // ── Incremental view maintenance (IVM) ───────────────────────────────────

    /// A reactive bare `count()` over `channelId = channel`.
    fn count_filter(channel: &str) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "channelId".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Text(channel.into()),
                }],
                read_cols: Some(vec![]), // bare count → value-only updates pruned
                aggregate: Some(pulse_core::Aggregate {
                    func: AggFunc::Count,
                    field: None,
                    distinct: false,
                }),
            },
        );
        rs
    }

    fn insert_keyed(channel: &str, key: i64) -> Change {
        Change {
            key: PrimaryKey::single(KeyValue::Int(key)),
            ..insert_into(channel)
        }
    }

    fn delete_from(channel: &str) -> Change {
        let mut old = HashMap::new();
        old.insert("channelId".to_string(), KeyValue::Text(channel.into()));
        Change {
            table: TableId::new("messages"),
            key: PrimaryKey::single(KeyValue::Int(1)),
            op: ChangeOp::Delete,
            new: None,
            old: Some(old),
        }
    }

    async fn count_sub(reactor: &InMemoryReactor, client: &str, channel: &str, initial: i64) {
        reactor
            .add_subscription(Subscription {
                client_id: client.into(),
                sub: format!("messages.count::{channel}"),
                path: vec!["messages".into(), "count".into()],
                input: json!({ "channelId": channel }),
                headers: HashMap::new(),
                read_set: count_filter(channel),
                last: Some(json!(initial)),
                last_lsn: Lsn::ZERO,
            })
            .await;
    }

    fn pushed_data(push: &SsePush) -> Value {
        serde_json::from_str::<Value>(&push.body).unwrap()["data"].clone()
    }

    fn cs_of(changes: Vec<Change>) -> ChangeSet {
        ChangeSet {
            commit_lsn: Lsn(1),
            changes,
        }
    }

    #[tokio::test]
    async fn ivm_count_increments_on_insert_without_reexec() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        count_sub(&reactor, "a", "A", 2).await;

        reactor
            .apply_change_set(cs_of(vec![insert_into("A")]))
            .await;

        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            0,
            "count is maintained from the delta — no worker re-exec"
        );
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(3));
    }

    #[tokio::test]
    async fn ivm_count_decrements_on_delete() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        count_sub(&reactor, "a", "A", 3).await;

        reactor
            .apply_change_set(cs_of(vec![delete_from("A")]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(2));
    }

    #[tokio::test]
    async fn ivm_count_multi_row_tx_is_one_push() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        count_sub(&reactor, "a", "A", 0).await;

        reactor
            .apply_change_set(cs_of(vec![
                insert_keyed("A", 1),
                insert_keyed("A", 2),
                insert_keyed("A", 3),
            ]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            pushed_data(&rx.try_recv().unwrap()),
            json!(3),
            "delta accumulated"
        );
        assert!(rx.try_recv().is_err(), "one push per ChangeSet");
    }

    #[tokio::test]
    async fn ivm_count_net_zero_change_suppresses_push() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        count_sub(&reactor, "a", "A", 5).await;

        // One row enters, one leaves → net 0 → identical value → no push.
        reactor
            .apply_change_set(cs_of(vec![insert_keyed("A", 9), delete_from("A")]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert!(
            rx.try_recv().is_err(),
            "unchanged count → no redundant push"
        );
    }

    // ── IVM: sum(integer field) ──────────────────────────────────────────────

    fn sum_filter(channel: &str) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "channelId".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Text(channel.into()),
                }],
                read_cols: Some(vec!["amount".into()]),
                aggregate: Some(pulse_core::Aggregate {
                    func: AggFunc::Sum,
                    field: Some("amount".into()),
                    distinct: false,
                }),
            },
        );
        rs
    }

    async fn sum_sub(reactor: &InMemoryReactor, client: &str, channel: &str, initial: f64) {
        reactor
            .add_subscription(Subscription {
                client_id: client.into(),
                sub: format!("messages.sum::{channel}"),
                path: vec!["messages".into(), "sum".into()],
                input: json!({ "channelId": channel }),
                headers: HashMap::new(),
                read_set: sum_filter(channel),
                last: Some(json!(initial)),
                last_lsn: Lsn::ZERO,
            })
            .await;
    }

    fn amount_row(channel: &str, amount: i64) -> RowValues {
        let mut m = RowValues::new();
        m.insert("channelId".to_string(), KeyValue::Text(channel.into()));
        m.insert("amount".to_string(), KeyValue::Int(amount));
        m
    }
    fn insert_amount(channel: &str, key: i64, amount: i64) -> Change {
        Change {
            table: TableId::new("messages"),
            key: PrimaryKey::single(KeyValue::Int(key)),
            op: ChangeOp::Insert,
            new: Some(amount_row(channel, amount)),
            old: None,
        }
    }
    fn delete_amount(channel: &str, amount: i64) -> Change {
        Change {
            table: TableId::new("messages"),
            key: PrimaryKey::single(KeyValue::Int(1)),
            op: ChangeOp::Delete,
            new: None,
            old: Some(amount_row(channel, amount)),
        }
    }
    fn update_amount(channel: &str, old_a: i64, new_a: i64) -> Change {
        Change {
            table: TableId::new("messages"),
            key: PrimaryKey::single(KeyValue::Int(1)),
            op: ChangeOp::Update,
            old: Some(amount_row(channel, old_a)),
            new: Some(amount_row(channel, new_a)),
        }
    }

    #[tokio::test]
    async fn ivm_sum_increments_on_insert() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        sum_sub(&reactor, "a", "A", 10.0).await;

        reactor
            .apply_change_set(cs_of(vec![insert_amount("A", 1, 5)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(15.0));
    }

    #[tokio::test]
    async fn ivm_sum_decrements_on_delete() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        sum_sub(&reactor, "a", "A", 20.0).await;

        reactor
            .apply_change_set(cs_of(vec![delete_amount("A", 8)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(12.0));
    }

    #[tokio::test]
    async fn ivm_sum_applies_delta_on_value_change() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        sum_sub(&reactor, "a", "A", 10.0).await;

        // Row stays in the filter, amount 3→8 → sum += 5.
        reactor
            .apply_change_set(cs_of(vec![update_amount("A", 3, 8)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(15.0));
    }

    #[tokio::test]
    async fn ivm_sum_falls_back_when_result_is_zero() {
        // A computed sum of 0 is ambiguous — the set may be empty (SQL `sum` → NULL)
        // or its values may cancel to 0 — so both cases must fall back to the re-exec
        // rather than push a `0` that should be `NULL`.
        // (a) Emptying delete: sum {8} − 8 → 0.
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        sum_sub(&reactor, "a", "A", 8.0).await;
        reactor
            .apply_change_set(cs_of(vec![delete_amount("A", 8)]))
            .await;
        assert_eq!(
            reexec.calls.load(Ordering::SeqCst),
            1,
            "emptying delete (sum→0) must fall back, not push 0"
        );

        // (b) Cancel-to-zero while non-empty: sum 5, a −5 row enters → 0.
        let reexec2 = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor2 = InMemoryReactor::new(reexec2.clone());
        let _rx2 = reactor2.register_client("a".into()).await;
        sum_sub(&reactor2, "a", "A", 5.0).await;
        reactor2
            .apply_change_set(cs_of(vec![insert_amount("A", 2, -5)]))
            .await;
        assert_eq!(
            reexec2.calls.load(Ordering::SeqCst),
            1,
            "cancel-to-zero must fall back to re-exec"
        );
    }

    #[tokio::test]
    async fn ivm_sum_falls_back_when_field_absent_from_image() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        sum_sub(&reactor, "a", "A", 10.0).await;

        // The change image carries no `amount` (e.g. a float column, never
        // captured) → the delta can't be computed → fall back to re-exec.
        reactor
            .apply_change_set(cs_of(vec![insert_into("A")]))
            .await;
        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
    }

    fn agg_filter(channel: &str, agg: pulse_core::Aggregate) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "channelId".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Text(channel.into()),
                }],
                read_cols: Some(vec![]),
                aggregate: Some(agg),
            },
        );
        rs
    }

    // ── IVM: max(integer field) / min(integer field) ─────────────────────────

    async fn extreme_sub(
        reactor: &InMemoryReactor,
        client: &str,
        channel: &str,
        func: AggFunc,
        initial: Value,
    ) {
        reactor
            .add_subscription(Subscription {
                client_id: client.into(),
                sub: format!("messages.ext::{channel}"),
                path: vec!["messages".into(), "ext".into()],
                input: json!({ "channelId": channel }),
                headers: HashMap::new(),
                // read_cols must include the aggregate field, or a value-only update
                // lowering the extreme would be column-pruned and never re-checked
                // (capture_reads adds the aggregate field for exactly this reason).
                read_set: {
                    let mut rs = ReadSet::new();
                    rs.add_filter(
                        TableId::new("messages"),
                        Filter {
                            conds: vec![Cond {
                                field: "channelId".into(),
                                op: FilterOp::Eq,
                                value: KeyValue::Text(channel.into()),
                            }],
                            read_cols: Some(vec!["amount".into()]),
                            aggregate: Some(pulse_core::Aggregate {
                                func,
                                field: Some("amount".into()),
                                distinct: false,
                            }),
                        },
                    );
                    rs
                },
                last: Some(initial),
                last_lsn: Lsn::ZERO,
            })
            .await;
    }

    #[tokio::test]
    async fn ivm_max_rises_on_insert() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // A new high-water mark enters → maintained with no re-exec.
        reactor
            .apply_change_set(cs_of(vec![insert_amount("A", 1, 15)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(15.0));
    }

    #[tokio::test]
    async fn ivm_max_unchanged_on_smaller_insert() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // A row below the max enters → max stays 10, no re-exec, push suppressed.
        reactor
            .apply_change_set(cs_of(vec![insert_amount("A", 1, 3)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert!(rx.try_recv().is_err(), "unchanged max → no redundant push");
    }

    #[tokio::test]
    async fn ivm_max_holds_when_non_extreme_row_leaves() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // A row well below the max leaves → the max still has a holder, no re-exec.
        reactor
            .apply_change_set(cs_of(vec![delete_amount("A", 4)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert!(rx.try_recv().is_err(), "max unchanged → no push");
    }

    #[tokio::test]
    async fn ivm_max_falls_back_when_holder_leaves() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // The row holding the max leaves and nothing reaches it → next-best unknown
        // → fall back to a re-exec (the only source of truth for the new max).
        reactor
            .apply_change_set(cs_of(vec![delete_amount("A", 10)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ivm_max_falls_back_when_holder_drops_below() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // The max holder stays in the filter but its value drops below the max → the
        // new max is some other row's value we don't have → fall back.
        reactor
            .apply_change_set(cs_of(vec![update_amount("A", 10, 4)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ivm_max_rises_on_value_change_without_reexec() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Max, json!(10.0)).await;

        // A non-extreme row grows past the current max → maintained, no re-exec.
        reactor
            .apply_change_set(cs_of(vec![update_amount("A", 3, 20)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(20.0));
    }

    #[tokio::test]
    async fn ivm_min_falls_on_insert() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Min, json!(10.0)).await;

        // A new low enters → min maintained with no re-exec.
        reactor
            .apply_change_set(cs_of(vec![insert_amount("A", 1, 5)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 0);
        assert_eq!(pushed_data(&rx.try_recv().unwrap()), json!(5.0));
    }

    #[tokio::test]
    async fn ivm_min_falls_back_when_holder_leaves() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        extreme_sub(&reactor, "a", "A", AggFunc::Min, json!(10.0)).await;

        // The row holding the min leaves and nothing reaches it → fall back.
        reactor
            .apply_change_set(cs_of(vec![delete_amount("A", 10)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ivm_extreme_falls_back_on_empty_set_null_last() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let _rx = reactor.register_client("a".into()).await;
        // An empty set's max is SQL NULL; a non-int `last` can't seed the comparison.
        extreme_sub(&reactor, "a", "A", AggFunc::Max, Value::Null).await;

        reactor
            .apply_change_set(cs_of(vec![insert_amount("A", 1, 5)]))
            .await;

        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ivm_falls_back_for_unmaintainable_shapes() {
        use pulse_core::Aggregate;
        // Shapes IVM cannot maintain from a delta alone must re-execute: avg (needs
        // auxiliary sum+count the initial query doesn't return), count(field) and
        // count(distinct) (need the dataset). count(*), sum, and min/max have their
        // own dedicated tests.
        let shapes = [
            Aggregate {
                func: AggFunc::Avg,
                field: Some("p".into()),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Count,
                field: Some("p".into()),
                distinct: false,
            }, // count(field)
            Aggregate {
                func: AggFunc::Count,
                field: None,
                distinct: true,
            }, // count(distinct)
        ];
        for agg in shapes {
            let reexec = Arc::new(CountingReExec {
                calls: AtomicUsize::new(0),
            });
            let reactor = InMemoryReactor::new(reexec.clone());
            let _rx = reactor.register_client("a".into()).await;
            reactor
                .add_subscription(Subscription {
                    client_id: "a".into(),
                    sub: "agg::A".into(),
                    path: vec!["w".into(), "agg".into()],
                    input: json!({}),
                    headers: HashMap::new(),
                    read_set: agg_filter("A", agg.clone()),
                    last: Some(json!(0)),
                    last_lsn: Lsn::ZERO,
                })
                .await;
            reactor
                .apply_change_set(cs_of(vec![insert_into("A")]))
                .await;
            assert_eq!(
                reexec.calls.load(Ordering::SeqCst),
                1,
                "{agg:?} must fall back to re-exec"
            );
        }
    }

    /// Benchmark: a reactive `count()` maintained incrementally (no worker round-trip)
    /// vs. the same invalidation re-executed through the worker. The re-exec uses a
    /// `WORKER_MS` stand-in for the real worker+SQL cost (single-digit ms in practice
    /// — see the WAL latency bench). Run:
    ///   cargo test -p pulse-reactor --release -- --ignored --nocapture bench_ivm
    #[tokio::test]
    #[ignore = "benchmark; run with --release --ignored --nocapture bench_ivm"]
    async fn bench_ivm_vs_reexec() {
        use std::time::Instant;
        // The headline metric is the number of worker re-execs each path triggers
        // (IVM does zero), not wall-clock: the tokio test clock interferes with a
        // simulated worker delay. Each avoided re-exec saves a worker+SQL round-trip
        // — single-digit ms in practice (see the WAL latency bench) — which dwarfs
        // the reactor-side µs reported here.
        const N: usize = 5000;

        // A re-executor that returns a fresh value (never diff-suppressed) AND the
        // same filter read-set, so a re-exec sub keeps matching every change.
        struct BenchReExec {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl ReExecutor for BenchReExec {
            async fn exec(
                &self,
                _p: Vec<String>,
                _i: Value,
                _h: HashMap<String, String>,
            ) -> Result<(Value, ReadSet), String> {
                let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok((json!({ "n": n }), channel_filter("A")))
            }
        }

        // IVM: a bare count() maintained from the change delta — no worker calls.
        let ivm_reexec = Arc::new(BenchReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(ivm_reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        count_sub(&reactor, "a", "A", 0).await;
        let t0 = Instant::now();
        for i in 0..N {
            reactor
                .apply_change_set(cs_of(vec![insert_keyed("A", i as i64)]))
                .await;
            let _ = rx.try_recv(); // drain so the SSE channel never fills
        }
        let ivm = t0.elapsed();

        // Re-exec: a plain list() over the same filter → one worker re-exec/change.
        let re_reexec = Arc::new(BenchReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor2 = InMemoryReactor::new(re_reexec.clone());
        let mut rx2 = reactor2.register_client("b".into()).await;
        sub(&reactor2, "b", "A").await;
        let t1 = Instant::now();
        for i in 0..N {
            reactor2
                .apply_change_set(cs_of(vec![insert_keyed("A", i as i64)]))
                .await;
            let _ = rx2.try_recv(); // drain so the SSE channel never fills
        }
        let re = t1.elapsed();

        println!(
            "\nIVM count:   {N} updates, {} worker re-execs, {:.2} µs/update (reactor only)\n\
             re-exec:     {N} updates, {} worker re-execs, {:.2} µs/update (reactor only)\n\
             → IVM eliminates the worker+SQL round-trip entirely (the dominant cost,\n\
               single-digit ms each per the WAL latency bench); reactor-side cost\n\
               above excludes it.\n",
            ivm_reexec.calls.load(Ordering::SeqCst),
            ivm.as_micros() as f64 / N as f64,
            re_reexec.calls.load(Ordering::SeqCst),
            re.as_micros() as f64 / N as f64,
        );
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

    /// The SSE buffer is a configurable knob: a tiny buffer fills fast but must not
    /// wedge other clients, and zero is bumped to a usable minimum.
    #[tokio::test]
    async fn sse_buffer_is_configurable() {
        let reactor = Arc::new(
            InMemoryReactor::new(Arc::new(UniqueReExec {
                n: AtomicUsize::new(0),
            }))
            .with_sse_buffer(0), // → bumped to 1
        );
        let _rx_slow = reactor.register_client("slow".into()).await;
        let r2 = reactor.clone();
        tokio::spawn(async move {
            for _ in 0..8 {
                r2.push("slow", "s", &json!({}), pulse_core::Lsn::ZERO)
                    .await; // fills the tiny buffer, then parks
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reactor.register_client("other".into()),
        )
        .await;
        assert!(ok.is_ok(), "a tiny SSE buffer must not wedge other clients");
    }

    // ── SSE resume (M5) ──────────────────────────────────────────────────────

    fn reactor_for_resume() -> InMemoryReactor {
        InMemoryReactor::new(Arc::new(UniqueReExec {
            n: AtomicUsize::new(0),
        }))
    }

    /// Slice 1: a reconnect with a Last-Event-ID replays the buffered events
    /// after it, in order, and nothing else.
    #[tokio::test]
    async fn resume_replays_events_after_last_event_id() {
        let reactor = reactor_for_resume();
        let _rx = reactor.register_client("a".into()).await;
        for i in 1..=3 {
            reactor.push("a", "s", &json!({ "n": i }), Lsn::ZERO).await;
        }
        let (mut rx, outcome) = reactor.register_client_resume("a".into(), Some(1)).await;
        assert_eq!(outcome, Resume::Resumed);
        assert_eq!(rx.try_recv().unwrap().id, 2);
        assert_eq!(rx.try_recv().unwrap().id, 3);
        assert!(
            rx.try_recv().is_err(),
            "only the missed events are replayed"
        );
    }

    /// Slice 2: the buffer is bounded by PULSE_SSE_BUFFER — a reconnect can
    /// resume exactly the newest `cap` events.
    #[tokio::test]
    async fn resume_buffer_is_bounded() {
        let reactor = reactor_for_resume().with_sse_buffer(4);
        let mut rx0 = reactor.register_client("a".into()).await;
        for i in 1..=10 {
            reactor.push("a", "s", &json!({ "n": i }), Lsn::ZERO).await;
            let _ = rx0.try_recv(); // drain live delivery so the channel never fills
        }
        // Only ids 7..=10 remain; resuming after 6 replays exactly those four.
        let (mut rx, outcome) = reactor.register_client_resume("a".into(), Some(6)).await;
        assert_eq!(outcome, Resume::Resumed);
        let ids: Vec<u64> = std::iter::from_fn(|| rx.try_recv().ok().map(|p| p.id)).collect();
        assert_eq!(ids, vec![7, 8, 9, 10]);
    }

    /// Slice 3: when the resume point rolled past the buffer, the client gets a
    /// `resync` frame, not a partial/silent replay.
    #[tokio::test]
    async fn resume_past_the_buffer_requests_resync() {
        let reactor = reactor_for_resume().with_sse_buffer(4);
        let mut rx0 = reactor.register_client("a".into()).await;
        for i in 1..=10 {
            reactor.push("a", "s", &json!({ "n": i }), Lsn::ZERO).await;
            let _ = rx0.try_recv(); // drain live delivery so the channel never fills
        }
        // id 5 was evicted (only 7..=10 remain) → the gap can't be replayed.
        let (mut rx, outcome) = reactor.register_client_resume("a".into(), Some(5)).await;
        assert_eq!(outcome, Resume::Resync);
        let frame = rx.try_recv().unwrap();
        assert!(frame.body.contains("\"type\":\"resync\""));
        assert!(rx.try_recv().is_err(), "only the resync control frame");
    }

    /// Slice 4: replayed events precede live ones with no duplicate or gap.
    #[tokio::test]
    async fn resume_replay_precedes_live_pushes() {
        let reactor = reactor_for_resume();
        let _rx = reactor.register_client("a".into()).await;
        for i in 1..=5 {
            reactor.push("a", "s", &json!({ "n": i }), Lsn::ZERO).await;
        }
        let (mut rx, outcome) = reactor.register_client_resume("a".into(), Some(3)).await;
        assert_eq!(outcome, Resume::Resumed);
        // A live push after the resume continues the same monotonic stream.
        reactor.push("a", "s", &json!({ "n": 6 }), Lsn::ZERO).await;
        let ids: Vec<u64> = std::iter::from_fn(|| rx.try_recv().ok().map(|p| p.id)).collect();
        assert_eq!(
            ids,
            vec![4, 5, 6],
            "replay 4,5 then live 6 — no dup, no gap"
        );
    }

    /// Slice 5: an unknown client presenting a resume point (server restart, or a
    /// stale id) degrades to `resync`.
    #[tokio::test]
    async fn resume_unknown_client_requests_resync() {
        let reactor = reactor_for_resume();
        let (mut rx, outcome) = reactor
            .register_client_resume("never-seen".into(), Some(5))
            .await;
        assert_eq!(outcome, Resume::Resync);
        assert!(rx.try_recv().unwrap().body.contains("resync"));
    }

    /// No Last-Event-ID is a fresh stream (the existing connect path).
    #[tokio::test]
    async fn resume_without_last_event_id_is_fresh() {
        let reactor = reactor_for_resume();
        let (_rx, outcome) = reactor.register_client_resume("a".into(), None).await;
        assert_eq!(outcome, Resume::Fresh);
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
                Lsn::ZERO,
            )
            .await;

        assert!(recorded.is_none(), "recording for an unknown sub is None");
    }

    /// Records every set of tables announced to the interest sink.
    #[derive(Default)]
    struct RecordingSink {
        announced: Mutex<Vec<Vec<String>>>,
    }
    #[async_trait]
    impl InterestSink for RecordingSink {
        async fn on_tables_added(&self, tables: Vec<TableId>) {
            self.announced
                .lock()
                .await
                .push(tables.into_iter().map(|t| t.0).collect());
        }
    }

    /// Subscribing announces interest in a table the node wasn't watching; a second
    /// sub on the SAME table does not re-announce (the bucket was already non-empty).
    #[tokio::test]
    async fn new_table_announces_interest_once() {
        let sink = Arc::new(RecordingSink::default());
        let reactor = InMemoryReactor::new(Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        }))
        .with_interest(sink.clone());

        reactor.register_client("a".into()).await;
        reactor.register_client("b".into()).await;
        sub(&reactor, "a", "A").await; // first sub on `messages` → announce
        sub(&reactor, "b", "B").await; // another `messages` sub → no new announce

        let announced = sink.announced.lock().await.clone();
        assert_eq!(announced, vec![vec!["messages".to_string()]]);
        assert_eq!(
            reactor.interest_tables().await,
            vec![TableId::new("messages")]
        );
    }

    /// `refresh_subscription` re-execs one sub but suppresses the push when the
    /// value is unchanged (the catch-up no-op case).
    #[tokio::test]
    async fn refresh_subscription_is_a_noop_when_unchanged() {
        let reexec = Arc::new(CountingReExec {
            calls: AtomicUsize::new(0),
        });
        let reactor = InMemoryReactor::new(reexec.clone());
        let mut rx = reactor.register_client("a".into()).await;
        // Seed `last` to the value CountingReExec will return on its first call so
        // the refresh recomputes an identical value and suppresses the push.
        reactor
            .add_subscription(Subscription {
                client_id: "a".into(),
                sub: "messages.list::A".into(),
                path: vec!["messages".into(), "list".into()],
                input: json!({ "channelId": "A" }),
                headers: HashMap::new(),
                read_set: channel_filter("A"),
                last: Some(json!({ "n": 1 })),
                last_lsn: Lsn::ZERO,
            })
            .await;

        reactor.refresh_subscription("a", "messages.list::A").await;
        assert_eq!(reexec.calls.load(Ordering::SeqCst), 1, "re-exec ran once");
        assert!(rx.try_recv().is_err(), "unchanged value → no catch-up push");
    }

    /// A re-executor that returns a fresh value each call (so every push goes
    /// through) AND keeps the subscription's read-set stable (so it keeps matching
    /// across multiple applies — unlike `CountingReExec`, whose empty read-set
    /// deindexes the sub after the first run).
    struct StableUniqueReExec {
        n: AtomicUsize,
        read_set: ReadSet,
    }
    #[async_trait]
    impl ReExecutor for StableUniqueReExec {
        async fn exec(
            &self,
            _path: Vec<String>,
            _input: Value,
            _headers: HashMap<String, String>,
        ) -> Result<(Value, ReadSet), String> {
            Ok((
                json!({ "n": self.n.fetch_add(1, Ordering::SeqCst) }),
                self.read_set.clone(),
            ))
        }
    }

    fn commit_lsn_of(push: &SsePush) -> Lsn {
        let v: Value = serde_json::from_str(&push.body).unwrap();
        v["commitLsn"].as_str().unwrap().parse().unwrap()
    }

    fn change_at(channel: &str, lsn: u64) -> ChangeSet {
        ChangeSet {
            commit_lsn: Lsn(lsn),
            changes: vec![insert_into(channel)],
        }
    }

    /// The emitted `commitLsn` must never regress for a subscription, even when an
    /// invalidation arrives with a LOWER commit LSN (out-of-order delivery).
    #[tokio::test]
    async fn commit_lsn_watermark_never_regresses() {
        let reactor = InMemoryReactor::new(Arc::new(StableUniqueReExec {
            n: AtomicUsize::new(0),
            read_set: channel_filter("A"),
        }));
        let mut rx = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await;

        reactor.apply_change_set(change_at("A", 5)).await;
        reactor.apply_change_set(change_at("A", 3)).await; // lower LSN, out of order

        let first = commit_lsn_of(&rx.try_recv().unwrap());
        let second = commit_lsn_of(&rx.try_recv().unwrap());
        assert_eq!(first, Lsn(5));
        assert_eq!(
            second,
            Lsn(5),
            "watermark clamps up; never drops to the lower LSN"
        );
        assert!(second >= first);
    }

    /// With increasing commit LSNs the watermark follows them upward.
    #[tokio::test]
    async fn commit_lsn_watermark_follows_increasing_lsn() {
        let reactor = InMemoryReactor::new(Arc::new(StableUniqueReExec {
            n: AtomicUsize::new(0),
            read_set: channel_filter("A"),
        }));
        let mut rx = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await;

        reactor.apply_change_set(change_at("A", 3)).await;
        reactor.apply_change_set(change_at("A", 7)).await;

        assert_eq!(commit_lsn_of(&rx.try_recv().unwrap()), Lsn(3));
        assert_eq!(commit_lsn_of(&rx.try_recv().unwrap()), Lsn(7));
    }

    /// A coarse resync (stamped LSN zero) must not regress the watermark: the push
    /// still carries the highest LSN already emitted, not 0.
    #[tokio::test]
    async fn resync_does_not_regress_watermark() {
        let reactor = InMemoryReactor::new(Arc::new(StableUniqueReExec {
            n: AtomicUsize::new(0),
            read_set: channel_filter("A"),
        }));
        let mut rx = reactor.register_client("a".into()).await;
        sub(&reactor, "a", "A").await;

        reactor.apply_change_set(change_at("A", 9)).await;
        reactor.invalidate_all().await; // recovery path → Lsn::ZERO

        assert_eq!(commit_lsn_of(&rx.try_recv().unwrap()), Lsn(9));
        assert_eq!(
            commit_lsn_of(&rx.try_recv().unwrap()),
            Lsn(9),
            "resync keeps the watermark at 9, does not reset to 0"
        );
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
                last_lsn: Lsn::ZERO,
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
                    last_lsn: Lsn::ZERO,
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

    // ── Property test: `try_ivm` is never wrong ──────────────────────────────
    // For a random initial set and a random change sequence, whenever `try_ivm`
    // returns a value it must equal a brute-force recompute of the aggregate over
    // the rows satisfying the filter (channelId == "A"). `try_ivm` may return None
    // (fall back — the re-exec is the source of truth), but it must NEVER return a
    // wrong value. This is the core IVM correctness contract.
    mod props {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        type World = BTreeMap<u8, (String, i64)>;

        fn prow(ch: &str, amt: i64) -> RowValues {
            let mut m = RowValues::new();
            m.insert("channelId".to_string(), KeyValue::Text(ch.to_string()));
            m.insert("amount".to_string(), KeyValue::Int(amt));
            m
        }

        fn psub(func: AggFunc, field: Option<&str>, last: Value) -> Subscription {
            let mut rs = ReadSet::new();
            rs.add_filter(
                TableId::new("messages"),
                Filter {
                    conds: vec![Cond {
                        field: "channelId".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Text("A".into()),
                    }],
                    read_cols: Some(vec!["amount".into()]),
                    aggregate: Some(pulse_core::Aggregate {
                        func,
                        field: field.map(|s| s.to_string()),
                        distinct: false,
                    }),
                },
            );
            Subscription {
                client_id: "p".into(),
                sub: "s".into(),
                path: vec![],
                input: json!({}),
                headers: HashMap::new(),
                read_set: rs,
                last: Some(last),
                last_lsn: Lsn::ZERO,
            }
        }

        // Replay ops onto `initial`, returning (changes, post-world). An op is
        // (key, Some((channel, amount))) = upsert, or (key, None) = delete. Old/new
        // images are derived from the simulated world so the changeset is valid.
        fn replay(initial: &World, ops: &[(u8, Option<(String, i64)>)]) -> (Vec<Change>, World) {
            let mut world = initial.clone();
            let mut changes = Vec::new();
            for (k, act) in ops {
                let before = world.get(k).cloned();
                match act {
                    Some((ch, amt)) => {
                        world.insert(*k, (ch.clone(), *amt));
                        changes.push(Change {
                            table: TableId::new("messages"),
                            key: PrimaryKey::single(KeyValue::Int(*k as i64)),
                            op: if before.is_some() {
                                ChangeOp::Update
                            } else {
                                ChangeOp::Insert
                            },
                            old: before.as_ref().map(|(c, a)| prow(c, *a)),
                            new: Some(prow(ch, *amt)),
                        });
                    }
                    None => {
                        if let Some((c, a)) = before {
                            world.remove(k);
                            changes.push(Change {
                                table: TableId::new("messages"),
                                key: PrimaryKey::single(KeyValue::Int(*k as i64)),
                                op: ChangeOp::Delete,
                                old: Some(prow(&c, a)),
                                new: None,
                            });
                        }
                    }
                }
            }
            (changes, world)
        }

        fn amounts_in_a(world: &World) -> Vec<i64> {
            world
                .values()
                .filter(|(c, _)| c == "A")
                .map(|(_, a)| *a)
                .collect()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]
            #[test]
            fn try_ivm_is_never_wrong(
                initial in prop::collection::btree_map(0u8..8, ("A|B", -6i64..12), 0..6),
                ops in prop::collection::vec((0u8..8, prop::option::of(("A|B", -6i64..12))), 0..10),
            ) {
                let (changes, post) = replay(&initial, &ops);
                let cs = cs_of(changes);
                let pre = amounts_in_a(&initial);
                let post_a = amounts_in_a(&post);

                // count(*): always maintainable; an empty set is 0 (not NULL).
                {
                    let sub = psub(AggFunc::Count, None, json!(pre.len() as i64));
                    if let Some(v) = try_ivm(&sub, &cs) {
                        prop_assert_eq!(v, json!(post_a.len() as i64), "count");
                    }
                }
                // sum / min / max only seed a numeric `last` when the pre-set is
                // non-empty (an empty aggregate is SQL NULL → `last` is null → falls
                // back before any delta math).
                if !pre.is_empty() {
                    let truth_sum = if post_a.is_empty() {
                        Value::Null
                    } else {
                        json!(post_a.iter().sum::<i64>() as f64)
                    };
                    let sub = psub(AggFunc::Sum, Some("amount"), json!(pre.iter().sum::<i64>() as f64));
                    if let Some(v) = try_ivm(&sub, &cs) {
                        prop_assert_eq!(v, truth_sum, "sum");
                    }

                    for (func, is_max) in [(AggFunc::Max, true), (AggFunc::Min, false)] {
                        let seed = if is_max {
                            *pre.iter().max().unwrap()
                        } else {
                            *pre.iter().min().unwrap()
                        };
                        let truth = if post_a.is_empty() {
                            Value::Null
                        } else {
                            let e = if is_max {
                                *post_a.iter().max().unwrap()
                            } else {
                                *post_a.iter().min().unwrap()
                            };
                            json!(e as f64)
                        };
                        let sub = psub(func, Some("amount"), json!(seed as f64));
                        if let Some(v) = try_ivm(&sub, &cs) {
                            prop_assert_eq!(v, truth, "min/max");
                        }
                    }
                }
            }
        }
    }
}
