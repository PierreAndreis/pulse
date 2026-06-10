//! Coordinator orchestration, exercised entirely in-memory: a real
//! `InMemoryReactor` plus fakes for the three host seams (Executor, Publisher,
//! InterestRegistry) and the reactor's own `ReExecutor`. No Postgres, no worker.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use pulse_core::{
    Change, ChangeOp, ChangeSet, Cond, Filter, FilterOp, KeyValue, Lsn, PrimaryKey, ReadSet,
    TableId,
};
use pulse_reactor::{InMemoryReactor, ReExecutor, Subscription};

use super::*;

// ---- fakes for the three host seams ---------------------------------------

/// Scripted procedure runtime. `known` drives `contains`; the run returns the
/// configured value/read-set/changes (or a configured error).
struct FakeExecutor {
    known: HashSet<Vec<String>>,
    value: Value,
    read_set: ReadSet,
    changes: Vec<Change>,
    err: Option<ExecError>,
    calls: AtomicU64,
}

impl FakeExecutor {
    fn reads(path: &[&str], value: Value, read_set: ReadSet) -> Self {
        FakeExecutor {
            known: [path.iter().map(|s| s.to_string()).collect()].into(),
            value,
            read_set,
            changes: Vec::new(),
            err: None,
            calls: AtomicU64::new(0),
        }
    }
    fn writes(path: &[&str], value: Value, changes: Vec<Change>) -> Self {
        FakeExecutor {
            known: [path.iter().map(|s| s.to_string()).collect()].into(),
            value,
            read_set: ReadSet::new(),
            changes,
            err: None,
            calls: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    fn contains(&self, path: &[String]) -> bool {
        self.known.contains(path)
    }
    async fn execute(
        &self,
        _path: Vec<String>,
        _input: Value,
        _headers: HashMap<String, String>,
        _mutation_id: Option<String>,
    ) -> Result<ExecOutcome, ExecError> {
        self.calls.fetch_add(1, SeqCst);
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        Ok(ExecOutcome {
            value: self.value.clone(),
            read_set: self.read_set.clone(),
            changes: self.changes.clone(),
        })
    }
}

#[derive(Default)]
struct RecordingPublisher {
    published: Mutex<Vec<ChangeSet>>,
}
#[async_trait]
impl Publisher for RecordingPublisher {
    async fn publish(&self, change_set: &ChangeSet) {
        self.published.lock().unwrap().push(change_set.clone());
    }
}

#[derive(Default)]
struct FakeInterest {
    refreshed: Mutex<Vec<Vec<TableId>>>,
    pruned: AtomicU64,
}
#[async_trait]
impl InterestRegistry for FakeInterest {
    async fn refresh(&self, tables: Vec<TableId>) {
        self.refreshed.lock().unwrap().push(tables);
    }
    async fn prune(&self) {
        self.pruned.fetch_add(1, SeqCst);
    }
}

/// A reactor re-executor that returns a fresh value each call so a matched
/// subscription always pushes (the diff never suppresses).
#[derive(Default)]
struct FreshReExec {
    calls: AtomicU64,
}
#[async_trait]
impl ReExecutor for FreshReExec {
    async fn exec(
        &self,
        _path: Vec<String>,
        _input: Value,
        _headers: HashMap<String, String>,
    ) -> Result<(Value, ReadSet), String> {
        let n = self.calls.fetch_add(1, SeqCst) + 1;
        Ok((json!({ "n": n }), channel_filter("c1")))
    }
}

// ---- test data ------------------------------------------------------------

fn channel_filter(channel: &str) -> ReadSet {
    let mut rs = ReadSet::new();
    rs.add_filter(
        TableId::new("messages"),
        Filter {
            conds: vec![Cond {
                field: "channelId".into(),
                op: FilterOp::Eq,
                value: KeyValue::Text(channel.into()),
            }],
            read_cols: None,
            aggregate: None,
        },
    );
    rs
}

fn insert_into(channel: &str) -> Change {
    insert_into_key(channel, 1)
}

fn insert_into_key(channel: &str, key: i64) -> Change {
    let mut new = HashMap::new();
    new.insert("channelId".to_string(), KeyValue::Text(channel.into()));
    Change {
        table: TableId::new("messages"),
        key: PrimaryKey::single(KeyValue::Int(key)),
        op: ChangeOp::Insert,
        new: Some(new),
        old: None,
    }
}

struct Rig {
    coord: Coordinator,
    reactor: Arc<dyn Reactor>,
    publisher: Arc<RecordingPublisher>,
    interest: Arc<FakeInterest>,
    wal: Arc<AtomicU64>,
}

fn rig(executor: Arc<dyn Executor>) -> Rig {
    let reactor: Arc<dyn Reactor> =
        Arc::new(InMemoryReactor::new(Arc::new(FreshReExec::default())));
    let publisher = Arc::new(RecordingPublisher::default());
    let interest = Arc::new(FakeInterest::default());
    let wal = Arc::new(AtomicU64::new(0));
    let coord = Coordinator::new(
        reactor.clone(),
        executor,
        publisher.clone(),
        interest.clone(),
        wal.clone(),
    );
    Rig {
        coord,
        reactor,
        publisher,
        interest,
        wal,
    }
}

async fn recv(rx: &mut mpsc::Receiver<SsePush>) -> Option<SsePush> {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .ok()
        .flatten()
}

// ---- tests ----------------------------------------------------------------

#[tokio::test]
async fn mutation_propagation_stamps_lsn_publishes_and_applies_locally() {
    let exec = Arc::new(FakeExecutor::writes(
        &["messages", "send"],
        json!({ "ok": true }),
        vec![insert_into("c1")],
    ));
    let r = rig(exec);
    r.wal.store(4242, SeqCst);

    // A subscriber on channel c1 so the local apply has someone to push to.
    let mut rx = r.reactor.register_client("a".into()).await;
    r.reactor
        .add_subscription(Subscription {
            client_id: "a".into(),
            sub: "messages.list::c1".into(),
            path: vec!["messages".into(), "list".into()],
            input: json!({ "channelId": "c1" }),
            headers: HashMap::new(),
            read_set: channel_filter("c1"),
            last: Some(json!({ "n": 0 })),
            last_lsn: Lsn::ZERO,
        })
        .await;

    let ok = r
        .coord
        .handle_rpc(
            vec!["messages".into(), "send".into()],
            json!({ "channelId": "c1", "body": "hi" }),
            HashMap::new(),
            None,
        )
        .await
        .expect("rpc ok");

    assert_eq!(ok.value, json!({ "ok": true }));
    let cs = ok.propagate.expect("a write must propagate");
    assert_eq!(
        cs.commit_lsn,
        Lsn(4242),
        "stamped from the sampled WAL position"
    );

    r.coord.propagate(cs).await;

    // Published to the bus exactly once.
    assert_eq!(r.publisher.published.lock().unwrap().len(), 1);
    // Local subscriber on c1 got a push (the parallel local apply ran).
    assert!(
        recv(&mut rx).await.is_some(),
        "matched subscriber pushed locally"
    );
}

#[tokio::test]
async fn mode_b_dedups_wal_echo_but_applies_out_of_band_writes() {
    // A subscriber on c1; FreshReExec pushes on every matching apply.
    let exec = Arc::new(FakeExecutor::reads(
        &["messages", "list"],
        json!([]),
        channel_filter("c1"),
    ));
    let r = rig(exec);
    let mut rx = r.coord.register_client("a".into()).await;
    r.reactor
        .add_subscription(Subscription {
            client_id: "a".into(),
            sub: "messages.list::c1".into(),
            path: vec!["messages".into(), "list".into()],
            input: json!({ "channelId": "c1" }),
            headers: HashMap::new(),
            read_set: channel_filter("c1"),
            last: Some(json!({ "n": 0 })),
            last_lsn: Lsn::ZERO,
        })
        .await;

    // In-engine write: applied synchronously (subscriber pushed once) and
    // recorded in the dedup window.
    let change = insert_into_key("c1", 1);
    r.coord.propagate(stamp(vec![change.clone()])).await;
    assert!(
        recv(&mut rx).await.is_some(),
        "in-engine write pushes locally"
    );

    // The WAL echo of that same commit must be dropped — no second push.
    r.coord
        .apply_wal(stamp(vec![change.clone()]), Some(0))
        .await;
    assert!(
        recv(&mut rx).await.is_none(),
        "WAL echo of an in-engine write is deduped (Mode B)"
    );
    assert_eq!(r.coord.metrics().deduped.load(SeqCst), 1);

    // A genuine out-of-band write (a different row, same channel) is NOT in the
    // dedup window → it invalidates the subscription like any other change.
    r.coord
        .apply_wal(stamp(vec![insert_into_key("c1", 2)]), Some(0))
        .await;
    assert!(
        recv(&mut rx).await.is_some(),
        "out-of-band WAL write re-runs the matching subscription"
    );
    assert_eq!(r.coord.metrics().deduped.load(SeqCst), 1, "no extra dedup");
}

#[tokio::test]
async fn read_with_no_changes_does_not_propagate() {
    let exec = Arc::new(FakeExecutor::reads(
        &["messages", "list"],
        json!([{ "body": "hi" }]),
        channel_filter("c1"),
    ));
    let r = rig(exec);

    let ok = r
        .coord
        .handle_rpc(
            vec!["messages".into(), "list".into()],
            json!({ "channelId": "c1" }),
            HashMap::new(),
            None,
        )
        .await
        .expect("rpc ok");

    assert!(ok.propagate.is_none(), "a read never propagates");
    assert!(r.publisher.published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unknown_procedure_is_not_found() {
    let exec = Arc::new(FakeExecutor::reads(
        &["messages", "list"],
        json!(null),
        ReadSet::new(),
    ));
    let r = rig(exec.clone());

    let err = r
        .coord
        .handle_rpc(vec!["nope".into()], json!(null), HashMap::new(), None)
        .await
        .expect_err("missing procedure");

    assert_eq!(err.code, "NOT_FOUND");
    assert_eq!(
        exec.calls.load(SeqCst),
        0,
        "must not execute a missing procedure"
    );
}

#[tokio::test]
async fn subscribe_runs_adds_and_pushes_initial_value() {
    let exec = Arc::new(FakeExecutor::reads(
        &["messages", "list"],
        json!([{ "body": "first" }]),
        channel_filter("c1"),
    ));
    let r = rig(exec);

    let mut rx = r.coord.register_client("a".into()).await;
    r.coord
        .handle_subscribe(
            "a".into(),
            "messages.list::c1".into(),
            vec!["messages".into(), "list".into()],
            json!({ "channelId": "c1" }),
            HashMap::new(),
        )
        .await
        .expect("subscribe ok");

    // The initial push carries the query's first result.
    let push = recv(&mut rx).await.expect("initial push");
    assert!(push.body.contains("first"));

    // The subscription is now registered: a matching change re-pushes it.
    r.coord
        .apply_invalidation(
            Invalidation::Changes(stamp(vec![insert_into("c1")])),
            Some(0),
        )
        .await;
    assert!(
        recv(&mut rx).await.is_some(),
        "registered sub re-pushed on a matching change"
    );
}

#[tokio::test]
async fn apply_invalidation_routes_each_variant_and_counts() {
    let exec = Arc::new(FakeExecutor::reads(&["x"], json!(null), ReadSet::new()));
    let r = rig(exec);
    let m = r.coord.metrics();

    r.coord
        .apply_invalidation(
            Invalidation::Changes(stamp(vec![insert_into("c1")])),
            Some(5),
        )
        .await;
    r.coord
        .apply_invalidation(
            Invalidation::Tables([TableId::new("messages")].into()),
            None,
        )
        .await;
    r.coord.apply_invalidation(Invalidation::All, None).await;

    assert_eq!(m.events.load(SeqCst), 3, "every event counted");
    assert_eq!(
        m.changes.load(SeqCst),
        1,
        "only the precise change-set is a lag sample"
    );
    assert_eq!(m.lag_us_sum.load(SeqCst), 5);
    assert_eq!(m.resyncs.load(SeqCst), 2, "Tables + All are resyncs");
}

#[tokio::test]
async fn heartbeat_refreshes_watched_tables_and_prunes() {
    let exec = Arc::new(FakeExecutor::reads(&["x"], json!(null), ReadSet::new()));
    let r = rig(exec);

    // Watch a table so interest_tables() is non-empty.
    r.reactor
        .add_subscription(Subscription {
            client_id: "a".into(),
            sub: "s".into(),
            path: vec!["messages".into(), "list".into()],
            input: json!({ "channelId": "c1" }),
            headers: HashMap::new(),
            read_set: channel_filter("c1"),
            last: None,
            last_lsn: Lsn::ZERO,
        })
        .await;

    r.coord.on_heartbeat().await;

    let refreshed = r.interest.refreshed.lock().unwrap();
    assert_eq!(refreshed.len(), 1);
    assert_eq!(refreshed[0], vec![TableId::new("messages")]);
    assert_eq!(r.interest.pruned.load(SeqCst), 1);
}

#[test]
fn clamp_heartbeat_keeps_beat_inside_ttl() {
    assert_eq!(
        clamp_heartbeat(30, 10_000),
        10_000,
        "default beat is already < ttl/3"
    );
    assert_eq!(
        clamp_heartbeat(30, 20_000),
        10_000,
        "over ttl/3 → clamped to ttl/3"
    );
    assert_eq!(clamp_heartbeat(3, 5_000), 1_000);
    assert_eq!(clamp_heartbeat(0, 5_000), 333, "ttl floored at 1s → 333ms");
}

fn stamp(changes: Vec<Change>) -> ChangeSet {
    ChangeSet {
        commit_lsn: Lsn(1),
        changes,
    }
}
