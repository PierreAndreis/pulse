//! End-to-end tests for the `Worker` driver against a real ephemeral Postgres and
//! a stub worker process (`tests/support/stub_worker.rs`) that speaks the NDJSON
//! protocol — no Node/Bun required. These exercise the parts of `lib.rs` that pure
//! unit tests can't reach: process spawn + manifest handshake, the reader loop's op
//! routing, autocommit reads, transactional mutations, idempotent dedupe, and
//! rollback on handler error. Each test skips if Docker is unavailable.

mod common;

use std::collections::HashMap;

use pulse_core::ProcedureKind;
use pulse_jsruntime::{Worker, WorkerConfig};
use serde_json::{json, Value};
use sqlx::PgPool;

/// Path to the stub worker binary, built by Cargo as a `[[bin]]` of this crate.
const STUB: &str = env!("CARGO_BIN_EXE_stub_worker");

/// Spawn a worker backed by the stub process on the given pool (used for both the
/// OLTP and OLAP paths).
async fn spawn_with(pool: &PgPool) -> Worker {
    let config = WorkerConfig {
        bin: STUB.to_string(),
        script: "unused".to_string(),
        app: "unused".to_string(),
        pool: pool.clone(),
        olap_pool: pool.clone(),
        self_url: "http://127.0.0.1:0".to_string(),
    };
    Worker::spawn(config).await.expect("spawn stub worker")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_loads_manifest_and_finds_procedures() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    assert_eq!(worker.procedures().len(), 5);
    let write = worker
        .find(&["write".to_string()])
        .expect("write proc exists");
    assert_eq!(write.kind, ProcedureKind::Mutation);
    let read = worker
        .find(&["read".to_string()])
        .expect("read proc exists");
    assert_eq!(read.kind, ProcedureKind::Reactive);
    assert!(worker.find(&["nope".to_string()]).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_fails_for_missing_binary() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let config = WorkerConfig {
        bin: "pulse-no-such-binary-xyz".to_string(),
        script: "x".to_string(),
        app: "x".to_string(),
        pool: pool.clone(),
        olap_pool: pool.clone(),
        self_url: "http://127.0.0.1:0".to_string(),
    };
    let err = match Worker::spawn(config).await {
        Err(e) => e,
        Ok(_) => panic!("missing binary must fail"),
    };
    assert!(
        matches!(err, pulse_jsruntime::RuntimeError::Spawn(..)),
        "expected Spawn error, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_handler_returns_input_with_no_db_access() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    let input = json!({"hi": 1, "nested": [true]});
    let res = worker
        .execute(
            vec!["echo".to_string()],
            input.clone(),
            HashMap::new(),
            None,
        )
        .await
        .expect("echo ok");
    assert_eq!(res.value, input);
    // No db op ran, so nothing was read or changed.
    assert!(res.read_set.referenced_tables().is_empty());
    assert!(res.changes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_mutation_inserts_and_captures_change() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    let name = "write-mutation-row";
    assert_eq!(common::items_named(&pool, name).await, 0);

    let res = worker
        .execute(
            vec!["write".to_string()],
            json!({"name": name}),
            HashMap::new(),
            Some("write-mutation-id".to_string()),
        )
        .await
        .expect("write ok");

    // The insert committed: exactly one row, and the op produced one Change.
    assert_eq!(common::items_named(&pool, name).await, 1);
    assert_eq!(res.changes.len(), 1);
    // The handler's result is the new row id string.
    assert!(
        res.value.is_string(),
        "expected id string, got {}",
        res.value
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_with_same_id_is_deduped() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    let name = "dedupe-row";
    let id = "dedupe-mutation-id".to_string();

    let first = worker
        .execute(
            vec!["write".to_string()],
            json!({"name": name}),
            HashMap::new(),
            Some(id.clone()),
        )
        .await
        .expect("first ok");
    let second = worker
        .execute(
            vec!["write".to_string()],
            json!({"name": name}),
            HashMap::new(),
            Some(id.clone()),
        )
        .await
        .expect("second ok");

    // The second delivery hit the exactly-once fast path: the recorded result is
    // returned and the handler did not run again, so only one row exists.
    assert_eq!(common::items_named(&pool, name).await, 1);
    assert_eq!(first.value, second.value);
    assert!(second.changes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_error_rolls_back_the_transaction() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    let name = "rollback-row";
    assert_eq!(common::items_named(&pool, name).await, 0);

    let err = match worker
        .execute(
            vec!["fail".to_string()],
            json!({"name": name}),
            HashMap::new(),
            Some("rollback-mutation-id".to_string()),
        )
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("handler error must surface"),
    };
    assert_eq!(err.code, "BAD_INPUT");

    // The insert ran inside the tx but the handler failed, so it was rolled back.
    assert_eq!(common::items_named(&pool, name).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reactive_read_captures_read_set() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    // Ensure at least one row exists so the collect returns data.
    worker
        .execute(
            vec!["write".to_string()],
            json!({"name": "read-seed"}),
            HashMap::new(),
            Some("read-seed-id".to_string()),
        )
        .await
        .expect("seed write ok");

    let res = worker
        .execute(vec!["read".to_string()], Value::Null, HashMap::new(), None)
        .await
        .expect("read ok");

    // A collect returns the rows as an array, and the query's read was captured
    // so the reactor can invalidate this subscription on `items` changes.
    assert!(
        res.value.is_array(),
        "expected rows array, got {}",
        res.value
    );
    assert!(!res.read_set.referenced_tables().is_empty());
    assert!(res.changes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analytical_read_runs_on_olap_pool() {
    let Some(pool) = common::pool().await else {
        return;
    };
    let worker = spawn_with(&pool).await;
    // The `count` proc is Analytical, so its op routes through the OLAP pool path.
    let res = worker
        .execute(vec!["count".to_string()], Value::Null, HashMap::new(), None)
        .await
        .expect("analytical ok");
    assert!(res.value.is_array());
}
