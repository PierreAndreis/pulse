//! Execution of user TS query/mutation handlers.
//!
//! Implementation note (deviation from the spec's target): to reach the M2
//! reactive slice fastest, M1 drives handlers in a Node/Bun worker managed by
//! the engine. The worker's instrumented `ctx.db` proxies each operation back
//! over NDJSON; the engine executes it via `pulse-sql` (so Rust remains the
//! query engine and the read-set capture point). The spec's target — an embedded
//! V8 isolate pool (`deno_core`) with a deterministic sandbox — is the M4 upgrade
//! kept behind this crate's interface.

mod protocol;

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::timeout;
use uuid::Uuid;

use pulse_core::{Change, ProcedureKind, ReadSet};
use pulse_sql::{capture_reads, execute_op, execute_op_pool, introspect, Catalog, PgPool};

use protocol::{EngineMsg, WorkerOut};
pub use protocol::{ProcInfo, WorkerError};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("failed to spawn worker `{0}`: {1}")]
    Spawn(String, std::io::Error),
    #[error("worker did not send its manifest within the timeout")]
    ManifestTimeout,
    #[error("database introspection failed: {0}")]
    Introspect(#[from] sqlx::Error),
    #[error("io error talking to worker: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for launching the worker process.
pub struct WorkerConfig {
    /// Executable that runs the worker (e.g. `bun` or `node`).
    pub bin: String,
    /// The worker script (e.g. `packages/runtime-node/src/worker.ts`).
    pub script: String,
    /// The user app module the worker loads (schema + handlers).
    pub app: String,
    /// OLTP pool: mutations + reactive reads (low statement timeout, serializable).
    pub pool: PgPool,
    /// OLAP pool: analytical reads (separate budget + longer statement timeout),
    /// so a heavy analytical query can't starve the reactive hot path.
    pub olap_pool: PgPool,
    /// The engine's own base URL (e.g. `http://127.0.0.1:8787`), passed to the
    /// worker so actions can re-enter the engine via `/rpc` (runQuery/runMutation).
    pub self_url: String,
}

/// What one procedure read and changed (for precise read/write-set invalidation).
#[derive(Default)]
struct Capture {
    read_set: ReadSet,
    changes: Vec<Change>,
}

/// A procedure result plus the read-set it captured and the changes it produced.
pub struct ExecResult {
    pub value: Value,
    pub read_set: ReadSet,
    pub changes: Vec<Change>,
}

/// A message to a mutation's transaction task.
enum TxMsg {
    /// Run one db op inside the transaction and reply with its `Dbresult`.
    Op { op_id: u64, op: pulse_sql::DbOp },
    /// Finalize the transaction: commit (`true`) or roll back (`false`). The
    /// reply is `Ok(())` on success, or `Err(code)` if the commit/op hit a
    /// serialization failure (`"SERIALIZATION_FAILURE"`) — the trigger for retry —
    /// or `"DUPLICATE"` if the idempotency key was already applied. `result` is the
    /// handler's return value, recorded with the idempotency key.
    Finish {
        commit: bool,
        result: Option<Value>,
        done: oneshot::Sender<Result<(), String>>,
    },
}

/// True if a sqlx error is a Postgres serialization failure (SQLSTATE 40001) or
/// deadlock (40P01) — both are safe to retry for a deterministic handler.
fn is_serialization_failure(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = err {
        matches!(db.code().as_deref(), Some("40001") | Some("40P01"))
    } else {
        false
    }
}

/// Same check for a `pulse_sql::SqlError` (the op-path error type), which wraps a
/// `sqlx::Error` in its `Db` variant.
fn sql_err_is_serialization(err: &pulse_sql::SqlError) -> bool {
    matches!(err, pulse_sql::SqlError::Db(e) if is_serialization_failure(e))
}

struct Inner {
    pool: PgPool,
    olap_pool: PgPool,
    /// Max SERIALIZABLE retry attempts for a mutation before surfacing CONFLICT.
    /// From `PULSE_MAX_TX_ATTEMPTS` (default 25); lets tests force exhaustion.
    max_tx_attempts: u32,
    catalog: tokio::sync::OnceCell<Catalog>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, WorkerError>>>>,
    captures: Mutex<HashMap<String, Capture>>,
    /// For in-flight mutation requests: the channel to their transaction task.
    tx_senders: Mutex<HashMap<String, mpsc::Sender<TxMsg>>>,
    /// In-flight analytical requests — their autocommit ops route to `olap_pool`.
    analytical: Mutex<std::collections::HashSet<String>>,
    stdin: Mutex<ChildStdin>,
}

impl Inner {
    async fn write_msg(&self, msg: &EngineMsg) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(msg).expect("EngineMsg serializes");
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(&line).await?;
        stdin.flush().await
    }

    /// Fold an op's reads into the request's capture (before execution/reply).
    async fn capture_read(&self, request_id: &str, op: &pulse_sql::DbOp, catalog: &Catalog) {
        let mut caps = self.captures.lock().await;
        if let Some(capture) = caps.get_mut(request_id) {
            capture_reads(op, catalog, &mut capture.read_set);
        }
    }

    /// Record a change an op produced into the request's capture.
    async fn record_change(&self, request_id: &str, change: Change) {
        let mut caps = self.captures.lock().await;
        if let Some(capture) = caps.get_mut(request_id) {
            capture.changes.push(change);
        }
    }

    /// Reply to one db op with its result envelope.
    async fn reply_dbop(
        &self,
        request_id: String,
        op_id: u64,
        result: Result<(Value, Option<Change>), String>,
    ) {
        let msg = match result {
            Ok((value, change)) => {
                if let Some(change) = change {
                    self.record_change(&request_id, change).await;
                }
                EngineMsg::Dbresult {
                    request_id,
                    op_id,
                    ok: true,
                    value: Some(value),
                    error: None,
                }
            }
            Err(e) => EngineMsg::Dbresult {
                request_id,
                op_id,
                ok: false,
                value: None,
                error: Some(e),
            },
        };
        if let Err(e) = self.write_msg(&msg).await {
            tracing::error!(target: "pulse::worker", "failed to write db result: {e}");
        }
    }
}

/// Owns a mutation's transaction: runs each op on it and commits/rolls back at
/// the end. Keeping all of a mutation's ops on one connection inside one
/// `BEGIN … COMMIT` is what makes mutations atomic.
async fn tx_task(
    inner: Arc<Inner>,
    request_id: String,
    mut rx: mpsc::Receiver<TxMsg>,
    mutation_id: Option<String>,
) {
    let catalog = inner
        .catalog
        .get()
        .expect("catalog set before db ops")
        .clone();
    let mut tx = match inner.pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(target: "pulse::worker", "failed to begin tx: {e}");
            while let Some(msg) = rx.recv().await {
                match msg {
                    TxMsg::Op { op_id, .. } => {
                        inner
                            .reply_dbop(request_id.clone(), op_id, Err("tx begin failed".into()))
                            .await;
                    }
                    TxMsg::Finish { done, .. } => {
                        let _ = done.send(Err("INTERNAL".into()));
                        return;
                    }
                }
            }
            return;
        }
    };

    // Mutations run at SERIALIZABLE; on a 40001 conflict the whole (deterministic)
    // handler is retried by `execute`.
    if let Err(e) = sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *tx)
        .await
    {
        tracing::error!(target: "pulse::worker", "failed to set SERIALIZABLE: {e}");
    }

    // Sticky flag: a serialization failure on any op means the eventual commit is
    // doomed, so we surface it as the retry trigger.
    let mut serialization_failed = false;

    while let Some(msg) = rx.recv().await {
        match msg {
            TxMsg::Op { op_id, op } => {
                inner.capture_read(&request_id, &op, &catalog).await;
                // tx is sqlx::Transaction; execute_op takes &mut PgConnection (via DerefMut).
                let result = execute_op(&mut tx, &catalog, &op).await;
                if let Err(e) = &result {
                    if sql_err_is_serialization(e) {
                        serialization_failed = true;
                    }
                }
                inner
                    .reply_dbop(request_id.clone(), op_id, result.map_err(|e| e.to_string()))
                    .await;
            }
            TxMsg::Finish {
                commit,
                result,
                done,
            } => {
                let outcome = if commit && !serialization_failed {
                    // Record the idempotency key in the SAME tx; a unique-key
                    // conflict means this write was already applied → dedupe.
                    let mut duplicate = false;
                    if let Some(id) = &mutation_id {
                        let res = result.unwrap_or(Value::Null);
                        match pulse_sql::record_mutation(&mut tx, id, &res).await {
                            Ok(true) => {}
                            Ok(false) => duplicate = true,
                            Err(e) if is_serialization_failure(&e) => serialization_failed = true,
                            Err(e) => {
                                tracing::error!(target: "pulse::worker", "idempotency insert failed: {e}");
                                let _ = tx.rollback().await;
                                let _ = done.send(Err("INTERNAL".into()));
                                return;
                            }
                        }
                    }
                    if duplicate {
                        let _ = tx.rollback().await;
                        Err("DUPLICATE".into())
                    } else if serialization_failed {
                        let _ = tx.rollback().await;
                        Err("SERIALIZATION_FAILURE".into())
                    } else {
                        // The commit LSN is captured off the critical path (after the
                        // RPC returns) so it never lengthens this SERIALIZABLE tx —
                        // see the propagation task in pulse-server's rpc handler.
                        match tx.commit().await {
                            Ok(()) => Ok(()),
                            Err(e) if is_serialization_failure(&e) => {
                                Err("SERIALIZATION_FAILURE".into())
                            }
                            Err(e) => {
                                tracing::error!(target: "pulse::worker", "tx commit failed: {e}");
                                Err("INTERNAL".into())
                            }
                        }
                    }
                } else {
                    let _ = tx.rollback().await;
                    if serialization_failed {
                        Err("SERIALIZATION_FAILURE".into())
                    } else {
                        // Handler-requested rollback (its own error) — not a tx error.
                        Ok(())
                    }
                };
                let _ = done.send(outcome);
                return;
            }
        }
    }
    // Channel closed without an explicit finish: dropping `tx` rolls it back.
}

/// A handle to the running worker. Keeps the child process alive.
pub struct Worker {
    inner: Arc<Inner>,
    procedures: Vec<ProcInfo>,
    #[allow(dead_code)]
    child: Child,
}

impl Worker {
    /// Spawn the worker, wait for its manifest, and build the catalog.
    pub async fn spawn(config: WorkerConfig) -> Result<Worker, RuntimeError> {
        let mut child = Command::new(&config.bin)
            .arg(&config.script)
            .arg(&config.app)
            .env("PULSE_SELF_URL", &config.self_url)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| RuntimeError::Spawn(config.bin.clone(), e))?;

        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");

        let max_tx_attempts = std::env::var("PULSE_MAX_TX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(25);

        let inner = Arc::new(Inner {
            pool: config.pool,
            olap_pool: config.olap_pool,
            max_tx_attempts,
            catalog: tokio::sync::OnceCell::new(),
            pending: Mutex::new(HashMap::new()),
            captures: Mutex::new(HashMap::new()),
            tx_senders: Mutex::new(HashMap::new()),
            analytical: Mutex::new(std::collections::HashSet::new()),
            stdin: Mutex::new(stdin),
        });

        let (ready_tx, ready_rx) = oneshot::channel::<Vec<ProcInfo>>();
        tokio::spawn(reader_loop(inner.clone(), stdout, ready_tx));

        let procedures = timeout(Duration::from_secs(30), ready_rx)
            .await
            .map_err(|_| RuntimeError::ManifestTimeout)?
            .map_err(|_| RuntimeError::ManifestTimeout)?;

        Ok(Worker {
            inner,
            procedures,
            child,
        })
    }

    /// The procedures the worker loaded (path + kind), for routing.
    pub fn procedures(&self) -> &[ProcInfo] {
        &self.procedures
    }

    /// The introspected catalog (snake↔camel fields, id_ref, type classes), set
    /// from the manifest before the worker reports Ready. The WAL/CDC consumer
    /// needs it to build row images identical to the in-engine capture path.
    pub fn catalog(&self) -> Option<Catalog> {
        self.inner.catalog.get().cloned()
    }

    pub fn find(&self, path: &[String]) -> Option<&ProcInfo> {
        self.procedures.iter().find(|p| p.path == path)
    }

    /// Run a procedure, retrying the whole (deterministic) handler on a
    /// SERIALIZABLE conflict (SQLSTATE 40001). Reads run once; mutations retry up
    /// to a small bound before surfacing a CONFLICT error.
    pub async fn execute(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
        mutation_id: Option<String>,
    ) -> Result<ExecResult, WorkerError> {
        // Exactly-once fast path: a mutation already applied under this id returns
        // its recorded result without re-running the handler (sequential retries
        // and other tabs flushing the shared offline queue).
        if let Some(id) = &mutation_id {
            if self.find(&path).map(|p| p.kind) == Some(ProcedureKind::Mutation) {
                if let Some(value) = pulse_sql::lookup_mutation(&self.inner.pool, id).await {
                    return Ok(ExecResult {
                        value,
                        read_set: ReadSet::new(),
                        changes: Vec::new(),
                    });
                }
            }
        }

        // Hot single-row contention (N transactions racing the same key under
        // SERIALIZABLE) only lets one winner commit per round, so a deterministic
        // handler may need many retries to converge. Generous budget + exponential
        // backoff + per-attempt jitter (from a fresh uuid; no RNG/clock dep) so
        // contenders desynchronize instead of thundering-herd.
        let max_attempts = self.inner.max_tx_attempts;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self
                .execute_once(
                    path.clone(),
                    input.clone(),
                    headers.clone(),
                    mutation_id.clone(),
                )
                .await
            {
                // A concurrent delivery won the unique idempotency insert — return
                // its recorded result instead of applying the write twice.
                Err(e) if e.code == "DUPLICATE" => {
                    let value = match &mutation_id {
                        Some(id) => pulse_sql::lookup_mutation(&self.inner.pool, id)
                            .await
                            .unwrap_or(Value::Null),
                        None => Value::Null,
                    };
                    return Ok(ExecResult {
                        value,
                        read_set: ReadSet::new(),
                        changes: Vec::new(),
                    });
                }
                Err(e) if e.code == "SERIALIZATION_FAILURE" => {
                    if attempt >= max_attempts {
                        return Err(WorkerError {
                            code: "CONFLICT".into(),
                            data: Value::Null,
                            message: Some(format!(
                                "mutation conflicted after {max_attempts} attempts"
                            )),
                        });
                    }
                    tracing::debug!(target: "pulse::worker", "serialization conflict, retry {attempt}");
                    let base = (2u64 << attempt.min(5)).min(40);
                    let jitter = (Uuid::new_v4().as_u128() as u64) % base.max(1);
                    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
                }
                other => return other,
            }
        }
    }

    async fn execute_once(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
        mutation_id: Option<String>,
    ) -> Result<ExecResult, WorkerError> {
        let request_id = Uuid::new_v4().to_string();
        let kind = self.find(&path).map(|p| p.kind);
        let is_mutation = kind == Some(ProcedureKind::Mutation);
        let is_analytical = kind == Some(ProcedureKind::Analytical);

        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .insert(request_id.clone(), tx);
        self.inner
            .captures
            .lock()
            .await
            .insert(request_id.clone(), Capture::default());

        // A mutation runs all its db ops inside one transaction task.
        if is_mutation {
            let (op_tx, op_rx) = mpsc::channel::<TxMsg>(32);
            self.inner
                .tx_senders
                .lock()
                .await
                .insert(request_id.clone(), op_tx);
            tokio::spawn(tx_task(
                self.inner.clone(),
                request_id.clone(),
                op_rx,
                mutation_id.clone(),
            ));
        }
        // Analytical reads route their autocommit ops to the OLAP pool.
        if is_analytical {
            self.inner
                .analytical
                .lock()
                .await
                .insert(request_id.clone());
        }

        let msg = EngineMsg::Execute {
            request_id: request_id.clone(),
            path,
            input,
            headers,
        };
        if let Err(e) = self.inner.write_msg(&msg).await {
            self.inner.pending.lock().await.remove(&request_id);
            self.inner.captures.lock().await.remove(&request_id);
            self.inner.tx_senders.lock().await.remove(&request_id);
            self.inner.analytical.lock().await.remove(&request_id);
            return Err(WorkerError::internal(format!(
                "write to worker failed: {e}"
            )));
        }

        let outcome = match rx.await {
            Ok(result) => result,
            Err(_) => Err(WorkerError::internal("worker dropped the request")),
        };
        self.inner.analytical.lock().await.remove(&request_id);
        let capture = self
            .inner
            .captures
            .lock()
            .await
            .remove(&request_id)
            .unwrap_or_default();

        outcome.map(|value| ExecResult {
            value,
            read_set: capture.read_set,
            changes: capture.changes,
        })
    }
}

async fn reader_loop(
    inner: Arc<Inner>,
    stdout: tokio::process::ChildStdout,
    ready_tx: oneshot::Sender<Vec<ProcInfo>>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ready_tx = Some(ready_tx);
    let mut pending_manifest: Option<Vec<ProcInfo>> = None;

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => {
                tracing::warn!(target: "pulse::worker", "worker stdout closed");
                break;
            }
            Err(e) => {
                tracing::error!(target: "pulse::worker", "error reading worker: {e}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<WorkerOut>(&line) {
            Ok(WorkerOut::Manifest { procedures, schema }) => {
                match introspect(&inner.pool, &schema).await {
                    Ok(catalog) => {
                        let _ = inner.catalog.set(catalog);
                        pending_manifest = Some(procedures);
                    }
                    Err(e) => {
                        tracing::error!(target: "pulse::worker", "introspection failed: {e}");
                    }
                }
            }
            Ok(WorkerOut::Ready) => {
                if let (Some(tx), Some(procs)) = (ready_tx.take(), pending_manifest.take()) {
                    let _ = tx.send(procs);
                }
            }
            Ok(WorkerOut::Dbop {
                request_id,
                op_id,
                op,
            }) => {
                // Mutation ops are routed to the request's transaction task so all
                // of a mutation's writes share one BEGIN…COMMIT (atomic + retryable).
                // Other ops run autocommit on a pooled connection, concurrently.
                let tx_sender = inner.tx_senders.lock().await.get(&request_id).cloned();
                if let Some(sender) = tx_sender {
                    if sender.send(TxMsg::Op { op_id, op }).await.is_err() {
                        inner
                            .reply_dbop(request_id, op_id, Err("transaction closed".into()))
                            .await;
                    }
                } else {
                    // Analytical requests run on the OLAP pool (separate budget +
                    // longer statement timeout); everything else on the OLTP pool.
                    let is_analytical = inner.analytical.lock().await.contains(&request_id);
                    let inner = inner.clone();
                    tokio::spawn(async move {
                        let catalog = inner.catalog.get().expect("catalog set before db ops");
                        // Capture the read-set before replying, so it is in place
                        // before the worker can `complete`.
                        inner.capture_read(&request_id, &op, catalog).await;
                        let pool = if is_analytical {
                            &inner.olap_pool
                        } else {
                            &inner.pool
                        };
                        let result = execute_op_pool(pool, catalog, &op)
                            .await
                            .map_err(|e| e.to_string());
                        inner.reply_dbop(request_id, op_id, result).await;
                    });
                }
            }
            Ok(WorkerOut::Complete {
                request_id,
                ok,
                result,
                error,
            }) => {
                // If this was a mutation, finalize its transaction first: commit on
                // success, roll back on handler error — so the result the caller
                // observes reflects committed (or fully-rolled-back) state. A
                // serialization failure at commit becomes a SERIALIZATION_FAILURE
                // error so `execute` can retry the whole handler.
                let tx_sender = inner.tx_senders.lock().await.remove(&request_id);
                let mut commit_conflict = false;
                let mut duplicate = false;
                if let Some(sender) = tx_sender {
                    let (done_tx, done_rx) = oneshot::channel();
                    let finish = TxMsg::Finish {
                        commit: ok,
                        result: Some(result.clone()),
                        done: done_tx,
                    };
                    if sender.send(finish).await.is_ok() {
                        if let Ok(Err(code)) = done_rx.await {
                            match code.as_str() {
                                "SERIALIZATION_FAILURE" => commit_conflict = true,
                                "DUPLICATE" => duplicate = true,
                                _ => {}
                            }
                        }
                    }
                }
                if let Some(tx) = inner.pending.lock().await.remove(&request_id) {
                    let outcome = if commit_conflict {
                        Err(WorkerError {
                            code: "SERIALIZATION_FAILURE".into(),
                            data: Value::Null,
                            message: Some("serialization conflict".into()),
                        })
                    } else if duplicate {
                        Err(WorkerError {
                            code: "DUPLICATE".into(),
                            data: Value::Null,
                            message: Some("duplicate mutation".into()),
                        })
                    } else if ok {
                        Ok(result)
                    } else {
                        Err(error.unwrap_or_else(|| WorkerError::internal("handler failed")))
                    };
                    let _ = tx.send(outcome);
                }
            }
            Ok(WorkerOut::Log { level, message }) => {
                tracing::info!(target: "pulse::worker", level = %level, "{message}");
            }
            Err(e) => {
                tracing::warn!(target: "pulse::worker", "unparseable worker message ({e}): {line}");
            }
        }
    }
}
