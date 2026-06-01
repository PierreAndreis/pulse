//! The Pulse engine binary.
//!
//! M1: OLTP pool + TS worker + `POST /rpc` (non-reactive).
//! M2: `GET /sync` (SSE) + `POST /subscribe`/`/unsubscribe` + the reactor.
//! M3: precise per-change invalidation — a mutation's captured `ChangeSet` is
//! matched against each subscription's `ReadSet`, so only truly-affected
//! subscriptions re-run and push.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

use uuid::Uuid;

use pulse_core::{ChangeSet, Lsn};
use pulse_jsruntime::{Worker, WorkerConfig, WorkerError};
use pulse_reactor::{InMemoryReactor, ReExecutor, Reactor, Subscription};

struct AppState {
    worker: Arc<Worker>,
    reactor: Arc<dyn Reactor>,
    /// OLTP pool, used to publish change-sets to the cross-node bus.
    pool: pulse_sql::PgPool,
    /// This node's id — bus messages it originates are skipped on receipt.
    node_id: String,
}

/// Re-executes procedures for the reactor, over the JS-runtime worker.
struct WorkerReExecutor {
    worker: Arc<Worker>,
}

#[async_trait]
impl ReExecutor for WorkerReExecutor {
    async fn exec(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
    ) -> Result<Value, String> {
        self.worker
            .execute(path, input, headers, None)
            .await
            .map(|res| res.value)
            .map_err(|e| e.code)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("pulse=info,warn")),
        )
        .init();

    let database_url = env_or("DATABASE_URL", "postgres://pulse:pulse@localhost:54329/pulse");
    // The OLAP pool may point at a read replica; defaults to the same DB.
    let olap_url = env_or("PULSE_OLAP_DATABASE_URL", &database_url);
    let port: u16 = env_or("PULSE_PORT", "8787").parse().unwrap_or(8787);
    let max_conns: u32 = env_or("PULSE_OLTP_MAX_CONNS", "10").parse().unwrap_or(10);
    let olap_max_conns: u32 = env_or("PULSE_OLAP_MAX_CONNS", "4").parse().unwrap_or(4);
    // Statement timeouts (ms): OLTP low so a slow query can't pin the reactive
    // hot path; OLAP high for heavy analytics. 0 disables.
    let oltp_timeout: u64 = env_or("PULSE_OLTP_STATEMENT_TIMEOUT_MS", "15000").parse().unwrap_or(15000);
    let olap_timeout: u64 = env_or("PULSE_OLAP_STATEMENT_TIMEOUT_MS", "60000").parse().unwrap_or(60000);

    let worker_bin = env_or("PULSE_WORKER_BIN", "bun");
    let worker_script = env_or("PULSE_WORKER_SCRIPT", "packages/runtime-node/src/worker.ts");
    let app_module = env_or("PULSE_APP", "packages/examples-chat/src/app.ts");

    tracing::info!("connecting to Postgres (OLTP {max_conns} conns, OLAP {olap_max_conns} conns)");
    let pool = pulse_sql::connect_with(
        &database_url,
        pulse_sql::PoolConfig {
            max_connections: max_conns,
            serializable: true,
            statement_timeout_ms: (oltp_timeout > 0).then_some(oltp_timeout),
        },
    )
    .await?;
    // Idempotency-key log for exactly-once mutations.
    pulse_sql::ensure_mutation_log(&pool).await?;
    let olap_pool = pulse_sql::connect_with(
        &olap_url,
        pulse_sql::PoolConfig {
            max_connections: olap_max_conns,
            serializable: false,
            statement_timeout_ms: (olap_timeout > 0).then_some(olap_timeout),
        },
    )
    .await?;

    tracing::info!("spawning worker: {worker_bin} {worker_script} {app_module}");
    let worker = Worker::spawn(WorkerConfig {
        bin: worker_bin,
        script: worker_script,
        app: app_module,
        pool: pool.clone(),
        olap_pool,
        self_url: format!("http://127.0.0.1:{port}"),
    })
    .await?;
    tracing::info!("worker ready — {} procedures loaded", worker.procedures().len());

    let worker = Arc::new(worker);
    let reexec = Arc::new(WorkerReExecutor { worker: worker.clone() });
    let reactor: Arc<dyn Reactor> = Arc::new(InMemoryReactor::new(reexec));

    // Cross-node change bus: each node has an id; after a local mutation it
    // applies invalidation locally AND publishes the change-set, so writes on any
    // node invalidate subscriptions on every node. The listener drops messages
    // this node originated (already applied locally), keeping single-node behavior
    // unchanged.
    let node_id = Uuid::new_v4().to_string();
    match pulse_cdc::start_listener(&database_url, node_id.clone()).await {
        Ok(mut rx) => {
            let reactor = reactor.clone();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    match event {
                        pulse_cdc::BusEvent::Changes(cs) => reactor.apply_change_set(cs).await,
                        pulse_cdc::BusEvent::Resync => reactor.invalidate_all().await,
                    }
                }
            });
            tracing::info!("change bus listener started (node {node_id})");
        }
        Err(e) => tracing::error!("failed to start change bus listener: {e}"),
    }

    let state = Arc::new(AppState { worker, reactor, pool: pool.clone(), node_id });

    let app = Router::new()
        .route("/health", get(health))
        .route("/rpc", post(rpc))
        .route("/sync", get(sync))
        .route("/subscribe", post(subscribe))
        .route("/unsubscribe", post(unsubscribe))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("pulse-server listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

fn collect_headers(headers: &HeaderMap) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (name, value) in headers {
        if let Ok(v) = value.to_str() {
            out.insert(name.as_str().to_string(), v.to_string());
        }
    }
    out
}

fn status_for(code: &str) -> StatusCode {
    match code {
        "UNAUTHORIZED" => StatusCode::UNAUTHORIZED,
        "FORBIDDEN" => StatusCode::FORBIDDEN,
        "NOT_FOUND" => StatusCode::NOT_FOUND,
        "CONFLICT" => StatusCode::CONFLICT,
        "RATE_LIMITED" => StatusCode::TOO_MANY_REQUESTS,
        "BAD_REQUEST" => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_body(err: &WorkerError) -> Value {
    json!({ "error": { "code": err.code, "data": err.data, "message": err.message } })
}

#[derive(Deserialize)]
struct RpcRequest {
    path: Vec<String>,
    #[serde(default)]
    input: Value,
    /// Optional idempotency key for mutations (stable per queued write), so a
    /// retry or a second tab flushing the shared offline queue applies it once.
    #[serde(default, rename = "mutationId")]
    mutation_id: Option<String>,
}

async fn rpc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RpcRequest>,
) -> (StatusCode, Json<Value>) {
    if state.worker.find(&req.path).is_none() {
        let err = WorkerError {
            code: "NOT_FOUND".to_string(),
            data: Value::Null,
            message: Some(format!("no procedure at `{}`", req.path.join("."))),
        };
        return (StatusCode::NOT_FOUND, Json(error_body(&err)));
    }

    match state.worker.execute(req.path, req.input, collect_headers(&headers), req.mutation_id).await {
        Ok(res) => {
            if !res.changes.is_empty() {
                let change_set = ChangeSet { commit_lsn: Lsn::ZERO, changes: res.changes };
                // Apply locally now (low latency for this node's own subscribers)...
                let reactor = state.reactor.clone();
                tokio::spawn({
                    let cs = change_set.clone();
                    async move { reactor.apply_change_set(cs).await }
                });
                // ...and publish to the bus so every other node invalidates too.
                // (Self-originated messages are dropped on receipt — see node_id.)
                let pool = state.pool.clone();
                let node_id = state.node_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = pulse_cdc::publish(&pool, &node_id, &change_set).await {
                        tracing::warn!("change bus publish failed: {e}");
                    }
                });
            }
            (StatusCode::OK, Json(json!({ "result": res.value })))
        }
        Err(err) => (status_for(&err.code), Json(error_body(&err))),
    }
}

#[derive(Deserialize)]
struct SyncQuery {
    #[serde(rename = "clientId")]
    client_id: String,
}

async fn sync(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SyncQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.reactor.register_client(q.client_id).await;
    let stream = ReceiverStream::new(rx)
        .map(|push| Ok(Event::default().id(push.id.to_string()).data(push.body)));
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
struct SubscribeReq {
    #[serde(rename = "clientId")]
    client_id: String,
    sub: String,
    path: Vec<String>,
    #[serde(default)]
    input: Value,
}

async fn subscribe(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SubscribeReq>,
) -> (StatusCode, Json<Value>) {
    let hdrs = collect_headers(&headers);
    match state.worker.execute(req.path.clone(), req.input.clone(), hdrs.clone(), None).await {
        Ok(res) => {
            state
                .reactor
                .add_subscription(Subscription {
                    client_id: req.client_id.clone(),
                    sub: req.sub.clone(),
                    path: req.path,
                    input: req.input,
                    headers: hdrs,
                    read_set: res.read_set,
                    last: Some(res.value.clone()),
                })
                .await;
            // Initial push reflects no committed change yet → LSN zero.
            state.reactor.push(&req.client_id, &req.sub, &res.value, Lsn::ZERO).await;
            (StatusCode::OK, Json(json!({ "result": "ok" })))
        }
        Err(err) => (status_for(&err.code), Json(error_body(&err))),
    }
}

#[derive(Deserialize)]
struct UnsubscribeReq {
    #[serde(rename = "clientId")]
    client_id: String,
    sub: String,
}

async fn unsubscribe(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UnsubscribeReq>,
) -> StatusCode {
    state.reactor.remove_subscription(&req.client_id, &req.sub).await;
    StatusCode::OK
}
