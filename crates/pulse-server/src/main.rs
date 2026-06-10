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

use pulse_core::{ReadSet, TableId};
use pulse_engine::{
    Coordinator, ExecError, ExecOutcome, Executor, InterestRegistry, Invalidation, Publisher,
};
use pulse_jsruntime::{Worker, WorkerConfig};
use pulse_reactor::{InMemoryReactor, InterestSink, ReExecutor, Reactor};

/// The thin axum surface: every handler deserializes and delegates to the
/// [`Coordinator`], which owns the request and cross-node orchestration. `node_id`
/// and `broadcast` are kept only for the `/metrics` display.
struct AppState {
    coord: Arc<Coordinator>,
    node_id: String,
    broadcast: bool,
}

/// Microseconds since the Unix epoch (wall clock), for same-host bus-latency math.
fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Adapts the JS-runtime worker to the reactor's `ReExecutor` (value + read-set;
/// the write-set is dropped — re-execution is reads only).
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
    ) -> Result<(Value, ReadSet), String> {
        self.worker
            .execute(path, input, headers, None)
            .await
            .map(|res| (res.value, res.read_set))
            .map_err(|e| e.code)
    }
}

/// Adapts the worker to the coordinator's `Executor` (value + read-set + write-set,
/// plus the existence check that drives the NOT_FOUND pre-check).
struct WorkerExecutor {
    worker: Arc<Worker>,
}

#[async_trait]
impl Executor for WorkerExecutor {
    fn contains(&self, path: &[String]) -> bool {
        self.worker.find(path).is_some()
    }
    async fn execute(
        &self,
        path: Vec<String>,
        input: Value,
        headers: HashMap<String, String>,
        mutation_id: Option<String>,
    ) -> Result<ExecOutcome, ExecError> {
        self.worker
            .execute(path, input, headers, mutation_id)
            .await
            .map(|res| ExecOutcome {
                value: res.value,
                read_set: res.read_set,
                changes: res.changes,
            })
            .map_err(|e| ExecError {
                code: e.code,
                data: e.data,
                message: e.message,
            })
    }
}

/// Publishes committed change-sets to other nodes over the routed change bus —
/// the coordinator's `Publisher`. Best-effort: a failed publish is logged, not
/// surfaced (local delivery already happened; resync recovers a missed message).
struct BusPublisher {
    pool: pulse_sql::PgPool,
    node_id: String,
    opts: pulse_cdc::PublishOpts,
}

#[async_trait]
impl Publisher for BusPublisher {
    async fn publish(&self, change_set: &pulse_core::ChangeSet) {
        match self.pool.acquire().await {
            Ok(mut conn) => {
                if let Err(e) =
                    pulse_cdc::publish_on(&mut conn, &self.node_id, change_set, self.opts).await
                {
                    tracing::warn!("change bus publish failed: {e}");
                }
            }
            Err(e) => tracing::warn!("change bus publish: pool acquire failed: {e}"),
        }
    }
}

/// This node's interest on the change bus, satisfying two seams over one adapter:
/// the reactor's `InterestSink` (register the instant a table is first watched) and
/// the coordinator's `InterestRegistry` (heartbeat refresh + prune dead nodes).
struct BusInterest {
    pool: pulse_sql::PgPool,
    node_id: String,
    ttl_secs: i64,
}

impl BusInterest {
    async fn register(&self, tables: &[String]) {
        if let Err(e) = pulse_cdc::register_interest(&self.pool, &self.node_id, tables).await {
            tracing::warn!("interest register failed: {e}");
        }
    }
}

#[async_trait]
impl InterestSink for BusInterest {
    async fn on_tables_added(&self, tables: Vec<TableId>) {
        let names: Vec<String> = tables.into_iter().map(|t| t.0).collect();
        self.register(&names).await;
    }
}

#[async_trait]
impl InterestRegistry for BusInterest {
    async fn refresh(&self, tables: Vec<TableId>) {
        let names: Vec<String> = tables.into_iter().map(|t| t.0).collect();
        self.register(&names).await;
    }
    async fn prune(&self) {
        let _ = pulse_cdc::prune_interest(&self.pool, self.ttl_secs).await;
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Parse a numeric env value, falling back to `default` if unset or unparseable.
fn parse_num(raw: Option<String>, default: u64) -> u64 {
    raw.and_then(|s| s.trim().parse().ok()).unwrap_or(default)
}
fn num_env(key: &str, default: u64) -> u64 {
    parse_num(std::env::var(key).ok(), default)
}

/// A stable advisory-lock key derived from the slot name (FNV-1a) so every node
/// computes the same key and contends for the same single-consumer slot.
fn wal_lock_key(slot: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in slot.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as i64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("pulse=info,warn")),
        )
        .init();

    let database_url = env_or(
        "DATABASE_URL",
        "postgres://pulse:pulse@localhost:54329/pulse",
    );
    // The OLAP pool may point at a read replica; defaults to the same DB.
    let olap_url = env_or("PULSE_OLAP_DATABASE_URL", &database_url);
    let port: u16 = env_or("PULSE_PORT", "8787").parse().unwrap_or(8787);
    let max_conns: u32 = env_or("PULSE_OLTP_MAX_CONNS", "10").parse().unwrap_or(10);
    let olap_max_conns: u32 = env_or("PULSE_OLAP_MAX_CONNS", "4").parse().unwrap_or(4);
    // Statement timeouts (ms): OLTP low so a slow query can't pin the reactive
    // hot path; OLAP high for heavy analytics. 0 disables.
    let oltp_timeout: u64 = env_or("PULSE_OLTP_STATEMENT_TIMEOUT_MS", "15000")
        .parse()
        .unwrap_or(15000);
    let olap_timeout: u64 = env_or("PULSE_OLAP_STATEMENT_TIMEOUT_MS", "60000")
        .parse()
        .unwrap_or(60000);

    // Cross-node bus tuning knobs (great defaults; override for your deployment).
    let interest_ttl_secs = num_env("PULSE_INTEREST_TTL_SECS", 30) as i64;
    let configured_heartbeat = num_env("PULSE_HEARTBEAT_MS", 10_000);
    // The heartbeat must refresh interest well within the TTL, or a live node's
    // interest lapses and it silently misses invalidations — clamp to TTL/3.
    let heartbeat_ms = pulse_engine::clamp_heartbeat(interest_ttl_secs, configured_heartbeat);
    if heartbeat_ms != configured_heartbeat {
        tracing::warn!(
            "PULSE_HEARTBEAT_MS {configured_heartbeat} > ttl/3; clamping to {heartbeat_ms}"
        );
    }
    let wal_sample_ms = num_env("PULSE_WAL_SAMPLE_MS", 100).max(1);
    let sse_buffer = num_env("PULSE_SSE_BUFFER", pulse_reactor::DEFAULT_SSE_BUFFER as u64) as usize;

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
    tracing::info!(
        "worker ready — {} procedures loaded",
        worker.procedures().len()
    );

    let worker = Arc::new(worker);
    let reexec = Arc::new(WorkerReExecutor {
        worker: worker.clone(),
    });

    // Cross-node change bus: each node has an id; after a local mutation it applies
    // invalidation locally AND publishes the change-set, routed to nodes interested
    // in the touched tables. The interest registry is the routing table; this node
    // registers a table the moment the reactor begins watching it.
    let node_id = Uuid::new_v4().to_string();
    if let Err(e) = pulse_cdc::ensure_interest_table(&pool).await {
        tracing::error!("failed to ensure interest table: {e}");
    }
    let broadcast = env_or("PULSE_BUS_BROADCAST", "0") == "1";
    let chunk = env_or("PULSE_BUS_CHUNK", "1") == "1";

    // One adapter satisfies both interest seams: the reactor's `InterestSink`
    // (register on first watch) and the coordinator's `InterestRegistry` (heartbeat).
    let interest = Arc::new(BusInterest {
        pool: pool.clone(),
        node_id: node_id.clone(),
        ttl_secs: interest_ttl_secs,
    });
    let reactor: Arc<dyn Reactor> = Arc::new(
        InMemoryReactor::new(reexec)
            .with_interest(interest.clone())
            .with_sse_buffer(sse_buffer),
    );

    // WAL position sampler: refresh the commit watermark off the write path so
    // mutations don't each pay a WAL round-trip. ~100ms granularity is plenty for a
    // monotonic watermark; the first tick fires immediately so writes get a real LSN.
    let wal_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let pool = pool.clone();
        let wal_lsn = wal_lsn.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(wal_sample_ms));
            loop {
                tick.tick().await;
                if let Ok(mut conn) = pool.acquire().await {
                    if let Ok(lsn) = pulse_sql::current_wal_lsn(&mut conn).await {
                        wal_lsn.store(lsn.as_u64(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // The coordinator owns the request + cross-node orchestration over four seams:
    // the worker (Executor), the reactor, the bus publisher, and the interest registry.
    let executor = Arc::new(WorkerExecutor {
        worker: worker.clone(),
    });
    let publisher = Arc::new(BusPublisher {
        pool: pool.clone(),
        node_id: node_id.clone(),
        opts: pulse_cdc::PublishOpts {
            broadcast,
            chunk,
            ttl_secs: interest_ttl_secs,
        },
    });
    let coord = Arc::new(Coordinator::new(
        reactor.clone(),
        executor,
        publisher,
        interest,
        wal_lsn.clone(),
    ));

    // Heartbeat: keep this node's interest rows fresh and prune dead nodes'. The
    // interval must stay well under INTEREST_TTL_SECS so interest never lapses.
    {
        let coord = coord.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(heartbeat_ms));
            loop {
                tick.tick().await;
                coord.on_heartbeat().await;
            }
        });
    }

    // Cross-node bus listener: translate each wire event into a neutral
    // `Invalidation` (computing the commit→deliver lag at the edge, where the wall
    // clock lives) and hand it to the coordinator, which applies + accounts for it.
    match pulse_cdc::start_listener(&database_url, node_id.clone()).await {
        Ok(mut rx) => {
            let coord = coord.clone();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let (inv, lag) = match event {
                        pulse_cdc::BusEvent::Changes { cs, origin_us } => (
                            Invalidation::Changes(cs),
                            Some((now_us() - origin_us).max(0) as u64),
                        ),
                        pulse_cdc::BusEvent::ResyncTables(tables) => (
                            Invalidation::Tables(tables.into_iter().map(TableId::new).collect()),
                            None,
                        ),
                        pulse_cdc::BusEvent::Resync => (Invalidation::All, None),
                    };
                    coord.apply_invalidation(inv, lag).await;
                }
            });
            tracing::info!("change bus listener started (node {node_id}, broadcast={broadcast})");
        }
        Err(e) => tracing::error!("failed to start change bus listener: {e}"),
    }

    // WAL/CDC consumer (opt-in via PULSE_WAL=1): drain a logical replication slot
    // so writes made OUTSIDE the engine (raw SQL, other services, triggers) also
    // invalidate subscriptions. Mode B — the engine keeps its synchronous
    // in-engine apply; the WAL stream carries out-of-band writes plus echoes of
    // in-engine writes, which `apply_wal` dedups. Requires `wal_level=logical`
    // and a role that owns the single-consumer slot.
    if env_or("PULSE_WAL", "0") == "1" {
        let role = env_or("PULSE_ROLE", "all-in-one");
        let consumes = matches!(role.as_str(), "all-in-one" | "change-router");
        match (consumes, worker.catalog()) {
            (false, _) => tracing::info!("PULSE_WAL=1 but role `{role}` does not consume the WAL"),
            (true, None) => {
                tracing::error!("PULSE_WAL=1 but the catalog is not ready; skipping WAL consumer")
            }
            (true, Some(catalog)) => {
                let slot = env_or("PULSE_WAL_SLOT", pulse_cdc::wal::DEFAULT_SLOT);
                let publication = env_or("PULSE_WAL_PUBLICATION", pulse_cdc::wal::DEFAULT_PUBLICATION);
                let poll_ms = num_env("PULSE_WAL_POLL_MS", 50).max(1);
                if let Err(e) = pulse_cdc::wal::ensure_publication(&pool, &publication).await {
                    tracing::error!("WAL: ensure_publication failed: {e}");
                }
                if let Err(e) = pulse_cdc::wal::ensure_slot(&pool, &slot).await {
                    tracing::error!("WAL: ensure_slot failed: {e}");
                }
                // Single-consumer: only the node holding the advisory lock drains it.
                match pulse_cdc::wal::try_acquire_leadership(&database_url, wal_lock_key(&slot)).await
                {
                    Ok(Some(guard)) => {
                        let mut rx = pulse_cdc::wal::start_wal_consumer(
                            pool.clone(),
                            slot.clone(),
                            publication,
                            catalog,
                            std::time::Duration::from_millis(poll_ms),
                        );
                        let coord = coord.clone();
                        tokio::spawn(async move {
                            let _guard = guard; // hold leadership for the consumer's life
                            while let Some(ev) = rx.recv().await {
                                match ev {
                                    pulse_cdc::wal::WalEvent::Changes(cs) => {
                                        coord.apply_wal(cs, None).await
                                    }
                                    pulse_cdc::wal::WalEvent::Resync(tables) => {
                                        coord
                                            .apply_invalidation(
                                                Invalidation::Tables(tables.into_iter().collect()),
                                                None,
                                            )
                                            .await
                                    }
                                }
                            }
                        });
                        tracing::info!("WAL consumer started (slot={slot}, poll={poll_ms}ms)");
                    }
                    Ok(None) => {
                        tracing::info!("WAL consumer: another node holds the slot; standing by")
                    }
                    Err(e) => tracing::error!("WAL: leadership acquire failed: {e}"),
                }
            }
        }
    }

    let state = Arc::new(AppState {
        coord,
        node_id,
        broadcast,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(node_metrics))
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

/// Node metrics for the fan-out / latency benchmarks. `busEvents` is how many
/// cross-node bus messages this node received (the network-layer work). The latency
/// decomposition reports averages (ms): `busLagMs` = commit→deliver (bus
/// propagation), `applyMs` = deliver→applied (match + re-exec + push).
async fn node_metrics(State(state): State<Arc<AppState>>) -> Json<Value> {
    use std::sync::atomic::Ordering::Relaxed;
    let m = state.coord.metrics();
    let events = m.events.load(Relaxed);
    let changes = m.changes.load(Relaxed);
    let avg_ms = |sum: u64, n: u64| {
        if n == 0 {
            0.0
        } else {
            (sum as f64 / n as f64) / 1000.0
        }
    };
    Json(json!({
        "nodeId": state.node_id,
        "broadcast": state.broadcast,
        "busEvents": events,
        "changes": changes,
        "resyncs": m.resyncs.load(Relaxed),
        "busLagMs": avg_ms(m.lag_us_sum.load(Relaxed), changes),
        "applyMs": avg_ms(m.apply_us_sum.load(Relaxed), events),
        // Raw sums (µs) so a benchmark can compute a windowed average via deltas.
        "lagUsSum": m.lag_us_sum.load(Relaxed),
        "applyUsSum": m.apply_us_sum.load(Relaxed),
    }))
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

fn error_body(err: &ExecError) -> Value {
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
    match state
        .coord
        .handle_rpc(
            req.path,
            req.input,
            collect_headers(&headers),
            req.mutation_id,
        )
        .await
    {
        Ok(ok) => {
            // A mutation's change-set propagates off the RPC's critical path: local
            // apply in parallel with the routed cross-node publish. Spawning here
            // (a latency concern) is the handler's job; the orchestration is the
            // coordinator's.
            if let Some(change_set) = ok.propagate {
                let coord = state.coord.clone();
                tokio::spawn(async move { coord.propagate(change_set).await });
            }
            (StatusCode::OK, Json(json!({ "result": ok.value })))
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
    let rx = state.coord.register_client(q.client_id).await;
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
    match state
        .coord
        .handle_subscribe(
            req.client_id,
            req.sub,
            req.path,
            req.input,
            collect_headers(&headers),
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "result": "ok" }))),
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
    state
        .coord
        .handle_unsubscribe(&req.client_id, &req.sub)
        .await;
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::{HeaderName, HeaderValue};

    #[test]
    fn parse_num_uses_default_when_absent_or_invalid() {
        assert_eq!(parse_num(None, 100), 100); // unset → default
        assert_eq!(parse_num(Some("250".into()), 100), 250); // valid override
        assert_eq!(parse_num(Some("  42 ".into()), 100), 42); // trimmed
        assert_eq!(parse_num(Some("nope".into()), 100), 100); // garbage → default
        assert_eq!(parse_num(Some("".into()), 100), 100); // empty → default
    }

    #[test]
    fn status_for_known_and_unknown() {
        assert_eq!(status_for("UNAUTHORIZED"), StatusCode::UNAUTHORIZED);
        assert_eq!(status_for("FORBIDDEN"), StatusCode::FORBIDDEN);
        assert_eq!(status_for("NOT_FOUND"), StatusCode::NOT_FOUND);
        assert_eq!(status_for("CONFLICT"), StatusCode::CONFLICT);
        assert_eq!(status_for("RATE_LIMITED"), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(status_for("BAD_REQUEST"), StatusCode::BAD_REQUEST);

        // Anything not in the map falls through to 500.
        assert_eq!(status_for("INTERNAL"), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(status_for("not_found"), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(status_for(""), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(status_for("WHATEVER"), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_body_shape() {
        let err = ExecError {
            code: "NOT_FOUND".to_string(),
            data: json!({ "k": "v", "n": 1 }),
            message: Some("nope".to_string()),
        };
        let body = error_body(&err);
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "NOT_FOUND",
                    "data": { "k": "v", "n": 1 },
                    "message": "nope"
                }
            })
        );

        // message: None serializes to JSON null; arbitrary data passes through.
        let err = ExecError {
            code: "BAD_REQUEST".to_string(),
            data: json!([1, 2, 3]),
            message: None,
        };
        let body = error_body(&err);
        assert_eq!(body["error"]["code"], json!("BAD_REQUEST"));
        assert_eq!(body["error"]["data"], json!([1, 2, 3]));
        assert_eq!(body["error"]["message"], Value::Null);
    }

    #[test]
    fn collect_headers_skips_non_ascii() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-ok"),
            HeaderValue::from_static("yes"),
        );
        // A value with bytes that to_str() (ASCII) rejects: it should be skipped.
        headers.insert(
            HeaderName::from_static("x-bad"),
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );

        let out = collect_headers(&headers);
        assert_eq!(out.get("x-ok").map(String::as_str), Some("yes"));
        assert!(
            !out.contains_key("x-bad"),
            "non-ascii header must be dropped"
        );
    }

    #[test]
    fn collect_headers_duplicate_last_wins() {
        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("x-dup"),
            HeaderValue::from_static("first"),
        );
        headers.append(
            HeaderName::from_static("x-dup"),
            HeaderValue::from_static("second"),
        );

        let out = collect_headers(&headers);
        // HashMap insert overwrites, iterating in order → last value wins.
        assert_eq!(out.get("x-dup").map(String::as_str), Some("second"));
    }

    #[test]
    fn rpc_request_defaults() {
        let req: RpcRequest = serde_json::from_value(json!({ "path": ["a"] })).unwrap();
        assert_eq!(req.path, vec!["a".to_string()]);
        assert_eq!(req.input, Value::Null);
        assert_eq!(req.mutation_id, None);
    }

    #[test]
    fn rpc_request_mutation_id_camel_case() {
        let req: RpcRequest = serde_json::from_value(json!({
            "path": ["a", "b"],
            "input": { "x": 1 },
            "mutationId": "m-123"
        }))
        .unwrap();
        assert_eq!(req.path, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(req.input, json!({ "x": 1 }));
        assert_eq!(req.mutation_id, Some("m-123".to_string()));
    }

    #[test]
    fn rpc_request_path_must_be_array() {
        let res: Result<RpcRequest, _> = serde_json::from_value(json!({ "path": "a" }));
        assert!(res.is_err(), "non-array path must fail to deserialize");
    }

    #[test]
    fn subscribe_req_defaults_and_camel_case() {
        let req: SubscribeReq = serde_json::from_value(json!({
            "clientId": "c1",
            "sub": "s1",
            "path": ["a"]
        }))
        .unwrap();
        assert_eq!(req.client_id, "c1");
        assert_eq!(req.sub, "s1");
        assert_eq!(req.path, vec!["a".to_string()]);
        // input defaults to Null.
        assert_eq!(req.input, Value::Null);

        // snake_case client_id is rejected (rename means only camelCase maps).
        let res: Result<SubscribeReq, _> = serde_json::from_value(json!({
            "client_id": "c1",
            "sub": "s1",
            "path": ["a"]
        }));
        assert!(res.is_err(), "client_id requires camelCase clientId");
    }

    #[test]
    fn sync_query_camel_case() {
        let q: SyncQuery = serde_json::from_value(json!({ "clientId": "c1" })).unwrap();
        assert_eq!(q.client_id, "c1");

        let res: Result<SyncQuery, _> = serde_json::from_value(json!({ "client_id": "c1" }));
        assert!(res.is_err(), "clientId is required (camelCase)");
    }
}
