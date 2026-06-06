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

use pulse_core::{ChangeSet, Lsn, ReadSet, TableId};
use pulse_jsruntime::{Worker, WorkerConfig, WorkerError};
use pulse_reactor::{InMemoryReactor, InterestSink, ReExecutor, Reactor, Subscription};

struct AppState {
    worker: Arc<Worker>,
    reactor: Arc<dyn Reactor>,
    /// OLTP pool, used to publish change-sets to the cross-node bus.
    pool: pulse_sql::PgPool,
    /// This node's id — bus messages it originates are skipped on receipt.
    node_id: String,
    /// Latest sampled WAL position, refreshed by a background task. Mutations stamp
    /// their change-set's commit watermark from this instead of querying the WAL on
    /// every write — WAL only advances, so the watermark stays monotonic.
    wal_lsn: Arc<std::sync::atomic::AtomicU64>,
    /// Force global broadcast instead of interest routing (PULSE_BUS_BROADCAST) —
    /// the benchmark baseline and a routing kill-switch.
    broadcast: bool,
    /// Chunk oversized change-sets into precise messages instead of degrading to a
    /// coarse resync (PULSE_BUS_CHUNK; default on).
    chunk: bool,
    /// Interest freshness window (PULSE_INTEREST_TTL_SECS) used for routing.
    ttl_secs: i64,
    /// Cross-node bus metrics (events + latency decomposition), exposed at `/metrics`.
    metrics: Arc<BusMetrics>,
}

/// Counters for the cross-node "network layer": how many bus events arrived, and a
/// latency decomposition — commit→deliver (bus propagation) vs deliver→applied
/// (remote re-exec). Sums in µs; the benchmark divides by the counts for averages.
#[derive(Default)]
struct BusMetrics {
    /// All bus events received (the network-layer work the fan-out bench counts).
    events: std::sync::atomic::AtomicU64,
    /// Concrete `Changes` events (those carrying a publish timestamp → a lag sample).
    changes: std::sync::atomic::AtomicU64,
    /// Σ commit→deliver propagation latency over `changes` events (µs).
    lag_us_sum: std::sync::atomic::AtomicU64,
    /// Σ deliver→applied (match + re-exec + push) duration over all events (µs).
    apply_us_sum: std::sync::atomic::AtomicU64,
    /// Coarse resync events received (ResyncTables/Resync) — a change-set that blew
    /// past the NOTIFY payload cap and degraded to table-/global-scoped re-eval.
    resyncs: std::sync::atomic::AtomicU64,
}

/// Microseconds since the Unix epoch (wall clock), for same-host bus-latency math.
fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
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
    ) -> Result<(Value, ReadSet), String> {
        self.worker
            .execute(path, input, headers, None)
            .await
            .map(|res| (res.value, res.read_set))
            .map_err(|e| e.code)
    }
}

/// Registers this node's table interest on the change bus when the reactor begins
/// watching a new table — so a publisher routes the table's changes to this node.
struct BusInterestSink {
    pool: pulse_sql::PgPool,
    node_id: String,
}

#[async_trait]
impl InterestSink for BusInterestSink {
    async fn on_tables_added(&self, tables: Vec<TableId>) {
        let names: Vec<String> = tables.into_iter().map(|t| t.0).collect();
        if let Err(e) = pulse_cdc::register_interest(&self.pool, &self.node_id, &names).await {
            tracing::warn!("interest register failed: {e}");
        }
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
    let mut heartbeat_ms = num_env("PULSE_HEARTBEAT_MS", 10_000);
    // The heartbeat must refresh interest well within the TTL, or a live node's
    // interest lapses and it silently misses invalidations — clamp to TTL/3.
    let max_heartbeat = (interest_ttl_secs.max(1) as u64) * 1000 / 3;
    if heartbeat_ms > max_heartbeat {
        tracing::warn!("PULSE_HEARTBEAT_MS {heartbeat_ms} > ttl/3; clamping to {max_heartbeat}");
        heartbeat_ms = max_heartbeat.max(1);
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
    let interest_sink = Arc::new(BusInterestSink {
        pool: pool.clone(),
        node_id: node_id.clone(),
    });
    let reactor: Arc<dyn Reactor> = Arc::new(
        InMemoryReactor::new(reexec)
            .with_interest(interest_sink)
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

    // Heartbeat: keep this node's interest rows fresh and prune dead nodes'. The
    // interval must stay well under INTEREST_TTL_SECS so interest never lapses.
    {
        let pool = pool.clone();
        let node_id = node_id.clone();
        let reactor = reactor.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(heartbeat_ms));
            loop {
                tick.tick().await;
                let tables: Vec<String> = reactor
                    .interest_tables()
                    .await
                    .into_iter()
                    .map(|t| t.0)
                    .collect();
                let _ = pulse_cdc::register_interest(&pool, &node_id, &tables).await;
                let _ = pulse_cdc::prune_interest(&pool, interest_ttl_secs).await;
            }
        });
    }

    let broadcast = env_or("PULSE_BUS_BROADCAST", "0") == "1";
    let chunk = env_or("PULSE_BUS_CHUNK", "1") == "1";
    let metrics = Arc::new(BusMetrics::default());
    match pulse_cdc::start_listener(&database_url, node_id.clone()).await {
        Ok(mut rx) => {
            let reactor = reactor.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                use std::sync::atomic::Ordering::Relaxed;
                while let Some(event) = rx.recv().await {
                    // Each event crossed the network to this node — the unit of
                    // cross-node "network layer" work. Decompose its latency:
                    // commit→deliver (bus propagation) and deliver→applied (re-exec).
                    metrics.events.fetch_add(1, Relaxed);
                    let started = std::time::Instant::now();
                    match event {
                        pulse_cdc::BusEvent::Changes { cs, origin_us } => {
                            let lag = (now_us() - origin_us).max(0) as u64;
                            metrics.lag_us_sum.fetch_add(lag, Relaxed);
                            metrics.changes.fetch_add(1, Relaxed);
                            reactor.apply_change_set(cs).await;
                        }
                        pulse_cdc::BusEvent::ResyncTables(tables) => {
                            metrics.resyncs.fetch_add(1, Relaxed);
                            let ts = tables.into_iter().map(TableId::new).collect();
                            reactor.invalidate_tables(ts).await;
                        }
                        pulse_cdc::BusEvent::Resync => {
                            metrics.resyncs.fetch_add(1, Relaxed);
                            reactor.invalidate_all().await;
                        }
                    }
                    metrics
                        .apply_us_sum
                        .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
                }
            });
            tracing::info!("change bus listener started (node {node_id}, broadcast={broadcast})");
        }
        Err(e) => tracing::error!("failed to start change bus listener: {e}"),
    }

    let state = Arc::new(AppState {
        worker,
        reactor,
        pool: pool.clone(),
        node_id,
        wal_lsn,
        broadcast,
        chunk,
        ttl_secs: interest_ttl_secs,
        metrics,
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
    let m = &state.metrics;
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

    match state
        .worker
        .execute(
            req.path,
            req.input,
            collect_headers(&headers),
            req.mutation_id,
        )
        .await
    {
        Ok(res) => {
            if !res.changes.is_empty() {
                // Propagate off the RPC's critical path: capture the commit LSN
                // (after the tx already committed, so it never lengthens the
                // SERIALIZABLE transaction), then apply locally and publish to the
                // bus. The LSN is a monotonic watermark, so a post-commit read of
                // the WAL position is sufficient.
                // Stamp the watermark from the sampled WAL position — no per-write
                // WAL round-trip. Local apply and the cross-node publish then run
                // off the RPC's critical path.
                let commit_lsn = Lsn(state.wal_lsn.load(std::sync::atomic::Ordering::Relaxed));
                let reactor = state.reactor.clone();
                let pool = state.pool.clone();
                let node_id = state.node_id.clone();
                let opts = pulse_cdc::PublishOpts {
                    broadcast: state.broadcast,
                    chunk: state.chunk,
                    ttl_secs: state.ttl_secs,
                };
                let change_set = ChangeSet {
                    commit_lsn,
                    changes: res.changes,
                };
                tokio::spawn(async move {
                    // Local apply (this node's subscribers) in parallel with the
                    // routed cross-node publish; the latter needs one pooled conn.
                    let local = {
                        let reactor = reactor.clone();
                        let cs = change_set.clone();
                        tokio::spawn(async move { reactor.apply_change_set(cs).await })
                    };
                    if let Ok(mut conn) = pool.acquire().await {
                        if let Err(e) =
                            pulse_cdc::publish_on(&mut conn, &node_id, &change_set, opts).await
                        {
                            tracing::warn!("change bus publish failed: {e}");
                        }
                    }
                    let _ = local.await;
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
    match state
        .worker
        .execute(req.path.clone(), req.input.clone(), hdrs.clone(), None)
        .await
    {
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
                    last_lsn: Lsn::ZERO,
                })
                .await;
            // Initial push reflects no committed change yet → LSN zero.
            state
                .reactor
                .push(&req.client_id, &req.sub, &res.value, Lsn::ZERO)
                .await;
            // Catch-up: interest is now registered (add_subscription announced it),
            // so re-evaluate once to pick up any remote write that landed between the
            // initial query snapshot and interest registration. A dedup'd no-op when
            // nothing was missed (the common case).
            state
                .reactor
                .refresh_subscription(&req.client_id, &req.sub)
                .await;
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
    state
        .reactor
        .remove_subscription(&req.client_id, &req.sub)
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
        let err = WorkerError {
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
        let err = WorkerError {
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
