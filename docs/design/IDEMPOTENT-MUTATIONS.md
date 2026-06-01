# Exactly-once mutations (idempotency keys)

## Bug

Two browser tabs share one origin → one IndexedDB offline queue
(`pulse:mutation-queue`). Each tab's `LocalFirst`/`OfflineQueue` flushes
**independently**, and `rpcCall` carried only `{ path, input }` — no idempotency
key. So the same queued write is delivered twice:

- A fresh tab reads another tab's queued write on startup and flushes it, then
  the originating tab also flushes it on reconnect → duplicate.
- Even a single tab double-delivers if the network ack is lost *after* the server
  committed (retry-after-commit).

## Fix

Each queued mutation carries a stable UUID. The engine records applied mutation
ids in `_pulse_mutations` **inside the same SERIALIZABLE transaction** as the
mutation and dedupes. Exactly-once across tabs AND across retries.

## Client side — DONE

- `localfirst.ts` `nextId()` → `crypto.randomUUID()` (globally unique across
  tabs; a per-tab counter could collide). Removed the old `seq`/`SEQ_KEY`.
- `transport.ts` `rpcCall(options, path, input, mutationId?)` → includes
  `mutationId` in the body when present. (Backward-compatible: the current
  server ignores unknown JSON fields via serde.)
- `localfirst.ts` `flush()` passes `m.id` as the idempotency key.

## Server side — TO APPLY (Rust)

### 1. Migration — create the dedup table at startup

In `crates/pulse-server/src/main.rs` `build_state`, right after the OLTP pool is
opened (`let pool = pulse_sql::connect(db_url, 10).await?;`) and before
`introspect`:

```rust
sqlx::query(
    "CREATE TABLE IF NOT EXISTS _pulse_mutations (
        id text PRIMARY KEY,
        result jsonb NOT NULL DEFAULT 'null'::jsonb,
        created_at timestamptz NOT NULL DEFAULT now()
    )",
)
.execute(&pool)
.await?;
```

### 2. Carry `mutationId` on the wire

`RpcRequest`:

```rust
#[derive(Deserialize)]
struct RpcRequest {
    path: Vec<String>,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default, rename = "mutationId")]
    mutation_id: Option<String>,
}
```

`rpc_handler`:

```rust
let result = state
    .worker
    .run_procedure(request_id, req.path, req.input, kind, req.mutation_id)
    .await;
```

### 3. `run_procedure` — pre-check + DUPLICATE handling

Signature gains `mutation_id: Option<String>`. Before the retry loop, fast-path
for an already-applied mutation (skips the handler entirely):

```rust
if is_mutation {
    if let Some(id) = mutation_id.as_deref() {
        if let Some(cached) = self.lookup_mutation(id).await {
            return Ok(cached);
        }
    }
}
```

Inside the loop, thread `mutation_id.clone()` into `run_once`, and add a
`DUPLICATE` arm (a concurrent delivery won the unique insert):

```rust
Err(e) if e == "DUPLICATE" => {
    if let Some(id) = mutation_id.as_deref() {
        if let Some(cached) = self.lookup_mutation(id).await {
            return Ok(cached);
        }
    }
    return Ok(serde_json::Value::Null);
}
```

Helper on `Worker`/`Inner` (uses the OLTP pool, autocommit):

```rust
async fn lookup_mutation(&self, id: &str) -> Option<serde_json::Value> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT result::text FROM _pulse_mutations WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)   // self.inner.pool depending on receiver
            .await
            .ok()
            .flatten();
    row.and_then(|(s,)| serde_json::from_str(&s).ok())
}
```

### 4. `run_once` — pass id + result through to the tx task

Signature gains `mutation_id: Option<String>`. Spawn the tx task with it:

```rust
let task = tokio::spawn(transaction_task(
    Arc::clone(self),
    request_id.clone(),
    tx_rx,
    mutation_id.clone(),
));
```

When finishing, send the handler result so the tx task can persist it in-tx:

```rust
let commit = handler_result.is_ok();
let result = handler_result.as_ref().ok().cloned();
let _ = tx_tx.send(TxMsg::Finish { commit, result, done: done_tx }).await;
```

### 5. `TxMsg::Finish` — carry the result

```rust
Finish {
    commit: bool,
    result: Option<serde_json::Value>,
    done: oneshot::Sender<Result<(), String>>,
},
```

### 6. `transaction_task` — record the id in the SAME tx, dedupe on conflict

Signature gains `mutation_id: Option<String>`. In the `Finish` arm, before
committing, insert the idempotency row; a unique violation means another
delivery already committed this id → roll back and report `DUPLICATE`:

```rust
TxMsg::Finish { commit, result, done } => {
    let outcome = if commit && !serialization_failed {
        let mut duplicate = false;
        if let Some(id) = &mutation_id {
            let res_json = result.unwrap_or(serde_json::Value::Null).to_string();
            match sqlx::query(
                "INSERT INTO _pulse_mutations (id, result) VALUES ($1, $2::jsonb)",
            )
            .bind(id)
            .bind(&res_json)
            .execute(&mut *tx)
            .await
            {
                Ok(_) => {}
                Err(e) if is_unique_violation(&e) => duplicate = true,
                Err(e) if is_serialization_error(&e) => serialization_failed = true,
                Err(e) => {
                    tracing::error!(target: "pulse::worker", "idempotency insert failed: {e}");
                    let _ = tx.rollback().await;
                    let _ = done.send(Err(format!("idempotency insert failed: {e}")));
                    break;
                }
            }
        }
        if duplicate {
            let _ = tx.rollback().await;
            Err("DUPLICATE".to_string())
        } else if serialization_failed {
            let _ = tx.rollback().await;
            Err("SERIALIZATION_FAILURE".to_string())
        } else {
            match tx.commit().await {
                Ok(()) => Ok(()),
                Err(e) if is_serialization_error(&e) => Err("SERIALIZATION_FAILURE".into()),
                Err(e) => Err(format!("commit failed: {e}")),
            }
        }
    } else {
        let _ = tx.rollback().await;
        if serialization_failed { Err("SERIALIZATION_FAILURE".into()) } else { Err("rolled back".into()) }
    };
    let _ = done.send(outcome);
    break;
}
```

Helper:

```rust
fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e.as_database_error().and_then(|d| d.code()), Some(c) if c == "23505")
}
```

### Concurrency note

The unique index on `_pulse_mutations.id` serializes concurrent duplicates: the
first tx to insert wins; the second blocks on the key until the first commits,
then gets `23505` → rolls back → `DUPLICATE` → returns the winner's cached
result. Sequential duplicates are caught earlier by `lookup_mutation`.

## Test (integration)

`tests/integration/idempotency.test.ts`:

1. POST `/rpc` `messages.send` twice with the SAME `mutationId`.
2. Assert `messages.list` grew by exactly **one**.
3. Assert both responses return the same result.
4. Two-tab browser repro: both offline → send in A → open B → both online →
   exactly one message.
```
```
