//! Shared ephemeral-Postgres harness for the `pulse-jsruntime` worker integration
//! tests. One throwaway Postgres container is booted for the whole test binary
//! (via testcontainers; the reaper removes it on exit), but each test gets its own
//! connection pool so parallel tests don't starve a shared pool while mutations
//! hold connections across the worker round-trip. If Docker is unavailable, `pool()`
//! returns `None` so the calling test skips and the suite stays green.

use sqlx::{Executor, PgPool};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;

static URL: OnceCell<Option<String>> = OnceCell::const_new();

async fn url() -> Option<&'static str> {
    URL.get_or_init(|| async {
        let node = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .ok()?;
        let host = node.get_host().await.ok()?;
        let port = node.get_host_port_ipv4(5432).await.ok()?;
        // Keep the container alive for the whole process; the reaper cleans it up.
        std::mem::forget(node);
        Some(format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
    })
    .await
    .as_deref()
}

/// A fresh pool against the shared ephemeral Postgres, with the test schema seeded
/// (once per process). `None` if Docker is unavailable.
pub async fn pool() -> Option<PgPool> {
    let url = url().await?;
    let pool = pulse_sql::connect(url, 8).await.ok()?;
    seed(&pool).await;
    Some(pool)
}

/// Create the `items` table the handlers read/write and the `_pulse_mutations`
/// idempotency table the mutation path records into. Seeded once per process —
/// `CREATE TABLE IF NOT EXISTS` is not concurrency-safe, so the guard prevents a
/// race between parallel tests.
async fn seed(pool: &PgPool) {
    static SEEDED: OnceCell<()> = OnceCell::const_new();
    SEEDED
        .get_or_init(|| async {
            for ddl in [
                "create table if not exists items (\
                    _id uuid primary key default gen_random_uuid(),\
                    _creation_time bigint not null default 0,\
                    name text)",
                "create table if not exists _pulse_mutations (id text primary key, result jsonb not null)",
            ] {
                pool.execute(ddl).await.expect("seed ddl");
            }
        })
        .await;
}

/// Count rows in `items` with a given name — lets each test assert on its own
/// uniquely-named rows without serializing against the shared DB.
pub async fn items_named(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*) from items where name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("count items")
}
