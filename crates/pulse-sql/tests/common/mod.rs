//! Shared ephemeral-Postgres harness for the DB-backed `pulse-sql` tests.
//!
//! Each test binary boots ONE throwaway Postgres container via Docker and shares
//! it across that binary's tests; testcontainers' reaper removes it after the
//! process exits. If Docker is unavailable, `pool()` returns `None` so the test
//! skips — the suite stays green on machines without Docker.

use sqlx::PgPool;
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
        // Keep the container alive for the whole test process; the testcontainers
        // reaper cleans it up once the process exits.
        std::mem::forget(node);
        Some(format!("postgres://postgres:postgres@{host}:{port}/postgres"))
    })
    .await
    .as_deref()
}

/// A pool against a fresh ephemeral Postgres, or `None` if Docker is unavailable
/// (in which case the calling test should skip).
pub async fn pool() -> Option<PgPool> {
    let url = url().await?;
    pulse_sql::connect(url, 4).await.ok()
}
