//! Interest-registry behavior against a live Postgres (the bus routing table):
//! registration, interested-node lookup, TTL filtering, prune, and clear.
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;

use pulse_cdc::{
    clear_interest, ensure_interest_table, interested_nodes, prune_interest, register_interest,
};

static URL: OnceCell<Option<String>> = OnceCell::const_new();

async fn pool() -> Option<PgPool> {
    let url = URL
        .get_or_init(|| async {
            let node = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .ok()?;
            let host = node.get_host().await.ok()?;
            let port = node.get_host_port_ipv4(5432).await.ok()?;
            std::mem::forget(node); // reaper cleans up at process exit
            Some(format!(
                "postgres://postgres:postgres@{host}:{port}/postgres"
            ))
        })
        .await
        .as_deref()?;
    PgPool::connect(url).await.ok()
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

#[tokio::test]
async fn registry_routes_by_table_with_ttl_and_prune() {
    let Some(pool) = pool().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    ensure_interest_table(&pool).await.unwrap();
    ensure_interest_table(&pool).await.unwrap(); // idempotent
                                                 // Isolate from other tests sharing the container.
    clear_interest(&pool, "na").await.unwrap();
    clear_interest(&pool, "nb").await.unwrap();
    clear_interest(&pool, "nc").await.unwrap();

    register_interest(&pool, "na", &s(&["t1", "t2"]))
        .await
        .unwrap();
    register_interest(&pool, "nb", &s(&["t2"])).await.unwrap();

    // Only na watches t1; both na and nb watch t2; nobody watches t3.
    let mut on_t1 = interested_nodes(&pool, &s(&["t1"]), "other", 60)
        .await
        .unwrap();
    on_t1.sort();
    assert_eq!(on_t1, vec!["na"]);

    let mut on_t2 = interested_nodes(&pool, &s(&["t2"]), "other", 60)
        .await
        .unwrap();
    on_t2.sort();
    assert_eq!(on_t2, vec!["na", "nb"]);

    assert!(interested_nodes(&pool, &s(&["t3"]), "other", 60)
        .await
        .unwrap()
        .is_empty());

    // A node is never routed its own change.
    let excl = interested_nodes(&pool, &s(&["t2"]), "na", 60)
        .await
        .unwrap();
    assert_eq!(excl, vec!["nb"]);

    // TTL=0 means "nothing is fresh enough" → no interested nodes.
    assert!(interested_nodes(&pool, &s(&["t2"]), "other", 0)
        .await
        .unwrap()
        .is_empty());

    // Prune with a huge TTL keeps everything; prune with 0 removes all.
    assert_eq!(prune_interest(&pool, 3600).await.unwrap(), 0);
    let removed = prune_interest(&pool, 0).await.unwrap();
    assert!(removed >= 3, "prune(0) removes all rows, got {removed}");
    assert!(interested_nodes(&pool, &s(&["t2"]), "other", 60)
        .await
        .unwrap()
        .is_empty());
}
