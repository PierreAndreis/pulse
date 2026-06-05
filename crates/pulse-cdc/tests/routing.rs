//! End-to-end bus routing against a live Postgres: a published change reaches a
//! node interested in the touched table, and does NOT wake a node that isn't.
use std::time::Duration;

use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;

use pulse_cdc::{ensure_interest_table, publish, register_interest, start_listener, BusEvent};
use pulse_core::{Change, ChangeOp, ChangeSet, KeyValue, Lsn, PrimaryKey, TableId};

static URL: OnceCell<Option<String>> = OnceCell::const_new();

async fn url() -> Option<&'static str> {
    URL.get_or_init(|| async {
        let node = Postgres::default().with_tag("16-alpine").start().await.ok()?;
        let host = node.get_host().await.ok()?;
        let port = node.get_host_port_ipv4(5432).await.ok()?;
        std::mem::forget(node);
        Some(format!("postgres://postgres:postgres@{host}:{port}/postgres"))
    })
    .await
    .as_deref()
}

fn change_on(table: &str) -> ChangeSet {
    let mut cs = ChangeSet::new("0/1000".parse::<Lsn>().unwrap());
    cs.push(Change::point(
        TableId::new(table),
        PrimaryKey::single(KeyValue::Int(1)),
        ChangeOp::Insert,
    ));
    cs
}

#[tokio::test]
async fn publish_wakes_interested_nodes_only() {
    let Some(url) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let pool = PgPool::connect(url).await.unwrap();
    ensure_interest_table(&pool).await.unwrap();

    // Node B listens and registers interest in `widgets` only.
    let mut rx = start_listener(url, "B".into()).await.unwrap();
    register_interest(&pool, "B", &["widgets".to_string()]).await.unwrap();
    // Give LISTEN a moment to be established before the first NOTIFY.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A change to `widgets` from node A is routed to B (interested).
    publish(&pool, "A", &change_on("widgets")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(
        matches!(got, Ok(Some(BusEvent::Changes(_)))),
        "an interested node must receive the routed change, got {got:?}"
    );

    // A change to `authors` (B has no interest) must NOT wake B.
    publish(&pool, "A", &change_on("authors")).await.unwrap();
    let none = tokio::time::timeout(Duration::from_millis(800), rx.recv()).await;
    assert!(
        none.is_err(),
        "an uninterested node must not be woken by a foreign-table change"
    );
}
