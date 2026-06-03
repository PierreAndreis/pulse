//! Decisive isolation test: does a pooled transaction actually run at
//! SERIALIZABLE, and does a concurrent read-modify-write raise 40001?
//! Runs against an ephemeral Postgres container; skips if Docker is unavailable.

mod common;

use common::pool;
use sqlx::{Executor, Row};

#[tokio::test]
async fn pooled_tx_is_serializable() {
    let Some(pool) = pool().await else {
        eprintln!("skip: no Postgres");
        return;
    };
    let mut tx = pool.begin().await.unwrap();
    let row = sqlx::query("SHOW transaction_isolation")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let level: String = row.get(0);
    std::fs::write("/tmp/iso_level.txt", &level).ok();
    assert_eq!(
        level, "serializable",
        "pooled tx isolation is `{level}`, expected serializable"
    );
}

#[tokio::test]
async fn concurrent_rmw_raises_serialization_failure() {
    let Some(pool) = pool().await else {
        eprintln!("skip: no Postgres");
        return;
    };
    // Self-seed the table so the test does not depend on an externally
    // migrated DB (matches the self-seeding pattern in tests/collab.rs).
    pool.execute(
        "create table if not exists counters (\
            _id uuid primary key,\
            _creation_time bigint not null default 0,\
            name text,\
            value bigint not null default 0)",
    )
    .await
    .unwrap();

    // Fresh row.
    pool.execute(
        "INSERT INTO counters (_id, name, value) VALUES ('00000000-0000-0000-0000-0000000000ff','iso',0)
         ON CONFLICT (_id) DO UPDATE SET value = 0",
    )
    .await
    .unwrap();

    // Two transactions both read then write the same row.
    let mut a = pool.begin().await.unwrap();
    let mut b = pool.begin().await.unwrap();

    let va: i64 =
        sqlx::query("SELECT value FROM counters WHERE _id='00000000-0000-0000-0000-0000000000ff'")
            .fetch_one(&mut *a)
            .await
            .unwrap()
            .get(0);
    let vb: i64 =
        sqlx::query("SELECT value FROM counters WHERE _id='00000000-0000-0000-0000-0000000000ff'")
            .fetch_one(&mut *b)
            .await
            .unwrap()
            .get(0);

    sqlx::query("UPDATE counters SET value=$1 WHERE _id='00000000-0000-0000-0000-0000000000ff'")
        .bind(va + 1)
        .execute(&mut *a)
        .await
        .unwrap();
    a.commit().await.unwrap();

    // B's write now conflicts with A's committed change → must fail at update or commit.
    let upd = sqlx::query(
        "UPDATE counters SET value=$1 WHERE _id='00000000-0000-0000-0000-0000000000ff'",
    )
    .bind(vb + 1)
    .execute(&mut *b)
    .await;
    let commit = if upd.is_ok() {
        b.commit().await.map(|_| ())
    } else {
        Err(upd.err().unwrap())
    };

    let failed = match commit {
        Err(sqlx::Error::Database(db)) => db.code().as_deref() == Some("40001"),
        _ => false,
    };
    std::fs::write("/tmp/iso_conflict.txt", format!("failed={failed}")).ok();
    assert!(
        failed,
        "expected a 40001 serialization failure on the conflicting tx"
    );
}
