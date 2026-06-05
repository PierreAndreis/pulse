//! `current_wal_lsn` against a live Postgres: the stamped commit LSN must advance
//! on writes and never regress — the ordering property the reactor watermark relies on.
mod common;

use sqlx::Executor;

#[tokio::test]
async fn wal_lsn_advances_on_write_and_never_regresses() {
    let Some(pool) = common::pool().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let mut conn = pool.acquire().await.unwrap();
    conn.execute("create table if not exists wal_probe (id int)")
        .await
        .unwrap();

    let before = pulse_sql::current_wal_lsn(&mut conn).await.unwrap();
    for i in 0..50 {
        conn.execute(format!("insert into wal_probe values ({i})").as_str())
            .await
            .unwrap();
    }
    let after = pulse_sql::current_wal_lsn(&mut conn).await.unwrap();
    assert!(
        after > before,
        "WAL LSN must advance after committed writes: {before} -> {after}"
    );

    // Monotonic: a later read is never below an earlier one.
    let again = pulse_sql::current_wal_lsn(&mut conn).await.unwrap();
    assert!(again >= after, "WAL LSN regressed: {after} -> {again}");
}
