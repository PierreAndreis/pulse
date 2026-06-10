//! WAL/CDC consumer against a live Postgres: writes made *outside* the engine
//! (direct SQL, no Pulse mutation) are captured from a logical slot and decoded
//! into `ChangeSet`s — the out-of-band-write invalidation the in-engine
//! write-set path is blind to. Insert (tracer), Update (old-image on a filter
//! move), and Delete (pre-image) are each exercised end-to-end.
use std::time::{Duration, Instant};

use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;
use uuid::Uuid;

use pulse_cdc::wal::{ensure_slot, poll_events, WalDecoder, WalEvent};
use pulse_core::{Change, ChangeOp, KeyValue, TableId};
use pulse_sql::{Catalog, Column, PgTypeClass, Table};

static URL: OnceCell<Option<String>> = OnceCell::const_new();

async fn url() -> Option<&'static str> {
    URL.get_or_init(|| async {
        // `wal_level=logical` is required to create a logical replication slot.
        let node = Postgres::default()
            .with_tag("16-alpine")
            .with_cmd(["postgres", "-c", "wal_level=logical"])
            .start()
            .await
            .ok()?;
        let host = node.get_host().await.ok()?;
        let port = node.get_host_port_ipv4(5432).await.ok()?;
        std::mem::forget(node);
        Some(format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
    })
    .await
    .as_deref()
}

fn col(column: &str, field: &str, type_class: PgTypeClass, id_ref: Option<&str>) -> Column {
    Column {
        column: column.into(),
        field: field.into(),
        type_class,
        nullable: true,
        id_ref: id_ref.map(str::to_string),
    }
}

/// Per-test fixture: a uniquely-named table (so parallel tests don't share a
/// slot's stream), its own `pgoutput` slot + publication, and a matching
/// catalog. `REPLICA IDENTITY FULL` so Update/Delete carry old images.
struct Fixture {
    pool: PgPool,
    table: String,
    slot: String,
    publication: String,
    catalog: Catalog,
}

async fn fixture(tag: &str) -> Fixture {
    let pool = PgPool::connect(url().await.unwrap()).await.unwrap();
    let table = format!("m_{tag}");
    let slot = format!("slot_{tag}");
    let publication = format!("pub_{tag}");

    sqlx::query(&format!(
        "CREATE TABLE {table} (\
            _id uuid PRIMARY KEY, \
            channel_id uuid NOT NULL, \
            body text NOT NULL)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("ALTER TABLE {table} REPLICA IDENTITY FULL"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    ensure_slot(&pool, &slot).await.unwrap();

    let catalog = {
        let cols = vec![
            col("_id", "_id", PgTypeClass::Uuid, Some(&table)),
            col(
                "channel_id",
                "channelId",
                PgTypeClass::Uuid,
                Some("channels"),
            ),
            col("body", "body", PgTypeClass::Text, None),
        ];
        let mut c = Catalog::default();
        c.tables
            .insert(table.clone(), Table::from_columns(&table, cols));
        c
    };

    Fixture {
        pool,
        table,
        slot,
        publication,
        catalog,
    }
}

impl Fixture {
    /// Drain the slot and return every decoded change across the committed sets.
    async fn drain(&self) -> Vec<Change> {
        let mut decoder = WalDecoder::new(self.catalog.clone());
        poll_events(&self.pool, &self.slot, &self.publication, &mut decoder)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|e| match e {
                WalEvent::Changes(cs) => Some(cs.changes),
                WalEvent::Resync(_) => None,
            })
            .flatten()
            .collect()
    }
}

#[tokio::test]
async fn out_of_band_insert_is_captured_from_the_wal() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("insert").await;

    // A raw write — NOT through the engine.
    let (id, channel) = (Uuid::new_v4(), Uuid::new_v4());
    sqlx::query(&format!(
        "INSERT INTO {} (_id, channel_id, body) VALUES ($1,$2,$3)",
        fx.table
    ))
    .bind(id)
    .bind(channel)
    .bind("from-psql")
    .execute(&fx.pool)
    .await
    .unwrap();

    let changes = fx.drain().await;
    assert_eq!(changes.len(), 1, "expected one change, got {changes:?}");
    let c = &changes[0];
    assert_eq!(c.table, TableId::new(fx.table.clone()));
    assert_eq!(c.op, ChangeOp::Insert);
    let new = c.new.as_ref().expect("insert carries a new image");
    // id_ref columns decode to raw uuids; text passes through — identical to the
    // in-engine row_to_values image, so the same matcher fires.
    assert_eq!(new.get("channelId"), Some(&KeyValue::Uuid(channel)));
    assert_eq!(new.get("body"), Some(&KeyValue::Text("from-psql".into())));
    assert!(c.old.is_none());
}

#[tokio::test]
async fn out_of_band_update_carries_both_images() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("update").await;
    let (id, chan_a, chan_b) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    sqlx::query(&format!(
        "INSERT INTO {} (_id, channel_id, body) VALUES ($1,$2,'x')",
        fx.table
    ))
    .bind(id)
    .bind(chan_a)
    .execute(&fx.pool)
    .await
    .unwrap();
    // Move the row across the channel filter (A → B): the old image is what lets
    // a channel-A subscription drop the row, so it MUST be captured.
    sqlx::query(&format!(
        "UPDATE {} SET channel_id = $1 WHERE _id = $2",
        fx.table
    ))
    .bind(chan_b)
    .bind(id)
    .execute(&fx.pool)
    .await
    .unwrap();

    let changes = fx.drain().await;
    let upd = changes
        .iter()
        .find(|c| c.op == ChangeOp::Update)
        .expect("an update change");
    assert_eq!(
        upd.old.as_ref().and_then(|r| r.get("channelId")),
        Some(&KeyValue::Uuid(chan_a)),
        "pre-image must carry the old channel"
    );
    assert_eq!(
        upd.new.as_ref().and_then(|r| r.get("channelId")),
        Some(&KeyValue::Uuid(chan_b)),
        "post-image must carry the new channel"
    );
}

#[tokio::test]
async fn out_of_band_delete_carries_the_pre_image() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("delete").await;
    let (id, channel) = (Uuid::new_v4(), Uuid::new_v4());
    sqlx::query(&format!(
        "INSERT INTO {} (_id, channel_id, body) VALUES ($1,$2,'doomed')",
        fx.table
    ))
    .bind(id)
    .bind(channel)
    .execute(&fx.pool)
    .await
    .unwrap();
    sqlx::query(&format!("DELETE FROM {} WHERE _id = $1", fx.table))
        .bind(id)
        .execute(&fx.pool)
        .await
        .unwrap();

    let changes = fx.drain().await;
    let del = changes
        .iter()
        .find(|c| c.op == ChangeOp::Delete)
        .expect("a delete change");
    assert!(del.new.is_none(), "delete has no post-image");
    assert_eq!(
        del.old.as_ref().and_then(|r| r.get("channelId")),
        Some(&KeyValue::Uuid(channel)),
        "delete pre-image must carry the leaving row's filter columns"
    );
}

#[tokio::test]
async fn wal_consumer_streams_out_of_band_inserts() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("stream").await;
    let mut rx = pulse_cdc::wal::start_wal_consumer(
        fx.pool.clone(),
        fx.slot.clone(),
        fx.publication.clone(),
        fx.catalog.clone(),
        Duration::from_millis(20),
    );

    let (id, channel) = (Uuid::new_v4(), Uuid::new_v4());
    sqlx::query(&format!(
        "INSERT INTO {} (_id, channel_id, body) VALUES ($1,$2,'streamed')",
        fx.table
    ))
    .bind(id)
    .bind(channel)
    .execute(&fx.pool)
    .await
    .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("an event arrives within the poll window")
        .expect("consumer channel open");
    let WalEvent::Changes(cs) = ev else {
        panic!("expected a Changes event, got {ev:?}");
    };
    let c = &cs.changes[0];
    assert_eq!(c.op, ChangeOp::Insert);
    assert_eq!(
        c.new.as_ref().and_then(|r| r.get("channelId")),
        Some(&KeyValue::Uuid(channel))
    );
}

#[tokio::test]
async fn out_of_band_truncate_emits_a_resync() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("truncate").await;
    // Reuse one decoder so the relation oid (learned from the insert) is cached
    // when the Truncate message arrives — exactly how the real consumer runs.
    let mut decoder = WalDecoder::new(fx.catalog.clone());

    sqlx::query(&format!(
        "INSERT INTO {} (_id, channel_id, body) VALUES ($1,$2,'x')",
        fx.table
    ))
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(&fx.pool)
    .await
    .unwrap();
    // First drain caches the relation (and yields the insert).
    let _ = poll_events(&fx.pool, &fx.slot, &fx.publication, &mut decoder)
        .await
        .unwrap();

    // A raw TRUNCATE — invisible to the engine's write-set capture, and with no
    // per-row image, so it must surface as a coarse resync of the table.
    sqlx::query(&format!("TRUNCATE {}", fx.table))
        .execute(&fx.pool)
        .await
        .unwrap();
    let events = poll_events(&fx.pool, &fx.slot, &fx.publication, &mut decoder)
        .await
        .unwrap();

    assert!(
        events
            .iter()
            .any(|e| matches!(e, WalEvent::Resync(tables) if tables.contains(&TableId::new(fx.table.clone())))),
        "truncate must emit a Resync for the table, got {events:?}"
    );
}

#[tokio::test]
async fn wal_leadership_is_single_holder_with_failover() {
    let Some(url) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let key = 0x7012_3457; // unique to this test
    let a = pulse_cdc::wal::try_acquire_leadership(url, key)
        .await
        .unwrap();
    assert!(a.is_some(), "first acquirer becomes the WAL consumer");
    let b = pulse_cdc::wal::try_acquire_leadership(url, key)
        .await
        .unwrap();
    assert!(b.is_none(), "a second node is locked out of the slot");

    drop(a); // closes the connection → Postgres releases the session lock

    // Failover: another node can take over once the holder drops (small delay
    // while Postgres notices the disconnect).
    let mut took_over = None;
    for _ in 0..100 {
        if let Some(g) = pulse_cdc::wal::try_acquire_leadership(url, key)
            .await
            .unwrap()
        {
            took_over = Some(g);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        took_over.is_some(),
        "leadership fails over after the holder drops"
    );
}

// ── Mode A vs Mode B benchmark ───────────────────────────────────────────────
// Mode A routes EVERY invalidation through the WAL slot (polled), so its added
// latency over Mode B (synchronous in-engine apply, ~0 added) is exactly the
// WAL-visibility latency measured here. We also measure the cost Mode A pays
// continuously — the empty-poll round-trip — and the decode throughput.
//
//   cargo test -p pulse-cdc --test wal -- --ignored --nocapture bench

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let n = samples.len();
    let us = |d: Duration| d.as_secs_f64() * 1e3; // → ms
    let at = |p: f64| us(samples[((n as f64 * p) as usize).min(n - 1)]);
    let mean = us(samples.iter().sum::<Duration>() / n as u32);
    println!(
        "{label:<34} n={n:<4} min={:>7.3} p50={:>7.3} p95={:>7.3} p99={:>7.3} max={:>7.3} mean={:>7.3}  (ms)",
        us(samples[0]),
        at(0.50),
        at(0.95),
        at(0.99),
        us(samples[n - 1]),
        mean,
    );
}

#[tokio::test]
#[ignore = "benchmark; run with --ignored --nocapture bench"]
async fn bench_wal_invalidation_latency() {
    let Some(_) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let fx = fixture("bench").await;
    let insert = |body: &'static str| {
        let pool = fx.pool.clone();
        let table = fx.table.clone();
        async move {
            sqlx::query(&format!(
                "INSERT INTO {table} (_id, channel_id, body) VALUES ($1,$2,$3)"
            ))
            .bind(Uuid::new_v4())
            .bind(Uuid::new_v4())
            .bind(body)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    // Warm up: slot creation + first decode context.
    insert("warmup").await;
    while fx.drain().await.is_empty() {}

    const N: usize = 300;

    // (1) Mode A floor: commit → change visible via a tight poll loop. This is
    // the minimum latency WAL routing adds over Mode B's synchronous apply.
    let mut tight = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        insert("x").await;
        loop {
            if !fx.drain().await.is_empty() {
                break;
            }
        }
        tight.push(t0.elapsed());
    }
    report("Mode A floor: commit→visible", tight);

    // (2) Empty-poll cost: what Mode A pays every poll while idle (no changes).
    let mut empty = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        let got = fx.drain().await;
        empty.push(t0.elapsed());
        assert!(got.is_empty());
    }
    report("Mode A idle: empty poll", empty);

    // (3) Decode throughput: drain a batch of B changes in one poll, per-change
    // CPU cost of the pgoutput decode + RowValues build.
    const BATCH: usize = 500;
    for _ in 0..BATCH {
        insert("batch").await;
    }
    let t0 = Instant::now();
    let drained = fx.drain().await;
    let elapsed = t0.elapsed();
    let per_ns = elapsed.as_nanos() as f64 / drained.len().max(1) as f64;
    println!(
        "decode+drain: {} changes in {:.3} ms ({:.0} ns/change)",
        drained.len(),
        elapsed.as_secs_f64() * 1e3,
        per_ns,
    );
    println!(
        "\nMode B adds ~0 invalidation latency (synchronous in-engine apply) but \
         must dedup the WAL echo; Mode A's added latency is the 'commit→visible' \
         row above, paid uniformly for in-engine and out-of-band writes."
    );
}
