//! WAL/CDC consumer against a live Postgres: writes made *outside* the engine
//! (direct SQL, no Pulse mutation) are captured from a logical slot and decoded
//! into `ChangeSet`s — the out-of-band-write invalidation the in-engine
//! write-set path is blind to. Insert (tracer), Update (old-image on a filter
//! move), and Delete (pre-image) are each exercised end-to-end.
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;
use uuid::Uuid;

use pulse_cdc::wal::{ensure_slot, poll_change_sets, WalDecoder};
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
        Some(format!("postgres://postgres:postgres@{host}:{port}/postgres"))
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
            col("channel_id", "channelId", PgTypeClass::Uuid, Some("channels")),
            col("body", "body", PgTypeClass::Text, None),
        ];
        let mut c = Catalog::default();
        c.tables.insert(table.clone(), Table::from_columns(&table, cols));
        c
    };

    Fixture { pool, table, slot, publication, catalog }
}

impl Fixture {
    /// Drain the slot and return every decoded change across the committed sets.
    async fn drain(&self) -> Vec<Change> {
        let mut decoder = WalDecoder::new(self.catalog.clone());
        let sets = poll_change_sets(&self.pool, &self.slot, &self.publication, &mut decoder)
            .await
            .unwrap();
        sets.into_iter().flat_map(|cs| cs.changes).collect()
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
    sqlx::query(&format!("UPDATE {} SET channel_id = $1 WHERE _id = $2", fx.table))
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
