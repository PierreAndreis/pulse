//! WAL/CDC consumer against a live Postgres: a raw INSERT made *outside* the
//! engine (direct SQL, no Pulse mutation) is captured from the logical slot and
//! decoded into a `ChangeSet` — the out-of-band-write invalidation the in-engine
//! write-set path is blind to.
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use tokio::sync::OnceCell;
use uuid::Uuid;

use pulse_cdc::wal::{ensure_publication, ensure_slot, poll_change_sets, WalDecoder};
use pulse_core::{ChangeOp, KeyValue, TableId};
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

fn messages_catalog() -> Catalog {
    let cols = vec![
        col("_id", "_id", PgTypeClass::Uuid, Some("messages")),
        col("channel_id", "channelId", PgTypeClass::Uuid, Some("channels")),
        col("body", "body", PgTypeClass::Text, None),
    ];
    let mut c = Catalog::default();
    c.tables
        .insert("messages".into(), Table::from_columns("messages", cols));
    c
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

#[tokio::test]
async fn out_of_band_insert_is_captured_from_the_wal() {
    let Some(url) = url().await else {
        eprintln!("skip: docker unavailable");
        return;
    };
    let pool = PgPool::connect(url).await.unwrap();

    // A minimal `messages` table (REPLICA IDENTITY FULL so updates/deletes carry
    // old images too — used by later slices; harmless here).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS messages (\
            _id uuid PRIMARY KEY, \
            _creation_time bigint NOT NULL DEFAULT 0, \
            channel_id uuid NOT NULL, \
            author_id uuid NOT NULL, \
            body text NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ALTER TABLE messages REPLICA IDENTITY FULL")
        .execute(&pool)
        .await
        .unwrap();

    ensure_publication(&pool, "pulse_pub").await.unwrap();
    ensure_slot(&pool, "pulse_slot").await.unwrap();

    // A raw write — NOT through the engine. This is the change the in-engine
    // write-set capture can never see.
    let id = Uuid::new_v4();
    let channel = Uuid::new_v4();
    sqlx::query("INSERT INTO messages (_id, channel_id, author_id, body) VALUES ($1,$2,$3,$4)")
        .bind(id)
        .bind(channel)
        .bind(Uuid::new_v4())
        .bind("from-psql")
        .execute(&pool)
        .await
        .unwrap();

    let mut decoder = WalDecoder::new(messages_catalog());
    let sets = poll_change_sets(&pool, "pulse_slot", "pulse_pub", &mut decoder)
        .await
        .unwrap();

    // Exactly one committed transaction, one Insert on messages, with the row
    // image the reactor's matcher consumes (ids decoded to raw uuids).
    let changes: Vec<_> = sets.iter().flat_map(|cs| &cs.changes).collect();
    assert_eq!(changes.len(), 1, "expected one captured change, got {changes:?}");
    let c = changes[0];
    assert_eq!(c.table, TableId::new("messages"));
    assert_eq!(c.op, ChangeOp::Insert);
    let new = c.new.as_ref().expect("insert carries a new image");
    assert_eq!(new.get("channelId"), Some(&KeyValue::Uuid(channel)));
    assert_eq!(new.get("body"), Some(&KeyValue::Text("from-psql".into())));

    drop(pool);
}
