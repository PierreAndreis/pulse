//! DB-backed test: does `capture_reads` build a precise filter that prunes a
//! foreign-channel change, when driven by a catalog from the REAL `introspect`?
//!
//! The pure read-set logic is unit-tested in `src/ops.rs` against a hand-built
//! catalog; this test exists to pin the `introspect` boundary — that the column
//! shape `introspect` derives from Postgres feeds `capture_reads` correctly.
//! It runs against an ephemeral Postgres container (see `common`) and seeds its
//! own schema, so it is hermetic and skips entirely if Docker is unavailable.

mod common;

use std::collections::HashMap;

use pulse_core::{Change, ChangeOp, KeyValue, PrimaryKey, ReadSet, TableId};
use pulse_sql::{
    capture_reads, introspect, DbOp, FieldMeta, FilterExpr, PredOp, Predicate, QueryMode,
    SchemaMeta, TableSchema,
};
use serde_json::json;
use sqlx::Executor;
use uuid::Uuid;

fn chat_schema() -> SchemaMeta {
    let mut messages = HashMap::new();
    messages.insert(
        "authorId".to_string(),
        FieldMeta {
            kind: "id".into(),
            ref_table: Some("users".into()),
        },
    );
    messages.insert(
        "channelId".to_string(),
        FieldMeta {
            kind: "id".into(),
            ref_table: Some("channels".into()),
        },
    );
    messages.insert(
        "body".to_string(),
        FieldMeta {
            kind: "string".into(),
            ref_table: None,
        },
    );
    let mut tables = HashMap::new();
    tables.insert("messages".to_string(), TableSchema { fields: messages });
    SchemaMeta { tables }
}

fn change_into(channel: Uuid) -> Change {
    let mut new = HashMap::new();
    new.insert("channelId".to_string(), KeyValue::Uuid(channel));
    Change {
        table: TableId::new("messages"),
        key: PrimaryKey::single(KeyValue::Uuid(Uuid::nil())),
        op: ChangeOp::Insert,
        new: Some(new),
        old: None,
    }
}

/// A pool against the ephemeral container, self-seeded with the chat tables
/// `introspect` expects. Returns `None` (and the test skips) if Docker is down.
/// The schema is created exactly once per binary — `CREATE TABLE IF NOT EXISTS`
/// is not concurrency-safe, so the two tests must not race on it.
async fn seeded_pool() -> Option<sqlx::PgPool> {
    static SEEDED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    let pool = common::pool().await?;
    SEEDED
        .get_or_init(|| async {
            for ddl in [
                "create table if not exists channels (_id uuid primary key, _creation_time bigint not null default 0, name text)",
                "create table if not exists users (_id uuid primary key, _creation_time bigint not null default 0, name text)",
                "create table if not exists messages (\
                    _id uuid primary key default gen_random_uuid(),\
                    _creation_time bigint not null default 0,\
                    channel_id uuid,\
                    author_id uuid,\
                    body text)",
            ] {
                pool.execute(ddl).await.expect("seed ddl");
            }
        })
        .await;
    Some(pool)
}

#[tokio::test]
async fn capture_reads_prunes_foreign_channel() {
    let Some(pool) = seeded_pool().await else {
        eprintln!("skipping: no Postgres");
        return;
    };
    let catalog = introspect(&pool, &chat_schema()).await.expect("introspect");

    let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let b = Uuid::parse_str("00000000-0000-0000-0000-0000000000bb").unwrap();

    // list({channelId: A}) → query with an eq predicate on channelId.
    let op = DbOp::Query {
        table: "messages".to_string(),
        predicates: vec![Predicate {
            field: "channelId".to_string(),
            op: PredOp::Eq,
            value: json!("channels:00000000-0000-0000-0000-000000000001"),
        }],
        filters: vec![],
        order_by: vec![],
        limit: Some(100),
        offset: None,
        aggregate: None,
        group_by: None,
        having: None,
        mode: QueryMode::Take,
    };

    let mut rs = ReadSet::new();
    capture_reads(&op, &catalog, &mut rs);

    // It must be a precise filter, NOT a coarse table read.
    assert!(
        rs.tables.is_empty(),
        "expected no table-wildcard, got {:?}",
        rs.tables
    );
    assert!(
        rs.filters.contains_key(&TableId::new("messages")),
        "expected a messages filter: {rs:?}"
    );

    assert!(
        rs.matches_change(&change_into(a)),
        "same-channel change must match"
    );
    assert!(
        !rs.matches_change(&change_into(b)),
        "foreign-channel change must be pruned: {rs:?}"
    );
}

#[tokio::test]
async fn capture_reads_or_filter_matches_either_branch() {
    let Some(pool) = seeded_pool().await else {
        eprintln!("skipping: no Postgres");
        return;
    };
    let catalog = introspect(&pool, &chat_schema()).await.expect("introspect");

    let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let b = Uuid::parse_str("00000000-0000-0000-0000-0000000000bb").unwrap();
    let c = Uuid::parse_str("00000000-0000-0000-0000-0000000000cc").unwrap();

    // .filter(q => q.or(q.eq(channelId, A), q.eq(channelId, B)))
    let eq_channel = |id: &str| {
        FilterExpr::Cmp(Predicate {
            field: "channelId".to_string(),
            op: PredOp::Eq,
            value: json!(format!("channels:{id}")),
        })
    };
    let op = DbOp::Query {
        table: "messages".to_string(),
        predicates: vec![],
        filters: vec![FilterExpr::Or {
            or: vec![
                eq_channel("00000000-0000-0000-0000-000000000001"),
                eq_channel("00000000-0000-0000-0000-0000000000bb"),
            ],
        }],
        order_by: vec![],
        limit: None,
        offset: None,
        aggregate: None,
        group_by: None,
        having: None,
        mode: QueryMode::Collect,
    };

    let mut rs = ReadSet::new();
    capture_reads(&op, &catalog, &mut rs);

    // Precise OR: two filters, no coarse table read.
    assert!(
        rs.tables.is_empty(),
        "expected precise OR, got table read: {rs:?}"
    );
    assert!(rs.matches_change(&change_into(a)), "channel A must match");
    assert!(rs.matches_change(&change_into(b)), "channel B must match");
    assert!(
        !rs.matches_change(&change_into(c)),
        "channel C must be pruned: {rs:?}"
    );
}

#[tokio::test]
async fn capture_reads_not_filter_negates_precisely() {
    let Some(pool) = seeded_pool().await else {
        eprintln!("skipping: no Postgres");
        return;
    };
    let catalog = introspect(&pool, &chat_schema()).await.expect("introspect");
    let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let b = Uuid::parse_str("00000000-0000-0000-0000-0000000000bb").unwrap();

    // .filter(q => q.not(q.eq(channelId, A))) → channelId <> A (precise via flip).
    let op = DbOp::Query {
        table: "messages".to_string(),
        predicates: vec![],
        filters: vec![FilterExpr::Not {
            not: Box::new(FilterExpr::Cmp(Predicate {
                field: "channelId".to_string(),
                op: PredOp::Eq,
                value: json!("channels:00000000-0000-0000-0000-000000000001"),
            })),
        }],
        order_by: vec![],
        limit: None,
        offset: None,
        aggregate: None,
        group_by: None,
        having: None,
        mode: QueryMode::Collect,
    };
    let mut rs = ReadSet::new();
    capture_reads(&op, &catalog, &mut rs);

    assert!(
        rs.tables.is_empty(),
        "NOT(eq) should flip to a precise neq: {rs:?}"
    );
    assert!(
        !rs.matches_change(&change_into(a)),
        "channel A is excluded by NOT(=A)"
    );
    assert!(
        rs.matches_change(&change_into(b)),
        "channel B matches NOT(=A)"
    );
}

#[tokio::test]
async fn capture_reads_raw_matches_table_insert() {
    let Some(pool) = seeded_pool().await else {
        eprintln!("skipping: no Postgres");
        return;
    };
    let catalog = introspect(&pool, &chat_schema()).await.expect("introspect");

    let op = DbOp::Raw {
        sql: "SELECT count(*) FROM messages WHERE channel_id = $1::uuid".to_string(),
        params: vec![json!("channels:00000000-0000-0000-0000-000000000001")],
    };
    let mut rs = ReadSet::new();
    capture_reads(&op, &catalog, &mut rs);

    assert!(
        rs.tables.contains(&TableId::new("messages")),
        "raw read must cover messages: {:?}",
        rs.tables
    );
    assert!(
        rs.matches_change(&change_into(Uuid::nil())),
        "raw read must match an insert to messages"
    );
}
