//! DB-backed test: does `capture_reads` build a precise filter that prunes a
//! foreign-channel change? Requires the dev Postgres (skips if unreachable).

use std::collections::HashMap;

use pulse_core::{Change, ChangeOp, KeyValue, PrimaryKey, ReadSet, TableId};
use pulse_sql::{
    capture_reads, connect, introspect, DbOp, FieldMeta, Order, PredOp, Predicate, QueryMode,
    SchemaMeta, TableSchema,
};
use serde_json::json;
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

#[tokio::test]
async fn capture_reads_prunes_foreign_channel() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pulse:pulse@localhost:54329/pulse".to_string());
    let Ok(pool) = connect(&url, 2).await else {
        eprintln!("skipping: no Postgres at {url}");
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
        order: Some(Order::Desc),
        limit: Some(100),
        mode: QueryMode::Take,
    };

    let mut rs = ReadSet::new();
    capture_reads(&op, &catalog, &mut rs);

    std::fs::write(
        "/tmp/pulse_cap.txt",
        format!(
            "tables={:?}\nfilters={:?}\nmatch_a={}\nmatch_b={}\n",
            rs.tables,
            rs.filters,
            rs.matches_change(&change_into(a)),
            rs.matches_change(&change_into(b)),
        ),
    )
    .ok();

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
async fn capture_reads_raw_matches_table_insert() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pulse:pulse@localhost:54329/pulse".to_string());
    let Ok(pool) = connect(&url, 2).await else {
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
