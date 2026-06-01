//! DB-backed test for the collab SQL ops: two Yjs updates applied through
//! `execute_op` (ApplyCollab) into a `bytea` field both survive the CRDT merge,
//! and `GetCollab` reads the merged state back. Skips if Postgres is unreachable.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use pulse_sql::{connect, execute_op, introspect, DbOp, SchemaMeta};
use sqlx::Executor;
use yrs::updates::decoder::Decode;
use yrs::{GetString, ReadTxn, Text, Transact, Update};

async fn pool() -> Option<sqlx::PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pulse:pulse@localhost:54329/pulse".to_string());
    connect(&url, 4).await.ok()
}

/// A Yjs update inserting `text` at index 0 of root text "body", from empty.
fn insert_update(text: &str) -> Vec<u8> {
    let doc = yrs::Doc::new();
    let before = doc.transact().state_vector();
    {
        let body = doc.get_or_insert_text("body");
        let mut txn = doc.transact_mut();
        body.insert(&mut txn, 0, text);
    }
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&before)
}

fn read_body(state_b64: &str) -> String {
    let bytes = B64.decode(state_b64).unwrap();
    let doc = yrs::Doc::new();
    if !bytes.is_empty() {
        let update = Update::decode_v1(&bytes).unwrap();
        doc.transact_mut().apply_update(update).unwrap();
    }
    let body = doc.get_or_insert_text("body");
    let txn = doc.transact();
    body.get_string(&txn)
}

#[tokio::test]
async fn collab_ops_merge_two_updates_in_db() {
    let Some(pool) = pool().await else {
        eprintln!("skip: no Postgres");
        return;
    };

    // A throwaway table with a bytea collab column.
    pool.execute("drop table if exists collab_docs cascade")
        .await
        .unwrap();
    pool.execute(
        "create table collab_docs (\
            _id uuid primary key default gen_random_uuid(),\
            _creation_time bigint not null default 0,\
            body bytea)",
    )
    .await
    .unwrap();
    let id: String =
        sqlx::query_scalar("insert into collab_docs default values returning _id::text")
            .fetch_one(&pool)
            .await
            .unwrap();

    let schema = SchemaMeta::default();
    let catalog = introspect(&pool, &schema).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    // Apply two independent edits via execute_op.
    for text in ["hello", "world"] {
        let op = DbOp::ApplyCollab {
            table: "collab_docs".into(),
            id: id.clone(),
            field: "body".into(),
            update: B64.encode(insert_update(text)),
        };
        let (_merged, change) = execute_op(&mut conn, &catalog, &op).await.unwrap();
        assert!(
            change.is_some(),
            "ApplyCollab must emit a change for reactivity"
        );
    }

    // Read the merged state back via GetCollab.
    let get = DbOp::GetCollab {
        table: "collab_docs".into(),
        id: id.clone(),
        field: "body".into(),
    };
    let (state, _) = execute_op(&mut conn, &catalog, &get).await.unwrap();
    let state_b64 = state.as_str().unwrap();
    let body = read_body(state_b64);

    assert!(body.contains("hello"), "first edit lost: {body:?}");
    assert!(body.contains("world"), "second edit lost: {body:?}");

    pool.execute("drop table if exists collab_docs cascade")
        .await
        .unwrap();
}
