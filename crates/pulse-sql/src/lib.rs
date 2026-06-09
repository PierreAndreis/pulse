//! SQL layer: OLTP/OLAP connection pools, the document query builder → SQL
//! lowering, and (M2) read-set extraction.
//!
//! Values always cross the SQL boundary cast to/from `text`, so binding and row
//! decoding stay fully dynamic without per-type sqlx plumbing. Table-qualified
//! ids (`"table:uuid"`) are encoded/decoded using schema metadata supplied by
//! the worker.

pub mod naming;

mod catalog;
mod ops;

pub use catalog::{
    decode_id, encode_id, introspect, Catalog, Column, FieldMeta, PgTypeClass, SchemaMeta, Table,
    TableSchema,
};
pub use naming::{column_to_field, field_to_column};
pub use ops::{
    capture_reads, current_wal_lsn, ensure_mutation_log, execute_op, execute_op_pool,
    lookup_mutation, record_mutation, text_to_key_value, DbOp, FilterExpr, Order, OrderKey, PredOp,
    Predicate, QueryMode, SqlError,
};
// PoolConfig / connect_with are defined below.

pub use sqlx::postgres::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;

/// How to configure a connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: u32,
    /// Set every connection's default isolation to `SERIALIZABLE` (for the OLTP
    /// pool, so mutation transactions get true serializability + `40001` retry).
    pub serializable: bool,
    /// Per-statement timeout. The OLTP pool runs a low timeout so a slow query
    /// can't pin a reactive connection; the OLAP pool allows long analytics.
    pub statement_timeout_ms: Option<u64>,
}

/// Open a Postgres pool with the given configuration.
pub async fn connect_with(url: &str, config: PoolConfig) -> Result<PgPool, sqlx::Error> {
    let serializable = config.serializable;
    let timeout = config.statement_timeout_ms;
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                if serializable {
                    conn.execute(
                        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    )
                    .await?;
                }
                if let Some(ms) = timeout {
                    // statement_timeout takes a value in ms; 0 disables it.
                    conn.execute(format!("SET statement_timeout = {ms}").as_str())
                        .await?;
                }
                Ok(())
            })
        })
        .connect(url)
        .await
}

/// Open the default (OLTP) pool: `SERIALIZABLE`, no statement timeout.
/// Kept for existing callers/tests.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    connect_with(
        url,
        PoolConfig {
            max_connections,
            serializable: true,
            statement_timeout_ms: None,
        },
    )
    .await
}
