//! The NDJSON protocol spoken between the engine and the TS worker over the
//! worker's stdio. One JSON object per line.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use pulse_core::ProcedureKind;
use pulse_sql::{DbOp, SchemaMeta};

/// A procedure the worker has loaded, with its kind (for routing).
#[derive(Debug, Clone, Deserialize)]
pub struct ProcInfo {
    pub path: Vec<String>,
    pub kind: ProcedureKind,
}

/// A structured error returned by a handler.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerError {
    pub code: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub message: Option<String>,
}

impl WorkerError {
    pub fn internal(message: impl Into<String>) -> Self {
        WorkerError {
            code: "INTERNAL".to_string(),
            data: Value::Null,
            message: Some(message.into()),
        }
    }
}

/// Messages the worker sends to the engine (worker stdout).
#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum WorkerOut {
    /// Sent once at startup: the loaded procedures and the schema metadata.
    Manifest {
        procedures: Vec<ProcInfo>,
        schema: SchemaMeta,
    },
    /// Sent after the manifest once the worker is ready to serve.
    Ready,
    /// A database operation requested by a handler mid-execution.
    Dbop {
        request_id: String,
        op_id: u64,
        op: DbOp,
    },
    /// A handler finished (ok with a result, or an error).
    Complete {
        request_id: String,
        ok: bool,
        #[serde(default)]
        result: Value,
        #[serde(default)]
        error: Option<WorkerError>,
    },
    /// A log line from the worker.
    Log {
        #[serde(default)]
        level: String,
        message: String,
    },
}

/// Messages the engine sends to the worker (worker stdin).
#[derive(Debug, Serialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum EngineMsg {
    /// Run a procedure.
    Execute {
        request_id: String,
        path: Vec<String>,
        input: Value,
        headers: std::collections::HashMap<String, String>,
    },
    /// The result of a database operation.
    Dbresult {
        request_id: String,
        op_id: u64,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}
