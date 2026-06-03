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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn worker_out_dbop_decodes_camelcase() {
        let line = r#"{"type":"dbop","requestId":"r1","opId":7,"op":{"kind":"get","table":"users","id":"users:1"}}"#;
        let msg: WorkerOut = serde_json::from_str(line).unwrap();
        match msg {
            WorkerOut::Dbop {
                request_id,
                op_id,
                op,
            } => {
                assert_eq!(request_id, "r1");
                assert_eq!(op_id, 7);
                match op {
                    DbOp::Get { table, id } => {
                        assert_eq!(table, "users");
                        assert_eq!(id, "users:1");
                    }
                    other => panic!("expected DbOp::Get, got {other:?}"),
                }
            }
            other => panic!("expected WorkerOut::Dbop, got {other:?}"),
        }
    }

    #[test]
    fn worker_out_complete_defaults() {
        let line = r#"{"type":"complete","requestId":"r1","ok":true}"#;
        let msg: WorkerOut = serde_json::from_str(line).unwrap();
        match msg {
            WorkerOut::Complete {
                request_id,
                ok,
                result,
                error,
            } => {
                assert_eq!(request_id, "r1");
                assert!(ok);
                assert_eq!(result, Value::Null);
                assert!(error.is_none());
            }
            other => panic!("expected WorkerOut::Complete, got {other:?}"),
        }
    }

    #[test]
    fn engine_dbresult_omits_error_on_ok() {
        // ok case: "value" present, "error" omitted.
        let ok = EngineMsg::Dbresult {
            request_id: "r1".to_string(),
            op_id: 1,
            ok: true,
            value: Some(json!(1)),
            error: None,
        };
        let v = serde_json::to_value(&ok).unwrap();
        assert!(v.get("value").is_some());
        assert!(v.get("error").is_none());

        // error case: "error" present, "value" omitted.
        let err = EngineMsg::Dbresult {
            request_id: "r1".to_string(),
            op_id: 1,
            ok: false,
            value: None,
            error: Some("boom".to_string()),
        };
        let v = serde_json::to_value(&err).unwrap();
        assert!(v.get("error").is_some());
        assert!(v.get("value").is_none());
    }

    #[test]
    fn engine_execute_tag_and_camelcase() {
        let msg = EngineMsg::Execute {
            request_id: "r1".to_string(),
            path: vec!["users".to_string(), "create".to_string()],
            input: json!({"name": "ada"}),
            headers: HashMap::new(),
        };
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v.get("type").unwrap(), "execute");
        assert!(v.get("requestId").is_some());
        // No snake_case leak.
        assert!(v.get("request_id").is_none());

        // NDJSON framing safety: the serialized line must not contain a raw newline.
        let line = serde_json::to_string(&msg).unwrap();
        assert!(!line.contains('\n'));
    }

    #[test]
    fn worker_error_internal() {
        let e = WorkerError::internal("x");
        assert_eq!(e.code, "INTERNAL");
        assert_eq!(e.data, Value::Null);
        assert_eq!(e.message, Some("x".to_string()));

        let d = WorkerError::default();
        assert_eq!(d.code, "");
        assert_eq!(d.data, Value::Null);
        assert!(d.message.is_none());
    }
}
