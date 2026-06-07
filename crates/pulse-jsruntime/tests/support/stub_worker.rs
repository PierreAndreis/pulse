//! Test-only stub that speaks the engine's NDJSON worker protocol over stdio, so
//! the `pulse-jsruntime` integration tests can drive the real `Worker` without a
//! Node/Bun runtime. Behaviour is keyed by the executed procedure path. This is a
//! fixture for `tests/worker.rs` — not shipped logic.
//!
//! Protocol (one JSON object per line):
//!   out: manifest, ready, dbop, complete
//!   in:  execute, dbresult

use std::collections::HashMap;
use std::io::{BufRead, Write};

use serde_json::{json, Value};

fn emit(out: &mut impl Write, msg: &Value) {
    let mut line = serde_json::to_vec(msg).expect("serialize worker msg");
    line.push(b'\n');
    out.write_all(&line).expect("write to engine");
    out.flush().expect("flush to engine");
}

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    // One procedure per behaviour the tests drive. The schema is empty — the
    // engine introspects physical columns straight from Postgres.
    let procedures = json!([
        {"path": ["echo"], "kind": "reactive"},
        {"path": ["read"], "kind": "reactive"},
        {"path": ["count"], "kind": "analytical"},
        {"path": ["write"], "kind": "mutation"},
        {"path": ["fail"], "kind": "mutation"},
    ]);
    emit(
        &mut stdout,
        &json!({"type": "manifest", "procedures": procedures, "schema": {"tables": {}}}),
    );
    emit(&mut stdout, &json!({"type": "ready"}));

    // request_id -> what we asked the engine to do, so the eventual `dbresult`
    // knows how to finish the request.
    let mut pending: HashMap<String, &'static str> = HashMap::new();
    let mut op_seq: u64 = 0;

    for line in stdin.lock().lines() {
        let line = line.expect("read engine line");
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = serde_json::from_str(&line).expect("parse engine msg");
        match msg["type"].as_str() {
            Some("execute") => {
                let request_id = msg["requestId"].as_str().unwrap_or_default().to_string();
                let path0 = msg["path"][0].as_str().unwrap_or_default();
                let input = msg["input"].clone();
                match path0 {
                    // Pure handler: echo the input straight back, no db access.
                    "echo" => emit(
                        &mut stdout,
                        &json!({"type": "complete", "requestId": request_id, "ok": true, "result": input}),
                    ),
                    // Read paths: collect all rows of `items`, then complete with them.
                    "read" | "count" => {
                        op_seq += 1;
                        pending.insert(request_id.clone(), "read");
                        emit(
                            &mut stdout,
                            &json!({"type": "dbop", "requestId": request_id, "opId": op_seq,
                                    "op": {"kind": "query", "table": "items", "predicates": [], "mode": "collect"}}),
                        );
                    }
                    // Mutation: insert one row, then commit (complete ok).
                    "write" => {
                        op_seq += 1;
                        pending.insert(request_id.clone(), "write");
                        emit(
                            &mut stdout,
                            &json!({"type": "dbop", "requestId": request_id, "opId": op_seq,
                                    "op": {"kind": "insert", "table": "items", "value": {"name": input["name"].clone()}}}),
                        );
                    }
                    // Mutation that writes then errors — the engine must roll the
                    // insert back so nothing persists.
                    "fail" => {
                        op_seq += 1;
                        pending.insert(request_id.clone(), "fail");
                        emit(
                            &mut stdout,
                            &json!({"type": "dbop", "requestId": request_id, "opId": op_seq,
                                    "op": {"kind": "insert", "table": "items", "value": {"name": input["name"].clone()}}}),
                        );
                    }
                    _ => emit(
                        &mut stdout,
                        &json!({"type": "complete", "requestId": request_id, "ok": false,
                                "error": {"code": "NOT_FOUND"}}),
                    ),
                }
            }
            Some("dbresult") => {
                let request_id = msg["requestId"].as_str().unwrap_or_default().to_string();
                let kind = pending.remove(&request_id).unwrap_or("read");
                let ok = msg["ok"].as_bool().unwrap_or(false);
                let value = msg.get("value").cloned().unwrap_or(Value::Null);
                if !ok {
                    emit(
                        &mut stdout,
                        &json!({"type": "complete", "requestId": request_id, "ok": false,
                                "error": {"code": "DB_ERROR"}}),
                    );
                    continue;
                }
                match kind {
                    // The insert succeeded inside the tx, but the handler then fails:
                    // the engine rolls the transaction back.
                    "fail" => emit(
                        &mut stdout,
                        &json!({"type": "complete", "requestId": request_id, "ok": false,
                                "error": {"code": "BAD_INPUT", "message": "handler said no"}}),
                    ),
                    // read / write: complete with the op's value (rows or new id).
                    _ => emit(
                        &mut stdout,
                        &json!({"type": "complete", "requestId": request_id, "ok": true, "result": value}),
                    ),
                }
            }
            _ => {}
        }
    }
}
