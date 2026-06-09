//! WAL/CDC consumer: decode out-of-band Postgres writes (made via raw `psql`,
//! other services, or triggers — not through the Pulse engine) into
//! `ChangeSet`s feeding the same `apply_change_set` seam the in-engine path uses.
//!
//! Transport is **polling over the existing sqlx pool**, not a streaming
//! replication connection: mainline `tokio-postgres` doesn't ship the logical
//! replication protocol, so instead we drain the slot with
//! `pg_logical_slot_get_binary_changes(...)` using the built-in **`pgoutput`**
//! output plugin and decode the binary messages here. `pgoutput`'s binary form
//! is stable and unambiguous (no text-quoting guesswork), and decoding it
//! ourselves keeps the dependency set unchanged. A future upgrade to streaming
//! replication is purely a transport swap behind this same decoder + the
//! `ChangeSet` seam.
//!
//! Row images are built with `pulse_sql::text_to_key_value` against the same
//! `Catalog` the engine uses, so a WAL-sourced `Change` carries byte-identical
//! `RowValues` to an in-engine `RETURNING` row — the one matcher fires for both.

use std::collections::HashMap;

use pulse_core::{Change, ChangeOp, ChangeSet, KeyValue, Lsn, PrimaryKey, RowValues, TableId};
use pulse_sql::{text_to_key_value, Catalog, PgPool};
use uuid::Uuid;

/// Default names; overridable by the host when it wires the consumer.
pub const DEFAULT_SLOT: &str = "pulse_slot";
pub const DEFAULT_PUBLICATION: &str = "pulse_pub";

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("pgoutput message truncated (need {need} bytes at offset {at})")]
    Truncated { need: usize, at: usize },
    #[error("unexpected pgoutput tuple tag {0:?}")]
    BadTupleTag(u8),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

// ── pgoutput binary reader (network byte order) ─────────────────────────────

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], WalError> {
        if self.pos + n > self.buf.len() {
            return Err(WalError::Truncated { need: n, at: self.pos });
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, WalError> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, WalError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, WalError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, WalError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// A null-terminated UTF-8 string.
    fn cstr(&mut self) -> Result<String, WalError> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(WalError::Truncated { need: 1, at: start });
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.pos]).into_owned();
        self.pos += 1; // skip NUL
        Ok(s)
    }
}

/// A pgoutput `Relation` message: the ordered (snake_case) column names for a
/// relation oid. Tuple messages reference columns positionally, so this must be
/// seen before any Insert/Update/Delete for the relation.
struct Relation {
    table: String,
    columns: Vec<String>,
}

/// One decoded tuple as positional `Some(text)` / `None` (null / unchanged-toast
/// / binary) values, aligned with the relation's columns.
type Tuple = Vec<Option<String>>;

/// Stateful pgoutput decoder: caches relations and accumulates a transaction's
/// changes, emitting one `ChangeSet` per commit (preserving the atomic-tx unit).
pub struct WalDecoder {
    catalog: Catalog,
    relations: HashMap<i32, Relation>,
    pending: Vec<Change>,
}

impl WalDecoder {
    pub fn new(catalog: Catalog) -> Self {
        WalDecoder {
            catalog,
            relations: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// Feed one pgoutput message. Returns `Some(ChangeSet)` when a transaction
    /// commits with at least one reactive change; `None` otherwise.
    pub fn feed(&mut self, data: &[u8]) -> Result<Option<ChangeSet>, WalError> {
        let mut r = Reader::new(data);
        match r.u8()? {
            b'B' => {
                // Begin: Int64 final_lsn, Int64 timestamp, Int32 xid — start a tx.
                self.pending.clear();
                Ok(None)
            }
            b'C' => {
                // Commit: Int8 flags, Int64 commit_lsn, Int64 end_lsn, Int64 ts.
                let _flags = r.u8()?;
                let commit_lsn = Lsn(r.u64()?);
                let changes = std::mem::take(&mut self.pending);
                if changes.is_empty() {
                    return Ok(None);
                }
                Ok(Some(ChangeSet {
                    commit_lsn,
                    changes,
                }))
            }
            b'R' => {
                self.read_relation(&mut r)?;
                Ok(None)
            }
            b'I' => {
                let relid = r.i32()?;
                let _tag = r.u8()?; // 'N'
                let new = read_tuple(&mut r)?;
                if let Some(c) = self.change(relid, ChangeOp::Insert, Some(new), None) {
                    self.pending.push(c);
                }
                Ok(None)
            }
            b'U' => {
                let relid = r.i32()?;
                let mut old = None;
                let mut tag = r.u8()?;
                if tag == b'K' || tag == b'O' {
                    old = Some(read_tuple(&mut r)?);
                    tag = r.u8()?; // expect 'N'
                }
                debug_assert_eq!(tag, b'N');
                let new = read_tuple(&mut r)?;
                if let Some(c) = self.change(relid, ChangeOp::Update, Some(new), old) {
                    self.pending.push(c);
                }
                Ok(None)
            }
            b'D' => {
                let relid = r.i32()?;
                let _tag = r.u8()?; // 'K' (key) or 'O' (old)
                let old = read_tuple(&mut r)?;
                if let Some(c) = self.change(relid, ChangeOp::Delete, None, Some(old)) {
                    self.pending.push(c);
                }
                Ok(None)
            }
            // Type / Origin / Message / Truncate — not reactive-relevant for the
            // row matcher (Truncate handling is a later slice).
            _ => Ok(None),
        }
    }

    fn read_relation(&mut self, r: &mut Reader) -> Result<(), WalError> {
        let relid = r.i32()?;
        let _namespace = r.cstr()?;
        let table = r.cstr()?;
        let _replica_identity = r.u8()?;
        let ncols = r.i16()?;
        let mut columns = Vec::with_capacity(ncols.max(0) as usize);
        for _ in 0..ncols {
            let _flags = r.u8()?; // 1 = part of the key
            columns.push(r.cstr()?);
            let _type_oid = r.i32()?;
            let _type_mod = r.i32()?;
        }
        self.relations.insert(relid, Relation { table, columns });
        Ok(())
    }

    /// Build a `Change` from a decoded tuple, mapping snake columns → catalog
    /// fields → `KeyValue` exactly like the in-engine path. Returns `None` for a
    /// relation absent from the catalog (internal `_pulse_*` tables, etc.).
    fn change(
        &self,
        relid: i32,
        op: ChangeOp,
        new: Option<Tuple>,
        old: Option<Tuple>,
    ) -> Option<Change> {
        let rel = self.relations.get(&relid)?;
        let table = self.catalog.table(&rel.table)?;
        let to_values = |t: &Tuple| -> RowValues {
            let mut out = RowValues::new();
            for (name, val) in rel.columns.iter().zip(t.iter()) {
                if let Some(col) = table.column_by_name(name) {
                    if let Some(kv) = text_to_key_value(val.as_deref(), col) {
                        out.insert(col.field.clone(), kv);
                    }
                }
            }
            out
        };
        // Primary key (`_id`) from whichever image is present.
        let key = new
            .as_ref()
            .and_then(|t| pk_from(rel, t))
            .or_else(|| old.as_ref().and_then(|t| pk_from(rel, t)))
            .unwrap_or_else(|| PrimaryKey::single(KeyValue::Null));
        Some(Change {
            table: TableId::new(rel.table.clone()),
            key,
            op,
            new: new.as_ref().map(&to_values),
            old: old.as_ref().map(&to_values),
        })
    }
}

/// The `_id` primary key from a tuple, parsed as a uuid.
fn pk_from(rel: &Relation, t: &Tuple) -> Option<PrimaryKey> {
    let idx = rel.columns.iter().position(|c| c == "_id")?;
    let raw = t.get(idx)?.as_deref()?;
    let uuid = Uuid::parse_str(raw).ok()?;
    Some(PrimaryKey::single(KeyValue::Uuid(uuid)))
}

/// Read a `TupleData`: Int16 ncols, then per column a kind byte and (for text)
/// a length-prefixed value. Null / unchanged-toast / binary → `None`.
fn read_tuple(r: &mut Reader) -> Result<Tuple, WalError> {
    let ncols = r.i16()?;
    let mut out = Vec::with_capacity(ncols.max(0) as usize);
    for _ in 0..ncols {
        match r.u8()? {
            b'n' | b'u' => out.push(None), // null / unchanged TOAST
            b't' => {
                let len = r.i32()? as usize;
                let bytes = r.take(len)?;
                out.push(Some(String::from_utf8_lossy(bytes).into_owned()));
            }
            b'b' => {
                let len = r.i32()? as usize;
                r.take(len)?; // binary value — not indexed; treat as absent
                out.push(None);
            }
            other => return Err(WalError::BadTupleTag(other)),
        }
    }
    Ok(out)
}

// ── slot / publication setup + polling (sqlx) ───────────────────────────────

/// Create the publication (`FOR ALL TABLES`) if absent. Idempotent.
pub async fn ensure_publication(pool: &PgPool, name: &str) -> Result<(), WalError> {
    let exists: Option<(String,)> =
        sqlx::query_as("SELECT pubname FROM pg_publication WHERE pubname = $1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    if exists.is_none() {
        // Publication names can't be bound; `name` is a controlled identifier.
        sqlx::query(&format!("CREATE PUBLICATION {name} FOR ALL TABLES"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Create the `pgoutput` logical slot if absent. Idempotent.
pub async fn ensure_slot(pool: &PgPool, slot: &str) -> Result<(), WalError> {
    let exists: Option<(String,)> =
        sqlx::query_as("SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot)
            .fetch_optional(pool)
            .await?;
    if exists.is_none() {
        sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
            .bind(slot)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Drop the slot (test teardown / dev reset). Ignores "does not exist".
pub async fn drop_slot(pool: &PgPool, slot: &str) -> Result<(), WalError> {
    sqlx::query("SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)")
        .bind(slot)
        .execute(pool)
        .await?;
    Ok(())
}

/// Drain the slot once, returning the raw pgoutput message buffers in WAL order.
/// `get_binary_changes` consumes (advances the slot), so each buffer is seen once.
pub async fn poll_raw(pool: &PgPool, slot: &str, publication: &str) -> Result<Vec<Vec<u8>>, WalError> {
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
        "SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, \
         'proto_version', '1', 'publication_names', $2)",
    )
    .bind(slot)
    .bind(publication)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(d,)| d).collect())
}

/// Drain the slot once and decode into committed `ChangeSet`s.
pub async fn poll_change_sets(
    pool: &PgPool,
    slot: &str,
    publication: &str,
    decoder: &mut WalDecoder,
) -> Result<Vec<ChangeSet>, WalError> {
    let mut out = Vec::new();
    for buf in poll_raw(pool, slot, publication).await? {
        if let Some(cs) = decoder.feed(&buf)? {
            out.push(cs);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_sql::{Column, PgTypeClass, Table};

    fn messages_catalog() -> Catalog {
        let cols = vec![
            Column {
                column: "_id".into(),
                field: "_id".into(),
                type_class: PgTypeClass::Uuid,
                nullable: false,
                id_ref: Some("messages".into()),
            },
            Column {
                column: "channel_id".into(),
                field: "channelId".into(),
                type_class: PgTypeClass::Uuid,
                nullable: false,
                id_ref: Some("channels".into()),
            },
            Column {
                column: "body".into(),
                field: "body".into(),
                type_class: PgTypeClass::Text,
                nullable: false,
                id_ref: None,
            },
        ];
        let mut c = Catalog::default();
        c.tables
            .insert("messages".into(), Table::from_columns("messages", cols));
        c
    }

    // Hand-encode pgoutput messages so the decoder is locked without a database.
    fn put_cstr(b: &mut Vec<u8>, s: &str) {
        b.extend_from_slice(s.as_bytes());
        b.push(0);
    }
    fn put_text_col(b: &mut Vec<u8>, s: &str) {
        b.push(b't');
        b.extend_from_slice(&(s.len() as i32).to_be_bytes());
        b.extend_from_slice(s.as_bytes());
    }

    fn relation_msg(relid: i32, table: &str, cols: &[&str]) -> Vec<u8> {
        let mut b = vec![b'R'];
        b.extend_from_slice(&relid.to_be_bytes());
        put_cstr(&mut b, "public");
        put_cstr(&mut b, table);
        b.push(b'd'); // replica identity default
        b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for (i, c) in cols.iter().enumerate() {
            b.push(if i == 0 { 1 } else { 0 }); // first col is the key
            put_cstr(&mut b, c);
            b.extend_from_slice(&0i32.to_be_bytes()); // type oid
            b.extend_from_slice(&(-1i32).to_be_bytes()); // type mod
        }
        b
    }

    fn insert_msg(relid: i32, vals: &[&str]) -> Vec<u8> {
        let mut b = vec![b'I'];
        b.extend_from_slice(&relid.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&(vals.len() as i16).to_be_bytes());
        for v in vals {
            put_text_col(&mut b, v);
        }
        b
    }

    fn begin_msg() -> Vec<u8> {
        let mut b = vec![b'B'];
        b.extend_from_slice(&7u64.to_be_bytes()); // final_lsn
        b.extend_from_slice(&0u64.to_be_bytes()); // timestamp
        b.extend_from_slice(&42i32.to_be_bytes()); // xid
        b
    }
    fn commit_msg(lsn: u64) -> Vec<u8> {
        let mut b = vec![b'C', 0]; // tag + flags
        b.extend_from_slice(&lsn.to_be_bytes()); // commit_lsn
        b.extend_from_slice(&lsn.to_be_bytes()); // end_lsn
        b.extend_from_slice(&0u64.to_be_bytes()); // timestamp
        b
    }

    #[test]
    fn decodes_insert_into_a_committed_change_set() {
        let mut d = WalDecoder::new(messages_catalog());
        let id = Uuid::nil();
        let chan = Uuid::from_u128(0xAB);
        // BEGIN, RELATION, INSERT, COMMIT — one transaction.
        assert!(d.feed(&begin_msg()).unwrap().is_none());
        assert!(d
            .feed(&relation_msg(1, "messages", &["_id", "channel_id", "body"]))
            .unwrap()
            .is_none());
        assert!(d
            .feed(&insert_msg(
                1,
                &[&id.to_string(), &chan.to_string(), "hello"],
            ))
            .unwrap()
            .is_none());
        let cs = d.feed(&commit_msg(99)).unwrap().expect("commit emits a ChangeSet");

        assert_eq!(cs.commit_lsn, Lsn(99));
        assert_eq!(cs.changes.len(), 1);
        let c = &cs.changes[0];
        assert_eq!(c.table, TableId::new("messages"));
        assert_eq!(c.op, ChangeOp::Insert);
        assert_eq!(c.key, PrimaryKey::single(KeyValue::Uuid(id)));
        let new = c.new.as_ref().unwrap();
        // id_ref columns decode to raw uuids; text passes through; identical to
        // the in-engine `row_to_values` image so the same matcher fires.
        assert_eq!(new.get("channelId"), Some(&KeyValue::Uuid(chan)));
        assert_eq!(new.get("body"), Some(&KeyValue::Text("hello".into())));
        assert!(c.old.is_none());
    }

    #[test]
    fn relation_absent_from_catalog_is_skipped() {
        let mut d = WalDecoder::new(messages_catalog());
        d.feed(&begin_msg()).unwrap();
        d.feed(&relation_msg(5, "_pulse_node_interest", &["_id"]))
            .unwrap();
        d.feed(&insert_msg(5, &[&Uuid::nil().to_string()])).unwrap();
        // No reactive change accumulated → commit emits nothing.
        assert!(d.feed(&commit_msg(1)).unwrap().is_none());
    }
}
