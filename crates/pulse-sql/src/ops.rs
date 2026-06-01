use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgPool, postgres::PgRow, PgConnection, Row};
use uuid::Uuid;

use pulse_core::{
    Change, ChangeOp, Cond, Filter, FilterOp, KeyValue, PrimaryKey, ReadSet, RowValues, TableId,
};

use crate::catalog::{decode_id, encode_id, Catalog, Column, PgTypeClass, Table};

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("unknown table `{0}`")]
    UnknownTable(String),
    #[error("unknown field `{field}` on table `{table}`")]
    UnknownField { table: String, field: String },
    #[error("expected unique result but found multiple rows")]
    NotUnique,
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PredOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl PredOp {
    fn sql(self) -> &'static str {
        match self {
            PredOp::Eq => "=",
            PredOp::Gt => ">",
            PredOp::Gte => ">=",
            PredOp::Lt => "<",
            PredOp::Lte => "<=",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Predicate {
    pub field: String,
    pub op: PredOp,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryMode {
    Take,
    Collect,
    First,
    Unique,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DbOp {
    Get {
        table: String,
        id: String,
    },
    Query {
        table: String,
        #[serde(default)]
        predicates: Vec<Predicate>,
        #[serde(default)]
        order: Option<Order>,
        #[serde(default)]
        limit: Option<i64>,
        mode: QueryMode,
    },
    Insert {
        table: String,
        value: Map<String, Value>,
    },
    Patch {
        table: String,
        id: String,
        fields: Map<String, Value>,
    },
    Replace {
        table: String,
        id: String,
        value: Map<String, Value>,
    },
    Delete {
        table: String,
        id: String,
    },
    /// Read a collaborative (`v.collab()`) field's full Yjs state, base64-encoded.
    GetCollab {
        table: String,
        id: String,
        field: String,
    },
    /// Merge a base64 Yjs update into a collab field (CRDT merge via `pulse-collab`,
    /// inside the surrounding transaction). Returns the merged state (base64).
    ApplyCollab {
        table: String,
        id: String,
        field: String,
        update: String,
    },
    /// Raw analytical SQL (read-only). The user writes the SQL and any casts;
    /// params are bound as text with table-qualified ids decoded to their uuid.
    Raw {
        sql: String,
        #[serde(default)]
        params: Vec<Value>,
    },
}

impl DbOp {
    /// The table this op touches and whether it is a write — used by the reactor
    /// to capture read/write-sets. Raw analytical SQL is opaque (returns `None`).
    pub fn access(&self) -> Option<(&str, bool)> {
        match self {
            DbOp::Get { table, .. }
            | DbOp::Query { table, .. }
            | DbOp::GetCollab { table, .. } => Some((table, false)),
            DbOp::Insert { table, .. }
            | DbOp::Patch { table, .. }
            | DbOp::Replace { table, .. }
            | DbOp::Delete { table, .. }
            | DbOp::ApplyCollab { table, .. } => Some((table, true)),
            DbOp::Raw { .. } => None,
        }
    }
}

/// Strip a `table:` prefix from an id-looking string for raw-SQL binds.
fn decode_id_param(s: &str) -> &str {
    if let Some((prefix, rest)) = s.split_once(':') {
        if !prefix.is_empty() && Uuid::parse_str(rest).is_ok() {
            return rest;
        }
    }
    s
}

fn raw_bind(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(decode_id_param(s).to_string()),
        other => Some(other.to_string()),
    }
}

fn table<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a Table, SqlError> {
    catalog.table(name).ok_or_else(|| SqlError::UnknownTable(name.to_string()))
}

fn column<'a>(t: &'a Table, field: &str) -> Result<&'a Column, SqlError> {
    t.column_by_field(field).ok_or_else(|| SqlError::UnknownField {
        table: t.name.clone(),
        field: field.to_string(),
    })
}

/// Convert a stored value (read as text) to JSON, honoring id encoding and type.
fn text_to_json(text: Option<String>, col: &Column) -> Value {
    let Some(s) = text else { return Value::Null };
    if let Some(ref_table) = &col.id_ref {
        return Value::String(encode_id(ref_table, &s));
    }
    match col.type_class {
        PgTypeClass::Int8 => s.parse::<i64>().map(|n| json!(n)).unwrap_or(Value::String(s)),
        PgTypeClass::Float8 => s.parse::<f64>().map(|n| json!(n)).unwrap_or(Value::String(s)),
        PgTypeClass::Bool => Value::Bool(s == "true" || s == "t"),
        PgTypeClass::Jsonb => serde_json::from_str(&s).unwrap_or(Value::String(s)),
        _ => Value::String(s),
    }
}

/// Convert a JSON input value to the text we bind (cast applied in SQL).
fn json_to_bind(value: &Value, col: &Column) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => {
            if col.id_ref.is_some() {
                Some(decode_id(s).to_string())
            } else {
                Some(s.clone())
            }
        }
        other => Some(other.to_string()),
    }
}

fn row_to_json(row: &sqlx::postgres::PgRow, t: &Table) -> Result<Value, SqlError> {
    let mut obj = Map::new();
    for col in &t.columns {
        let text: Option<String> = row.try_get(col.column.as_str())?;
        obj.insert(col.field.clone(), text_to_json(text, col));
    }
    Ok(Value::Object(obj))
}

async fn fetch_rows(
    conn: &mut PgConnection,
    sql: &str,
    binds: &[Option<String>],
    t: &Table,
) -> Result<Vec<Value>, SqlError> {
    let mut q = sqlx::query(sql);
    for b in binds {
        q = q.bind(b.clone());
    }
    let rows = q.fetch_all(conn).await?;
    rows.iter().map(|r| row_to_json(r, t)).collect()
}

fn pred_op_to_filter(op: PredOp) -> FilterOp {
    match op {
        PredOp::Eq => FilterOp::Eq,
        PredOp::Gt => FilterOp::Gt,
        PredOp::Gte => FilterOp::Gte,
        PredOp::Lt => FilterOp::Lt,
        PredOp::Lte => FilterOp::Lte,
    }
}

/// Coerce a JSON predicate value to a `KeyValue` for the column, decoding ids.
/// Returns `None` for null / unorderable columns (float/jsonb) — the caller then
/// drops the condition, broadening the filter (safe over-approximation).
fn json_to_key_value(value: &Value, col: &Column) -> Option<KeyValue> {
    if value.is_null() {
        return None;
    }
    if col.id_ref.is_some() {
        return Uuid::parse_str(decode_id(value.as_str()?)).ok().map(KeyValue::Uuid);
    }
    match col.type_class {
        PgTypeClass::Int8 => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .map(KeyValue::Int),
        PgTypeClass::Bool => value.as_bool().map(KeyValue::Bool),
        PgTypeClass::Text => value.as_str().map(|s| KeyValue::Text(s.to_string())),
        PgTypeClass::Uuid => {
            value.as_str().and_then(|s| Uuid::parse_str(decode_id(s)).ok()).map(KeyValue::Uuid)
        }
        _ => None,
    }
}

/// Fold the reads performed by `op` into `rs` (precise where analyzable, coarse
/// table-wildcard otherwise — never a miss).
pub fn capture_reads(op: &DbOp, catalog: &Catalog, rs: &mut ReadSet) {
    match op {
        DbOp::Get { table: name, id } => {
            let tid = TableId::new(name.clone());
            match Uuid::parse_str(decode_id(id)) {
                Ok(uuid) => rs.add_key(tid, PrimaryKey::single(KeyValue::Uuid(uuid))),
                Err(_) => rs.add_table(tid),
            }
        }
        DbOp::Query { table: name, predicates, .. } => {
            let tid = TableId::new(name.clone());
            if predicates.is_empty() {
                rs.add_table(tid); // full-table read
                return;
            }
            let Some(t) = catalog.table(name) else {
                rs.add_table(tid);
                return;
            };
            let mut conds = Vec::new();
            for p in predicates {
                if let Some(col) = t.column_by_field(&p.field) {
                    if let Some(value) = json_to_key_value(&p.value, col) {
                        conds.push(Cond { field: p.field.clone(), op: pred_op_to_filter(p.op), value });
                    }
                    // unbuildable cond dropped → filter broadens (still safe)
                }
            }
            rs.add_filter(tid, Filter { conds });
        }
        DbOp::Raw { .. } => {
            // Raw SQL reads are opaque → conservatively depend on every table.
            for name in catalog.tables.keys() {
                rs.add_table(TableId::new(name.clone()));
            }
        }
        DbOp::GetCollab { table: name, id, .. } => {
            // Reading a collab doc depends on that row (key-level).
            let tid = TableId::new(name.clone());
            match Uuid::parse_str(decode_id(id)) {
                Ok(uuid) => rs.add_key(tid, PrimaryKey::single(KeyValue::Uuid(uuid))),
                Err(_) => rs.add_table(tid),
            }
        }
        DbOp::Insert { .. }
        | DbOp::Patch { .. }
        | DbOp::Replace { .. }
        | DbOp::Delete { .. }
        | DbOp::ApplyCollab { .. } => {}
    }
}

/// A row's primary key (`_id`) as a `PrimaryKey`, for change matching/dedup.
fn pk_of(row: &PgRow) -> PrimaryKey {
    let id: Option<String> = row.try_get("_id").ok().flatten();
    let kv = id
        .and_then(|s| Uuid::parse_str(&s).ok())
        .map(KeyValue::Uuid)
        .unwrap_or(KeyValue::Null);
    PrimaryKey::single(kv)
}

/// The filterable columns of a row as a `RowValues` image (ids decoded to raw
/// uuids so they compare equal to predicate values). Float/jsonb/timestamptz and
/// nulls are omitted — a filter on such a column degrades to a table-wildcard.
fn row_to_values(row: &PgRow, t: &Table) -> RowValues {
    let mut out = RowValues::new();
    for col in &t.columns {
        let text: Option<String> = row.try_get(col.column.as_str()).ok().flatten();
        let Some(s) = text else { continue };
        let kv = if col.id_ref.is_some() {
            Uuid::parse_str(&s).ok().map(KeyValue::Uuid)
        } else {
            match col.type_class {
                PgTypeClass::Int8 => s.parse::<i64>().ok().map(KeyValue::Int),
                PgTypeClass::Bool => Some(KeyValue::Bool(s == "true" || s == "t")),
                PgTypeClass::Uuid => Uuid::parse_str(&s).ok().map(KeyValue::Uuid),
                PgTypeClass::Text => Some(KeyValue::Text(s)),
                _ => None,
            }
        };
        if let Some(kv) = kv {
            out.insert(col.field.clone(), kv);
        }
    }
    out
}

/// Create the idempotency-key log used to make mutations exactly-once. A queued
/// write carries a stable client id; the engine records applied ids here inside
/// the mutation's own transaction, so a write flushed twice (lost ack, or two
/// tabs draining the shared offline queue) is applied at most once.
pub async fn ensure_mutation_log(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _pulse_mutations (\
            id text PRIMARY KEY, \
            result jsonb NOT NULL DEFAULT 'null'::jsonb, \
            created_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// The recorded result of an already-applied mutation, if any (a fast pre-check
/// on the autocommit pool so a duplicate skips the handler entirely).
pub async fn lookup_mutation(pool: &PgPool, id: &str) -> Option<Value> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT result::text FROM _pulse_mutations WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.and_then(|(s,)| serde_json::from_str(&s).ok())
}

/// Reserve a mutation id inside the surrounding (SERIALIZABLE) transaction,
/// storing its result. Returns `false` when the id already exists — i.e. this is
/// a duplicate delivery to dedupe. A concurrent duplicate blocks on the unique
/// key until the winner commits, then surfaces here as `Ok(false)`.
pub async fn record_mutation(
    conn: &mut PgConnection,
    id: &str,
    result: &Value,
) -> Result<bool, sqlx::Error> {
    let res_text = result.to_string();
    match sqlx::query("INSERT INTO _pulse_mutations (id, result) VALUES ($1, $2::jsonb)")
        .bind(id)
        .bind(&res_text)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => Ok(true),
        Err(sqlx::Error::Database(d)) if d.code().as_deref() == Some("23505") => Ok(false),
        Err(e) => Err(e),
    }
}

/// Execute one operation on a freshly-acquired pooled connection (autocommit).
/// Used for read-only / non-transactional ops.
pub async fn execute_op_pool(
    pool: &PgPool,
    catalog: &Catalog,
    op: &DbOp,
) -> Result<(Value, Option<Change>), SqlError> {
    let mut conn = pool.acquire().await?;
    execute_op(&mut conn, catalog, op).await
}

/// Execute one database operation on a given connection (which may be inside a
/// transaction). Returns JSON: a document, array, id string, or null.
pub async fn execute_op(
    conn: &mut PgConnection,
    catalog: &Catalog,
    op: &DbOp,
) -> Result<(Value, Option<Change>), SqlError> {
    match op {
        DbOp::Get { table: name, id } => {
            let t = table(catalog, name)?;
            let sql = format!(
                "SELECT {} FROM {} WHERE _id = $1::uuid",
                t.select_list(),
                name
            );
            let rows = fetch_rows(&mut *conn, &sql, &[Some(decode_id(id).to_string())], t).await?;
            Ok((rows.into_iter().next().unwrap_or(Value::Null), None))
        }

        DbOp::Query { table: name, predicates, order, limit, mode } => {
            let t = table(catalog, name)?;
            let mut sql = format!("SELECT {} FROM {}", t.select_list(), name);
            let mut binds: Vec<Option<String>> = Vec::new();

            if !predicates.is_empty() {
                let mut clauses = Vec::new();
                for p in predicates {
                    let col = column(t, &p.field)?;
                    binds.push(json_to_bind(&p.value, col));
                    clauses.push(format!(
                        "{} {} ${}::{}",
                        col.column,
                        p.op.sql(),
                        binds.len(),
                        col.type_class.cast()
                    ));
                }
                sql.push_str(" WHERE ");
                sql.push_str(&clauses.join(" AND "));
            }

            if let Some(ord) = order {
                let dir = match ord {
                    Order::Asc => "ASC",
                    Order::Desc => "DESC",
                };
                sql.push_str(&format!(" ORDER BY _creation_time {dir}"));
            }

            match mode {
                QueryMode::Take => {
                    if let Some(n) = limit {
                        sql.push_str(&format!(" LIMIT {n}"));
                    }
                }
                QueryMode::First => sql.push_str(" LIMIT 1"),
                QueryMode::Unique => sql.push_str(" LIMIT 2"),
                QueryMode::Collect => {}
            }

            let rows = fetch_rows(&mut *conn, &sql, &binds, t).await?;
            let value = match mode {
                QueryMode::Take | QueryMode::Collect => Value::Array(rows),
                QueryMode::First => rows.into_iter().next().unwrap_or(Value::Null),
                QueryMode::Unique => {
                    if rows.len() > 1 {
                        return Err(SqlError::NotUnique);
                    }
                    rows.into_iter().next().unwrap_or(Value::Null)
                }
            };
            Ok((value, None))
        }

        DbOp::Insert { table: name, value } => {
            let t = table(catalog, name)?;
            let mut cols = Vec::new();
            let mut placeholders = Vec::new();
            let mut binds: Vec<Option<String>> = Vec::new();
            for (field, val) in value {
                let col = column(t, field)?;
                binds.push(json_to_bind(val, col));
                cols.push(col.column.clone());
                placeholders.push(format!("${}::{}", binds.len(), col.type_class.cast()));
            }
            let sql = if cols.is_empty() {
                format!("INSERT INTO {name} DEFAULT VALUES RETURNING {}", t.select_list())
            } else {
                format!(
                    "INSERT INTO {name} ({}) VALUES ({}) RETURNING {}",
                    cols.join(", "),
                    placeholders.join(", "),
                    t.select_list()
                )
            };
            let mut q = sqlx::query(&sql);
            for b in &binds {
                q = q.bind(b.clone());
            }
            let rows = q.fetch_all(&mut *conn).await?;
            let change = rows.first().map(|r| Change {
                table: TableId::new(name.clone()),
                key: pk_of(r),
                op: ChangeOp::Insert,
                new: Some(row_to_values(r, t)),
                old: None,
            });
            let value = match rows.first() {
                Some(r) => row_to_json(r, t)?,
                None => Value::Null,
            };
            // Insert returns the new id.
            let id = value.get("_id").and_then(|v| v.as_str()).map(str::to_string);
            Ok((id.map(Value::String).unwrap_or(Value::Null), change))
        }

        DbOp::Patch { table: name, id, fields } => {
            update(&mut *conn, catalog, name, id, fields).await
        }
        DbOp::Replace { table: name, id, value } => {
            update(&mut *conn, catalog, name, id, value).await
        }

        DbOp::Delete { table: name, id } => {
            let t = table(catalog, name)?;
            // RETURNING the leaving row so its pre-image can invalidate any
            // subscription whose filter matched it.
            let sql = format!(
                "DELETE FROM {name} WHERE _id = $1::uuid RETURNING {}",
                t.select_list()
            );
            let row = sqlx::query(&sql).bind(decode_id(id).to_string()).fetch_optional(&mut *conn).await?;
            let change = row.as_ref().map(|r| Change {
                table: TableId::new(name.to_string()),
                key: pk_of(r),
                op: ChangeOp::Delete,
                new: None,
                old: Some(row_to_values(r, t)),
            });
            Ok((Value::Null, change))
        }

        DbOp::GetCollab { table: name, id, field } => {
            let t = table(catalog, name)?;
            let col = column(t, field)?.column.clone();
            let sql = format!("SELECT {col} FROM {name} WHERE _id = $1::uuid");
            let row = sqlx::query(&sql)
                .bind(decode_id(id).to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let state: Vec<u8> = match &row {
                Some(r) => r.try_get::<Option<Vec<u8>>, _>(col.as_str())?.unwrap_or_default(),
                None => Vec::new(),
            };
            // Collab state crosses the wire base64-encoded.
            Ok((Value::String(B64.encode(state)), None))
        }

        DbOp::ApplyCollab { table: name, id, field, update } => {
            let t = table(catalog, name)?;
            let col = column(t, field)?.column.clone();
            let update_bytes =
                B64.decode(update).map_err(|e| SqlError::Db(sqlx::Error::Protocol(e.to_string())))?;

            // Load current state, merge via the CRDT, persist — all on `conn`
            // (inside the surrounding serializable tx for mutations).
            let sel = format!("SELECT {col} FROM {name} WHERE _id = $1::uuid");
            let cur = sqlx::query(&sel)
                .bind(decode_id(id).to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let state: Vec<u8> = match &cur {
                Some(r) => r.try_get::<Option<Vec<u8>>, _>(col.as_str())?.unwrap_or_default(),
                None => Vec::new(),
            };
            let merged = pulse_collab::apply_update(&state, &update_bytes)
                .map_err(|e| SqlError::Db(sqlx::Error::Protocol(e.to_string())))?;

            let upd = format!("UPDATE {name} SET {col} = $1 WHERE _id = $2::uuid");
            sqlx::query(&upd)
                .bind(merged.clone())
                .bind(decode_id(id).to_string())
                .execute(&mut *conn)
                .await?;

            // Coarse change: notify key/table subscribers of this row so they
            // re-fetch the merged state. (Precise Yjs-update delta push is a
            // follow-up slice.)
            let change = Change {
                table: TableId::new(name.to_string()),
                key: PrimaryKey::single(KeyValue::Uuid(
                    Uuid::parse_str(decode_id(id)).unwrap_or(Uuid::nil()),
                )),
                op: ChangeOp::Update,
                new: None,
                old: None,
            };
            Ok((Value::String(B64.encode(merged)), Some(change)))
        }

        DbOp::Raw { sql, params } => {
            // Wrap so arbitrary result columns decode dynamically as one jsonb value.
            let wrapped =
                format!("SELECT to_jsonb(__pulse_sub) AS j FROM ( {sql} ) AS __pulse_sub");
            let mut q = sqlx::query(&wrapped);
            for p in params {
                q = q.bind(raw_bind(p));
            }
            let rows = q.fetch_all(&mut *conn).await?;
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                out.push(r.try_get::<Value, _>("j")?);
            }
            Ok((Value::Array(out), None))
        }
    }
}

async fn update(
    conn: &mut PgConnection,
    catalog: &Catalog,
    name: &str,
    id: &str,
    fields: &Map<String, Value>,
) -> Result<(Value, Option<Change>), SqlError> {
    let t = table(catalog, name)?;
    if fields.is_empty() {
        return Ok((Value::Null, None));
    }
    let raw_id = decode_id(id).to_string();

    // Pre-image: needed so a row leaving a filter (e.g. channelId A→B) still
    // invalidates the old channel's subscriptions (matching evaluates new OR old).
    let pre_sql = format!("SELECT {} FROM {name} WHERE _id = $1::uuid", t.select_list());
    let old_row = sqlx::query(&pre_sql).bind(raw_id.clone()).fetch_optional(&mut *conn).await?;

    let mut sets = Vec::new();
    let mut binds: Vec<Option<String>> = Vec::new();
    for (field, val) in fields {
        let col = column(t, field)?;
        binds.push(json_to_bind(val, col));
        sets.push(format!("{} = ${}::{}", col.column, binds.len(), col.type_class.cast()));
    }
    binds.push(Some(raw_id));
    let sql = format!(
        "UPDATE {name} SET {} WHERE _id = ${}::uuid RETURNING {}",
        sets.join(", "),
        binds.len(),
        t.select_list()
    );
    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b.clone());
    }
    let new_row = q.fetch_optional(&mut *conn).await?;

    let change = new_row.as_ref().or(old_row.as_ref()).map(|r| Change {
        table: TableId::new(name.to_string()),
        key: pk_of(r),
        op: ChangeOp::Update,
        new: new_row.as_ref().map(|r| row_to_values(r, t)),
        old: old_row.as_ref().map(|r| row_to_values(r, t)),
    });
    Ok((Value::Null, change))
}
