use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgPool, postgres::PgRow, PgConnection, Row};
use uuid::Uuid;

use pulse_core::{
    Change, ChangeOp, Cond, Filter, FilterOp, KeyValue, Lsn, PrimaryKey, ReadSet, RowValues,
    TableId,
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
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    IsNull,
    IsNotNull,
}

impl PredOp {
    fn sql(self) -> &'static str {
        match self {
            PredOp::Eq => "=",
            PredOp::Neq => "<>",
            PredOp::Gt => ">",
            PredOp::Gte => ">=",
            PredOp::Lt => "<",
            PredOp::Lte => "<=",
            PredOp::Like => "LIKE",
            PredOp::Ilike => "ILIKE",
            PredOp::IsNull => "IS NULL",
            PredOp::IsNotNull => "IS NOT NULL",
        }
    }

    /// Unary operators take no right-hand operand (no bind placeholder).
    fn is_unary(self) -> bool {
        matches!(self, PredOp::IsNull | PredOp::IsNotNull)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Predicate {
    pub field: String,
    pub op: PredOp,
    pub value: Value,
}

/// A boolean filter expression tree from `.filter(q => ...)`: a leaf comparison,
/// or `and`/`or` of sub-expressions. Lowered to a SQL `WHERE` clause and, for
/// reactivity, to disjunctive normal form (an OR of AND-groups) that the read-set
/// records as multiple `Filter`s — so `OR`/`IN` keep precise invalidation.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FilterExpr {
    And { and: Vec<FilterExpr> },
    Or { or: Vec<FilterExpr> },
    Not { not: Box<FilterExpr> },
    Cmp(Predicate),
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

/// One sort key. `field` defaults to `_creationTime` when absent.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderKey {
    #[serde(default)]
    pub field: Option<String>,
    pub dir: Order,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryMode {
    Take,
    Collect,
    First,
    Unique,
}

/// A reactive aggregate over the query's filtered rows. `count` ignores `field`;
/// `sum` requires a numeric one. Reactivity reuses the query's filter read-set:
/// any change to a matching row re-runs and recomputes the scalar.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl From<AggFn> for pulse_core::AggFunc {
    fn from(f: AggFn) -> Self {
        match f {
            AggFn::Count => pulse_core::AggFunc::Count,
            AggFn::Sum => pulse_core::AggFunc::Sum,
            AggFn::Min => pulse_core::AggFunc::Min,
            AggFn::Max => pulse_core::AggFunc::Max,
            AggFn::Avg => pulse_core::AggFunc::Avg,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Agg {
    pub func: AggFn,
    #[serde(default)]
    pub field: Option<String>,
    /// `count(distinct field)` when true (count only).
    #[serde(default)]
    pub distinct: bool,
}

/// A `HAVING` predicate on a grouped aggregate's value (`<agg> <op> <value>`).
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Having {
    pub op: PredOp,
    pub value: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DbOp {
    Get {
        table: String,
        id: String,
    },
    /// Batched point lookup: many ids of one table in a single op. The result is a
    /// JSON array aligned to `ids` order (a missing/unparseable id → null). Lets the
    /// worker's DataLoader collapse N concurrent `get()`s into one round-trip while
    /// keeping per-id key-level read-set capture (M3 precision).
    GetMany {
        table: String,
        ids: Vec<String>,
    },
    Query {
        table: String,
        #[serde(default)]
        predicates: Vec<Predicate>,
        /// Rich filter trees from `.filter(q => ...)` (supports and/or/in). ANDed
        /// with `predicates`. Lowered to SQL + a DNF read-set for precise OR reactivity.
        #[serde(default)]
        filters: Vec<FilterExpr>,
        /// Sort keys (multi-column). Empty → unordered; each defaults to _creationTime.
        #[serde(default, rename = "orderBy")]
        order_by: Vec<OrderKey>,
        #[serde(default)]
        limit: Option<i64>,
        /// Rows to skip for offset pagination (applied with `limit`).
        #[serde(default)]
        offset: Option<i64>,
        /// When set, return a single aggregate scalar over the filtered rows
        /// instead of the rows themselves (order/limit/offset are ignored).
        #[serde(default)]
        aggregate: Option<Agg>,
        /// With `aggregate`, group by this field and return one `{key, value}` row
        /// per group. Reactivity is the same filter read-set as a scalar aggregate.
        #[serde(default, rename = "groupBy")]
        group_by: Option<String>,
        /// With `group_by`, keep only groups whose aggregate satisfies this `HAVING`.
        #[serde(default)]
        having: Option<Having>,
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
            | DbOp::GetMany { table, .. }
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
    catalog
        .table(name)
        .ok_or_else(|| SqlError::UnknownTable(name.to_string()))
}

fn column<'a>(t: &'a Table, field: &str) -> Result<&'a Column, SqlError> {
    t.column_by_field(field)
        .ok_or_else(|| SqlError::UnknownField {
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
        PgTypeClass::Int8 => s
            .parse::<i64>()
            .map(|n| json!(n))
            .unwrap_or(Value::String(s)),
        PgTypeClass::Float8 => s
            .parse::<f64>()
            .map(|n| json!(n))
            .unwrap_or(Value::String(s)),
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

/// Bind text params (same scheme as `fetch_rows`) and read a single i64 scalar
/// (e.g. `count(*)`).
async fn fetch_scalar_i64(
    conn: &mut PgConnection,
    sql: &str,
    binds: &[Option<String>],
) -> Result<i64, SqlError> {
    let mut q = sqlx::query_scalar::<_, i64>(sql);
    for b in binds {
        q = q.bind(b.clone());
    }
    Ok(q.fetch_one(conn).await?)
}

/// Bind text params and read a single nullable f64 (e.g. `sum`/`min`/`max`/`avg`,
/// which are NULL over an empty/all-NULL set).
async fn fetch_scalar_opt_f64(
    conn: &mut PgConnection,
    sql: &str,
    binds: &[Option<String>],
) -> Result<Option<f64>, SqlError> {
    let mut q = sqlx::query_scalar::<_, Option<f64>>(sql);
    for b in binds {
        q = q.bind(b.clone());
    }
    Ok(q.fetch_one(conn).await?)
}

/// The SQL aggregate expression (uncast) for a scalar or grouped aggregate.
fn agg_sql(t: &Table, agg: &Agg) -> Result<String, SqlError> {
    let col = |a: &Agg| -> Result<String, SqlError> {
        Ok(column(t, a.field.as_deref().unwrap_or_default())?
            .column
            .clone())
    };
    Ok(match agg.func {
        AggFn::Count => match (agg.distinct, agg.field.as_deref()) {
            (true, Some(f)) => format!("count(distinct {})", column(t, f)?.column),
            (false, Some(f)) => format!("count({})", column(t, f)?.column), // non-NULL count
            _ => "count(*)".to_string(),
        },
        // sum/min/max/avg are NULL over an empty/all-NULL set (SQL standard).
        AggFn::Sum => format!("sum({})", col(agg)?),
        AggFn::Min => format!("min({})", col(agg)?),
        AggFn::Max => format!("max({})", col(agg)?),
        AggFn::Avg => format!("avg({})", col(agg)?),
    })
}

/// Fetch grouped aggregate rows as `{key, value}` objects. Column 0 is the group
/// key (read as text, decoded via `keycol`), column 1 the aggregate (f64/null).
async fn fetch_grouped(
    conn: &mut PgConnection,
    sql: &str,
    binds: &[Option<String>],
    keycol: &Column,
) -> Result<Vec<Value>, SqlError> {
    let mut q = sqlx::query(sql);
    for b in binds {
        q = q.bind(b.clone());
    }
    let rows = q.fetch_all(conn).await?;
    rows.iter()
        .map(|r| {
            let k: Option<String> = r.try_get(0)?;
            let v: Option<f64> = r.try_get(1)?;
            Ok(json!({ "key": text_to_json(k, keycol), "value": v }))
        })
        .collect()
}

/// Render a filter expression tree to a parenthesized SQL boolean, pushing binds.
fn render_expr(
    e: &FilterExpr,
    t: &Table,
    binds: &mut Vec<Option<String>>,
) -> Result<String, SqlError> {
    match e {
        FilterExpr::Cmp(p) => {
            let col = column(t, &p.field)?;
            if p.op.is_unary() {
                return Ok(format!("{} {}", col.column, p.op.sql()));
            }
            binds.push(json_to_bind(&p.value, col));
            Ok(format!(
                "{} {} ${}::{}",
                col.column,
                p.op.sql(),
                binds.len(),
                col.type_class.cast()
            ))
        }
        FilterExpr::And { and } => {
            if and.is_empty() {
                return Ok("true".to_string());
            }
            let parts = and
                .iter()
                .map(|c| render_expr(c, t, binds))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        FilterExpr::Or { or } => {
            if or.is_empty() {
                return Ok("false".to_string());
            }
            let parts = or
                .iter()
                .map(|c| render_expr(c, t, binds))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        FilterExpr::Not { not } => Ok(format!("NOT ({})", render_expr(not, t, binds)?)),
    }
}

/// Map a predicate op to its read-set matcher op. `None` for ops the matcher
/// can't analyze precisely (`IS NULL`/`IS NOT NULL`) — the caller broadens.
fn pred_op_to_filter(op: PredOp) -> Option<FilterOp> {
    Some(match op {
        PredOp::Eq => FilterOp::Eq,
        PredOp::Neq => FilterOp::Neq,
        PredOp::Gt => FilterOp::Gt,
        PredOp::Gte => FilterOp::Gte,
        PredOp::Lt => FilterOp::Lt,
        PredOp::Lte => FilterOp::Lte,
        PredOp::Like => FilterOp::Like,
        PredOp::Ilike => FilterOp::Ilike,
        PredOp::IsNull | PredOp::IsNotNull => return None,
    })
}

/// Coerce a JSON predicate value to a `KeyValue` for the column, decoding ids.
/// Returns `None` for null / unorderable columns (float/jsonb) — the caller then
/// drops the condition, broadening the filter (safe over-approximation).
fn json_to_key_value(value: &Value, col: &Column) -> Option<KeyValue> {
    if value.is_null() {
        return None;
    }
    if col.id_ref.is_some() {
        return Uuid::parse_str(decode_id(value.as_str()?))
            .ok()
            .map(KeyValue::Uuid);
    }
    match col.type_class {
        PgTypeClass::Int8 => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .map(KeyValue::Int),
        PgTypeClass::Bool => value.as_bool().map(KeyValue::Bool),
        PgTypeClass::Text => value.as_str().map(|s| KeyValue::Text(s.to_string())),
        PgTypeClass::Uuid => value
            .as_str()
            .and_then(|s| Uuid::parse_str(decode_id(s)).ok())
            .map(KeyValue::Uuid),
        _ => None,
    }
}

/// Cap on DNF size — a filter expanding past this falls back to a coarse
/// table-level read rather than recording a huge OR of filters.
const DNF_CAP: usize = 64;

/// One coercible comparison as a single-conjunction DNF. An uncoercible value
/// (null / float / jsonb) becomes the empty conjunction (`TRUE`): in an AND it
/// drops out (broadening), in an OR it makes the branch match-all (→ coarse).
fn cmp_dnf(t: &Table, field: &str, op: PredOp, value: &Value) -> Vec<Vec<Cond>> {
    // Ops the matcher can't analyze (IS NULL / IS NOT NULL) → match-all term: in
    // an AND it drops (still precise on siblings), in an OR it broadens to coarse.
    let Some(fop) = pred_op_to_filter(op) else {
        return vec![vec![]];
    };
    match t
        .column_by_field(field)
        .and_then(|col| json_to_key_value(value, col))
    {
        Some(v) => vec![vec![Cond {
            field: field.to_string(),
            op: fop,
            value: v,
        }]],
        None => vec![vec![]],
    }
}

/// AND two DNFs (distribute): cartesian product of their conjunctions.
fn and_dnf(a: Vec<Vec<Cond>>, b: &[Vec<Cond>]) -> Option<Vec<Vec<Cond>>> {
    let mut out = Vec::new();
    for x in &a {
        for y in b {
            let mut conj = x.clone();
            conj.extend(y.iter().cloned());
            out.push(conj);
            if out.len() > DNF_CAP {
                return None;
            }
        }
    }
    Some(out)
}

/// The negation of a comparison op, or `None` when it can't be flipped precisely
/// (`LIKE`/`ILIKE` — `NOT LIKE` has no read-set matcher op, so it broadens).
fn flip_op(op: PredOp) -> Option<PredOp> {
    Some(match op {
        PredOp::Eq => PredOp::Neq,
        PredOp::Neq => PredOp::Eq,
        PredOp::Gt => PredOp::Lte,
        PredOp::Gte => PredOp::Lt,
        PredOp::Lt => PredOp::Gte,
        PredOp::Lte => PredOp::Gt,
        PredOp::IsNull => PredOp::IsNotNull,
        PredOp::IsNotNull => PredOp::IsNull,
        PredOp::Like | PredOp::Ilike => return None,
    })
}

/// DNF of a filter expression. `None` if it would exceed {@link DNF_CAP}.
fn expr_dnf(t: &Table, e: &FilterExpr) -> Option<Vec<Vec<Cond>>> {
    match e {
        FilterExpr::Cmp(p) => Some(cmp_dnf(t, &p.field, p.op, &p.value)),
        FilterExpr::And { and } => {
            let mut dnf = vec![Vec::new()];
            for c in and {
                dnf = and_dnf(dnf, &expr_dnf(t, c)?)?;
            }
            Some(dnf)
        }
        FilterExpr::Or { or } => {
            let mut out = Vec::new();
            for c in or {
                for conj in expr_dnf(t, c)? {
                    out.push(conj);
                    if out.len() > DNF_CAP {
                        return None;
                    }
                }
            }
            Some(out)
        }
        FilterExpr::Not { not } => negate_dnf(t, not),
    }
}

/// DNF of `NOT(e)`, pushing the negation to the leaves via De Morgan so OR/AND/NOT
/// all keep precise (flip-able) read-set filters.
fn negate_dnf(t: &Table, e: &FilterExpr) -> Option<Vec<Vec<Cond>>> {
    match e {
        FilterExpr::Cmp(p) => Some(match flip_op(p.op) {
            Some(op) => cmp_dnf(t, &p.field, op, &p.value),
            None => vec![vec![]], // un-negatable leaf → match-all (coarse)
        }),
        // NOT(a AND b ...) = (NOT a) OR (NOT b) ...
        FilterExpr::And { and } => {
            let mut out = Vec::new();
            for c in and {
                out.extend(negate_dnf(t, c)?);
                if out.len() > DNF_CAP {
                    return None;
                }
            }
            Some(out)
        }
        // NOT(a OR b ...) = (NOT a) AND (NOT b) ...
        FilterExpr::Or { or } => {
            let mut dnf = vec![Vec::new()];
            for c in or {
                dnf = and_dnf(dnf, &negate_dnf(t, c)?)?;
            }
            Some(dnf)
        }
        FilterExpr::Not { not } => expr_dnf(t, not), // double negation
    }
}

/// DNF for a whole query: `predicates` (flat AND) AND each `filters` tree.
fn query_dnf(
    t: &Table,
    predicates: &[Predicate],
    filters: &[FilterExpr],
) -> Option<Vec<Vec<Cond>>> {
    let mut dnf = vec![Vec::new()]; // TRUE
    for p in predicates {
        dnf = and_dnf(dnf, &cmp_dnf(t, &p.field, p.op, &p.value))?;
    }
    for f in filters {
        dnf = and_dnf(dnf, &expr_dnf(t, f)?)?;
    }
    Some(dnf)
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
        DbOp::GetMany { table: name, ids } => {
            // One key per id — value-identical to N separate Get reads, so a change
            // to any one row still invalidates with point-key precision (M3).
            let tid = TableId::new(name.clone());
            for id in ids {
                match Uuid::parse_str(decode_id(id)) {
                    Ok(uuid) => rs.add_key(tid.clone(), PrimaryKey::single(KeyValue::Uuid(uuid))),
                    Err(_) => rs.add_table(tid.clone()),
                }
            }
        }
        DbOp::Query {
            table: name,
            predicates,
            filters,
            aggregate,
            group_by,
            having,
            ..
        } => {
            let tid = TableId::new(name.clone());
            if predicates.is_empty() && filters.is_empty() {
                // No filter. A full-doc read depends on the whole table; an
                // aggregate (e.g. unfiltered count) still depends on the table for
                // membership, so both fall back to a table-wildcard read.
                rs.add_table(tid);
                return;
            }
            let Some(t) = catalog.table(name) else {
                rs.add_table(tid);
                return;
            };
            // Columns the result depends on beyond membership: full-doc reads → None
            // (any change matters); aggregates → the group key + aggregated field
            // (a bare count() → empty, so value-only updates to matching rows prune).
            let read_cols: Option<Vec<String>> = match aggregate {
                None => None,
                Some(agg) => {
                    let mut cols = Vec::new();
                    if let Some(k) = group_by {
                        cols.push(k.clone());
                    }
                    let bare_count =
                        matches!(agg.func, AggFn::Count) && !agg.distinct && agg.field.is_none();
                    if !bare_count {
                        if let Some(f) = &agg.field {
                            cols.push(f.clone());
                        }
                    }
                    Some(cols)
                }
            };
            // A single *scalar* aggregate (no group_by/having) can be maintained
            // incrementally by the reactor. Grouped/having results are arrays —
            // never IVM-able, so record no aggregate descriptor for them.
            let agg_meta: Option<pulse_core::Aggregate> = match aggregate {
                Some(agg) if group_by.is_none() && having.is_none() => {
                    Some(pulse_core::Aggregate {
                        func: agg.func.into(),
                        field: agg.field.clone(),
                        distinct: agg.distinct,
                    })
                }
                _ => None,
            };
            // Convert predicates + filter trees to DNF. Each conjunction becomes a
            // precise `Filter` (an OR across them). An empty conjunction means
            // "match anything" (uncoercible value), and `None` means it grew past
            // the cap — both fall back to a coarse table read (never a miss).
            match query_dnf(t, predicates, filters) {
                Some(dnf) if !dnf.is_empty() && !dnf.iter().any(|c| c.is_empty()) => {
                    for conds in dnf {
                        rs.add_filter(
                            tid.clone(),
                            Filter {
                                conds,
                                read_cols: read_cols.clone(),
                                aggregate: agg_meta.clone(),
                            },
                        );
                    }
                }
                _ => rs.add_table(tid),
            }
        }
        DbOp::Raw { .. } => {
            // Raw SQL reads are opaque → conservatively depend on every table.
            for name in catalog.tables.keys() {
                rs.add_table(TableId::new(name.clone()));
            }
        }
        DbOp::GetCollab {
            table: name, id, ..
        } => {
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

/// Coerce a column's value, read as Postgres `::text`, into a filterable
/// `KeyValue` — mirroring the document builder's read decoding. Ids decode to
/// their raw uuid so they compare equal to predicate values. Returns `None` for
/// an absent value or a class we don't index (float/jsonb/timestamptz/null), so
/// a filter on such a column degrades to a table-wildcard.
///
/// Shared by the in-engine capture path (`row_to_values`) and the WAL/`pgoutput`
/// consumer so both produce byte-identical `RowValues` — the same `ReadSet`
/// matcher must fire whether a change came from an in-engine `RETURNING` row or
/// from logical replication.
pub fn text_to_key_value(text: Option<&str>, col: &Column) -> Option<KeyValue> {
    let s = text?;
    if col.id_ref.is_some() {
        return Uuid::parse_str(s).ok().map(KeyValue::Uuid);
    }
    match col.type_class {
        PgTypeClass::Int8 => s.parse::<i64>().ok().map(KeyValue::Int),
        // double precision is carried in the image (for float aggregates), even
        // though it can't be a primary key or a filter-predicate value.
        PgTypeClass::Float8 => s.parse::<f64>().ok().map(KeyValue::Float),
        PgTypeClass::Bool => Some(KeyValue::Bool(s == "true" || s == "t")),
        PgTypeClass::Uuid => Uuid::parse_str(s).ok().map(KeyValue::Uuid),
        PgTypeClass::Text => Some(KeyValue::Text(s.to_string())),
        _ => None,
    }
}

/// The filterable columns of a row as a `RowValues` image (ids decoded to raw
/// uuids so they compare equal to predicate values). Float/jsonb/timestamptz and
/// nulls are omitted — a filter on such a column degrades to a table-wildcard.
fn row_to_values(row: &PgRow, t: &Table) -> RowValues {
    let mut out = RowValues::new();
    for col in &t.columns {
        let text: Option<String> = row.try_get(col.column.as_str()).ok().flatten();
        if let Some(kv) = text_to_key_value(text.as_deref(), col) {
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

/// The current WAL insert position, used to stamp a committed change-set with an
/// approximate commit LSN. Queried as the last statement of a mutation's
/// transaction (before COMMIT): it is monotonically non-decreasing, so for
/// serialized/conflicting commits it preserves commit order. It is an upper bound
/// on positions written so far, not the exact commit-record LSN — a globally
/// exact per-commit LSN needs a logical-replication consumer.
pub async fn current_wal_lsn(conn: &mut PgConnection) -> Result<Lsn, sqlx::Error> {
    let (s,): (String,) = sqlx::query_as("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut *conn)
        .await?;
    s.parse()
        .map_err(|e: pulse_core::ParseLsnError| sqlx::Error::Decode(Box::new(e)))
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
        DbOp::GetMany { table: name, ids } => {
            let t = table(catalog, name)?;
            // Decode + parse; an unparseable id can't match a row → null at its index.
            let valid: Vec<String> = ids
                .iter()
                .filter_map(|id| Uuid::parse_str(decode_id(id)).ok().map(|u| u.to_string()))
                .collect();
            if valid.is_empty() {
                return Ok((
                    Value::Array(ids.iter().map(|_| Value::Null).collect()),
                    None,
                ));
            }
            // One round-trip: `_id = ANY(ARRAY[$1::uuid, …])` (order-independent, dedups).
            let placeholders: Vec<String> =
                (1..=valid.len()).map(|i| format!("${i}::uuid")).collect();
            let sql = format!(
                "SELECT {} FROM {} WHERE _id = ANY(ARRAY[{}])",
                t.select_list(),
                name,
                placeholders.join(", ")
            );
            let binds: Vec<Option<String>> = valid.into_iter().map(Some).collect();
            let rows = fetch_rows(&mut *conn, &sql, &binds, t).await?;
            // Key rows by their raw uuid, then align to the requested `ids` order so
            // the worker can distribute results by index (missing → null).
            let mut by_id: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            for row in rows {
                if let Some(enc) = row.get("_id").and_then(|v| v.as_str()) {
                    if let Ok(u) = Uuid::parse_str(decode_id(enc)) {
                        by_id.insert(u.to_string(), row);
                    }
                }
            }
            let out: Vec<Value> = ids
                .iter()
                .map(|id| {
                    Uuid::parse_str(decode_id(id))
                        .ok()
                        .and_then(|u| by_id.get(&u.to_string()).cloned())
                        .unwrap_or(Value::Null)
                })
                .collect();
            Ok((Value::Array(out), None))
        }

        DbOp::Query {
            table: name,
            predicates,
            filters,
            order_by,
            limit,
            offset,
            aggregate,
            group_by,
            having,
            mode,
        } => {
            let t = table(catalog, name)?;

            // Shared WHERE clause + binds: flat `predicates` (from withIndex) AND
            // the rich filter trees (from `.filter(q => ...)`, supports and/or/in).
            let mut where_sql = String::new();
            let mut binds: Vec<Option<String>> = Vec::new();
            let mut clauses: Vec<String> = Vec::new();
            for p in predicates {
                let col = column(t, &p.field)?;
                if p.op.is_unary() {
                    clauses.push(format!("{} {}", col.column, p.op.sql()));
                    continue;
                }
                binds.push(json_to_bind(&p.value, col));
                clauses.push(format!(
                    "{} {} ${}::{}",
                    col.column,
                    p.op.sql(),
                    binds.len(),
                    col.type_class.cast()
                ));
            }
            for f in filters {
                clauses.push(render_expr(f, t, &mut binds)?);
            }
            if !clauses.is_empty() {
                where_sql = format!(" WHERE {}", clauses.join(" AND "));
            }

            // Aggregate path: one scalar over the filtered rows. order/limit/offset
            // don't apply. Reactivity is unchanged — `capture_reads` folds the same
            // filters into the read-set, so a matching write recomputes it. `count`
            // is 0 over an empty set; sum/min/max/avg are NULL (SQL standard).
            if let Some(agg) = aggregate {
                let expr = agg_sql(t, agg)?;
                // Grouped aggregate: one {key, value} row per group. Same filter
                // read-set as the scalar path → filter-precise reactivity.
                if let Some(gkey) = group_by {
                    let keycol = column(t, gkey)?;
                    // HAVING filters groups by the aggregate value (numeric literal,
                    // safe to inline). It doesn't change the read-set.
                    let having_sql = match having {
                        Some(h) => format!(" HAVING ({expr}) {} {}", h.op.sql(), h.value),
                        None => String::new(),
                    };
                    let sql = format!(
                        "SELECT {0}::text, ({expr})::double precision FROM {name}{where_sql} GROUP BY {0}{having_sql}",
                        keycol.column,
                    );
                    let rows = fetch_grouped(&mut *conn, &sql, &binds, keycol).await?;
                    return Ok((Value::Array(rows), None));
                }
                let value = match agg.func {
                    AggFn::Count => {
                        let sql = format!("SELECT {expr}::bigint FROM {name}{where_sql}");
                        json!(fetch_scalar_i64(&mut *conn, &sql, &binds).await?)
                    }
                    // sum/min/max/avg → NULL over an empty/all-NULL set.
                    _ => {
                        let sql = format!("SELECT {expr}::double precision FROM {name}{where_sql}");
                        match fetch_scalar_opt_f64(&mut *conn, &sql, &binds).await? {
                            Some(n) => json!(n),
                            None => Value::Null,
                        }
                    }
                };
                return Ok((value, None));
            }

            // Row path.
            let mut sql = format!("SELECT {} FROM {}{}", t.select_list(), name, where_sql);

            // Multi-column ORDER BY. Ordering doesn't change which rows are read,
            // so the read-set / reactivity is unaffected. Each key resolves its
            // field against the catalog, defaulting to creation time.
            if !order_by.is_empty() {
                let mut keys = Vec::new();
                for k in order_by {
                    let col = match &k.field {
                        Some(field) => column(t, field)?.column.as_str(),
                        None => "_creation_time",
                    };
                    let dir = match k.dir {
                        Order::Asc => "ASC",
                        Order::Desc => "DESC",
                    };
                    keys.push(format!("{col} {dir}"));
                }
                sql.push_str(&format!(" ORDER BY {}", keys.join(", ")));
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

            // Offset pagination (applied after LIMIT in SQL text; Postgres allows
            // OFFSET with or without LIMIT). Only meaningful with a stable order.
            if let Some(n) = offset {
                if *n > 0 {
                    sql.push_str(&format!(" OFFSET {n}"));
                }
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
                format!(
                    "INSERT INTO {name} DEFAULT VALUES RETURNING {}",
                    t.select_list()
                )
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
            let id = value
                .get("_id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Ok((id.map(Value::String).unwrap_or(Value::Null), change))
        }

        DbOp::Patch {
            table: name,
            id,
            fields,
        } => update(&mut *conn, catalog, name, id, fields).await,
        DbOp::Replace {
            table: name,
            id,
            value,
        } => replace(&mut *conn, catalog, name, id, value).await,

        DbOp::Delete { table: name, id } => {
            let t = table(catalog, name)?;
            // RETURNING the leaving row so its pre-image can invalidate any
            // subscription whose filter matched it.
            let sql = format!(
                "DELETE FROM {name} WHERE _id = $1::uuid RETURNING {}",
                t.select_list()
            );
            let row = sqlx::query(&sql)
                .bind(decode_id(id).to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let change = row.as_ref().map(|r| Change {
                table: TableId::new(name.to_string()),
                key: pk_of(r),
                op: ChangeOp::Delete,
                new: None,
                old: Some(row_to_values(r, t)),
            });
            Ok((Value::Null, change))
        }

        DbOp::GetCollab {
            table: name,
            id,
            field,
        } => {
            let t = table(catalog, name)?;
            let col = column(t, field)?.column.clone();
            let sql = format!("SELECT {col} FROM {name} WHERE _id = $1::uuid");
            let row = sqlx::query(&sql)
                .bind(decode_id(id).to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let state: Vec<u8> = match &row {
                Some(r) => r
                    .try_get::<Option<Vec<u8>>, _>(col.as_str())?
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            // Collab state crosses the wire base64-encoded.
            Ok((Value::String(B64.encode(state)), None))
        }

        DbOp::ApplyCollab {
            table: name,
            id,
            field,
            update,
        } => {
            let t = table(catalog, name)?;
            let col = column(t, field)?.column.clone();
            let update_bytes = B64
                .decode(update)
                .map_err(|e| SqlError::Db(sqlx::Error::Protocol(e.to_string())))?;

            // Load current state, merge via the CRDT, persist — all on `conn`
            // (inside the surrounding serializable tx for mutations).
            let sel = format!("SELECT {col} FROM {name} WHERE _id = $1::uuid");
            let cur = sqlx::query(&sel)
                .bind(decode_id(id).to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let state: Vec<u8> = match &cur {
                Some(r) => r
                    .try_get::<Option<Vec<u8>>, _>(col.as_str())?
                    .unwrap_or_default(),
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
    let pre_sql = format!(
        "SELECT {} FROM {name} WHERE _id = $1::uuid",
        t.select_list()
    );
    let old_row = sqlx::query(&pre_sql)
        .bind(raw_id.clone())
        .fetch_optional(&mut *conn)
        .await?;

    let mut sets = Vec::new();
    let mut binds: Vec<Option<String>> = Vec::new();
    for (field, val) in fields {
        let col = column(t, field)?;
        binds.push(json_to_bind(val, col));
        sets.push(format!(
            "{} = ${}::{}",
            col.column,
            binds.len(),
            col.type_class.cast()
        ));
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

/// True replace: the row becomes exactly `value`. Every user column is written —
/// provided fields to their value, omitted ones to `NULL` — while the system
/// columns (`_id`, `_creationTime`) are preserved. This is the semantic distinct
/// from `patch` (which only touches provided fields). A `NOT NULL` column omitted
/// from `value` surfaces a constraint error, since a full replace must supply
/// every required field. Unknown fields are rejected like `patch`.
async fn replace(
    conn: &mut PgConnection,
    catalog: &Catalog,
    name: &str,
    id: &str,
    value: &Map<String, Value>,
) -> Result<(Value, Option<Change>), SqlError> {
    let t = table(catalog, name)?;
    // Reject unknown fields up front (a typo fails loudly, as in patch).
    for field in value.keys() {
        column(t, field)?;
    }
    let raw_id = decode_id(id).to_string();

    // Pre-image (for invalidation of filters the old row matched) doubles as the
    // existence check: no row → nothing to replace.
    let pre_sql = format!(
        "SELECT {} FROM {name} WHERE _id = $1::uuid",
        t.select_list()
    );
    let old_row = sqlx::query(&pre_sql)
        .bind(raw_id.clone())
        .fetch_optional(&mut *conn)
        .await?;
    if old_row.is_none() {
        return Ok((Value::Null, None));
    }

    let mut sets = Vec::new();
    let mut binds: Vec<Option<String>> = Vec::new();
    for col in &t.columns {
        if col.field.starts_with('_') {
            continue; // system column — never overwritten by a replace
        }
        match value.get(&col.field) {
            Some(val) => {
                binds.push(json_to_bind(val, col));
                sets.push(format!(
                    "{} = ${}::{}",
                    col.column,
                    binds.len(),
                    col.type_class.cast()
                ));
            }
            None => sets.push(format!("{} = NULL", col.column)), // omitted → nulled
        }
    }
    if sets.is_empty() {
        return Ok((Value::Null, None));
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

/// For a scalar `avg(field)` query, compute the `sum`+`count` behind it so the
/// reactor can seed incremental avg maintenance (`avg = sum/count`). Returns None
/// for any non-avg / grouped / distinct shape. The WHERE clause mirrors the Query
/// execute path exactly, so the seed matches the rows the avg was computed over.
pub async fn fetch_avg_seed(
    conn: &mut PgConnection,
    catalog: &Catalog,
    op: &DbOp,
) -> Result<Option<(f64, i64)>, SqlError> {
    let DbOp::Query {
        table: name,
        predicates,
        filters,
        aggregate: Some(agg),
        group_by: None,
        ..
    } = op
    else {
        return Ok(None);
    };
    if !matches!(agg.func, AggFn::Avg) || agg.distinct {
        return Ok(None);
    }
    let Some(field) = agg.field.as_deref() else {
        return Ok(None);
    };
    let t = table(catalog, name)?;
    let col = column(t, field)?;

    let mut binds: Vec<Option<String>> = Vec::new();
    let mut clauses: Vec<String> = Vec::new();
    for p in predicates {
        let pcol = column(t, &p.field)?;
        if p.op.is_unary() {
            clauses.push(format!("{} {}", pcol.column, p.op.sql()));
            continue;
        }
        binds.push(json_to_bind(&p.value, pcol));
        clauses.push(format!(
            "{} {} ${}::{}",
            pcol.column,
            p.op.sql(),
            binds.len(),
            pcol.type_class.cast()
        ));
    }
    for f in filters {
        clauses.push(render_expr(f, t, &mut binds)?);
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    // `coalesce(sum, 0)` so the empty set seeds (0, 0) rather than NULL; count is 0
    // there, which the reactor reads as "empty → NULL avg".
    let sql = format!(
        "SELECT coalesce(sum({0}), 0)::double precision, count({0})::bigint FROM {1}{2}",
        col.column, name, where_sql
    );
    let mut q = sqlx::query_as::<_, (f64, i64)>(&sql);
    for b in &binds {
        q = q.bind(b.clone());
    }
    let (sum, count) = q.fetch_one(&mut *conn).await?;
    Ok(Some((sum, count)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, Table};
    use std::collections::HashMap;

    fn col(column: &str, field: &str, type_class: PgTypeClass, id_ref: Option<&str>) -> Column {
        Column {
            column: column.to_string(),
            field: field.to_string(),
            type_class,
            nullable: true,
            id_ref: id_ref.map(str::to_string),
        }
    }

    // Slice 0: lock the text→KeyValue coercion shared by row_to_values (in-engine
    // capture) and the WAL/pgoutput decoder, so both build identical RowValues.
    #[test]
    fn text_to_key_value_covers_each_class() {
        let u = Uuid::nil();
        // id_ref columns decode to the raw uuid (matches predicate-bound ids).
        let id = col(
            "channel_id",
            "channelId",
            PgTypeClass::Uuid,
            Some("channels"),
        );
        assert_eq!(
            text_to_key_value(Some(&u.to_string()), &id),
            Some(KeyValue::Uuid(u))
        );
        // int8 (parse failure → None, degrades to wildcard).
        let n = col("n", "n", PgTypeClass::Int8, None);
        assert_eq!(text_to_key_value(Some("42"), &n), Some(KeyValue::Int(42)));
        assert_eq!(text_to_key_value(Some("nan"), &n), None);
        // bool: Postgres `::text` yields `t`/`f`; the engine also accepts `true`.
        let b = col("done", "done", PgTypeClass::Bool, None);
        assert_eq!(text_to_key_value(Some("t"), &b), Some(KeyValue::Bool(true)));
        assert_eq!(
            text_to_key_value(Some("true"), &b),
            Some(KeyValue::Bool(true))
        );
        assert_eq!(
            text_to_key_value(Some("f"), &b),
            Some(KeyValue::Bool(false))
        );
        // text passes through.
        let t = col("body", "body", PgTypeClass::Text, None);
        assert_eq!(
            text_to_key_value(Some("hi"), &t),
            Some(KeyValue::Text("hi".into()))
        );
        // float8 is now captured (for float aggregates), though it can't be a key
        // or a filter-predicate value; a parse failure degrades to None.
        let f = col("amount", "amount", PgTypeClass::Float8, None);
        assert_eq!(
            text_to_key_value(Some("1.5"), &f),
            Some(KeyValue::Float(1.5))
        );
        assert_eq!(text_to_key_value(Some("xyz"), &f), None);
        // absent value → None.
        assert_eq!(text_to_key_value(None, &t), None);
    }

    /// `messages` with an `_id` and a `channelId` id-ref to `channels`.
    fn messages_table() -> Table {
        Table::from_columns(
            "messages",
            vec![
                col("_id", "_id", PgTypeClass::Uuid, Some("messages")),
                col(
                    "channel_id",
                    "channelId",
                    PgTypeClass::Uuid,
                    Some("channels"),
                ),
            ],
        )
    }

    /// `channels` with just an `_id`.
    fn channels_table() -> Table {
        Table::from_columns(
            "channels",
            vec![col("_id", "_id", PgTypeClass::Uuid, Some("channels"))],
        )
    }

    fn catalog_messages() -> Catalog {
        let mut c = Catalog::default();
        c.tables.insert("messages".to_string(), messages_table());
        c
    }

    /// `messages` with a text `body` and an int `n` — for WHERE-clause rendering.
    fn body_table() -> Table {
        Table::from_columns(
            "messages",
            vec![
                col("body", "body", PgTypeClass::Text, None),
                col("n", "n", PgTypeClass::Int8, None),
            ],
        )
    }

    // Injection safety: a comparison value is ALWAYS a `$N` placeholder with the
    // raw value pushed to binds — never interpolated into the SQL text.
    #[test]
    fn render_expr_parameterizes_values_no_injection() {
        let t = body_table();
        let mut binds = Vec::new();
        let evil = "x'; DROP TABLE messages;--";
        let e = FilterExpr::Cmp(Predicate {
            field: "body".into(),
            op: PredOp::Eq,
            value: json!(evil),
        });
        let sql = render_expr(&e, &t, &mut binds).unwrap();
        assert_eq!(sql, "body = $1::text"); // placeholder, not the literal
        assert_eq!(binds, vec![Some(evil.to_string())]); // raw value is bound
        assert!(!sql.contains("DROP")); // payload never reaches the SQL text
    }

    #[test]
    fn render_expr_unary_is_null_takes_no_bind() {
        let t = body_table();
        let mut binds = Vec::new();
        let e = FilterExpr::Cmp(Predicate {
            field: "body".into(),
            op: PredOp::IsNull,
            value: Value::Null,
        });
        let sql = render_expr(&e, &t, &mut binds).unwrap();
        assert_eq!(sql, "body IS NULL");
        assert!(binds.is_empty()); // a unary op has no right-hand operand to bind
    }

    // Empty boolean groups: AND of nothing is TRUE (match all), OR of nothing is
    // FALSE (match none) — the `in([])` / `not(in([]))` corner.
    #[test]
    fn render_expr_empty_and_is_true_empty_or_is_false() {
        let t = body_table();
        let mut binds = Vec::new();
        assert_eq!(
            render_expr(&FilterExpr::And { and: vec![] }, &t, &mut binds).unwrap(),
            "true"
        );
        assert_eq!(
            render_expr(&FilterExpr::Or { or: vec![] }, &t, &mut binds).unwrap(),
            "false"
        );
        assert!(binds.is_empty());
    }

    #[test]
    fn render_expr_composes_and_or_not_with_sequential_binds() {
        let t = body_table();
        let mut binds = Vec::new();
        let e = FilterExpr::Not {
            not: Box::new(FilterExpr::Or {
                or: vec![
                    FilterExpr::Cmp(Predicate {
                        field: "body".into(),
                        op: PredOp::Eq,
                        value: json!("hi"),
                    }),
                    FilterExpr::Cmp(Predicate {
                        field: "n".into(),
                        op: PredOp::Gt,
                        value: json!(5),
                    }),
                ],
            }),
        };
        let sql = render_expr(&e, &t, &mut binds).unwrap();
        assert_eq!(sql, "NOT ((body = $1::text OR n > $2::int8))");
        assert_eq!(binds, vec![Some("hi".to_string()), Some("5".to_string())]);
    }

    // ── DNF lowering: the boolean algebra that drives reactive read-set precision.
    // A bug here means under-invalidation (silently stale reads) or coarse fallback.
    fn abc_table() -> Table {
        Table::from_columns(
            "t",
            vec![
                col("a", "a", PgTypeClass::Int8, None),
                col("b", "b", PgTypeClass::Int8, None),
                col("c", "c", PgTypeClass::Int8, None),
            ],
        )
    }
    fn cmp(field: &str, op: PredOp, v: i64) -> FilterExpr {
        FilterExpr::Cmp(Predicate {
            field: field.into(),
            op,
            value: json!(v),
        })
    }
    fn cond(field: &str, op: FilterOp, v: i64) -> Cond {
        Cond {
            field: field.into(),
            op,
            value: KeyValue::Int(v),
        }
    }

    #[test]
    fn expr_dnf_distributes_or_over_and() {
        // (a=1 OR b=2) AND c=3  →  (a=1 AND c=3) OR (b=2 AND c=3)
        let e = FilterExpr::And {
            and: vec![
                FilterExpr::Or {
                    or: vec![cmp("a", PredOp::Eq, 1), cmp("b", PredOp::Eq, 2)],
                },
                cmp("c", PredOp::Eq, 3),
            ],
        };
        assert_eq!(
            expr_dnf(&abc_table(), &e).unwrap(),
            vec![
                vec![cond("a", FilterOp::Eq, 1), cond("c", FilterOp::Eq, 3)],
                vec![cond("b", FilterOp::Eq, 2), cond("c", FilterOp::Eq, 3)],
            ]
        );
    }

    #[test]
    fn expr_dnf_de_morgan_negates_and_and_or() {
        let t = abc_table();
        // NOT(a=1 AND b=2) = (a<>1) OR (b<>2)
        let not_and = FilterExpr::Not {
            not: Box::new(FilterExpr::And {
                and: vec![cmp("a", PredOp::Eq, 1), cmp("b", PredOp::Eq, 2)],
            }),
        };
        assert_eq!(
            expr_dnf(&t, &not_and).unwrap(),
            vec![
                vec![cond("a", FilterOp::Neq, 1)],
                vec![cond("b", FilterOp::Neq, 2)],
            ]
        );
        // NOT(a=1 OR b=2) = (a<>1) AND (b<>2)
        let not_or = FilterExpr::Not {
            not: Box::new(FilterExpr::Or {
                or: vec![cmp("a", PredOp::Eq, 1), cmp("b", PredOp::Eq, 2)],
            }),
        };
        assert_eq!(
            expr_dnf(&t, &not_or).unwrap(),
            vec![vec![
                cond("a", FilterOp::Neq, 1),
                cond("b", FilterOp::Neq, 2)
            ]]
        );
    }

    #[test]
    fn expr_dnf_double_negation_cancels() {
        // NOT(NOT(a>5)) = a>5 (not the flipped a<=5)
        let e = FilterExpr::Not {
            not: Box::new(FilterExpr::Not {
                not: Box::new(cmp("a", PredOp::Gt, 5)),
            }),
        };
        assert_eq!(
            expr_dnf(&abc_table(), &e).unwrap(),
            vec![vec![cond("a", FilterOp::Gt, 5)]]
        );
    }

    #[test]
    fn expr_dnf_past_the_cap_degrades_to_coarse_none() {
        // AND of seven 2-way ORs → 2^7 = 128 conjunctions > DNF_CAP(64) → None, so
        // the caller broadens to a coarse table read instead of a giant OR.
        let ors: Vec<FilterExpr> = (0..7)
            .map(|_| FilterExpr::Or {
                or: vec![cmp("a", PredOp::Eq, 1), cmp("b", PredOp::Eq, 2)],
            })
            .collect();
        assert!(expr_dnf(&abc_table(), &FilterExpr::And { and: ors }).is_none());
    }

    fn catalog_messages_channels() -> Catalog {
        let mut c = Catalog::default();
        c.tables.insert("messages".to_string(), messages_table());
        c.tables.insert("channels".to_string(), channels_table());
        c
    }

    const UUID_A: &str = "00000000-0000-0000-0000-000000000001";
    const UUID_B: &str = "00000000-0000-0000-0000-0000000000bb";

    fn uuid(s: &str) -> Uuid {
        Uuid::parse_str(s).unwrap()
    }

    /// An insert change to `messages` whose new image sets `channelId`.
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

    fn eq_pred(field: &str, value: Value) -> Predicate {
        Predicate {
            field: field.to_string(),
            op: PredOp::Eq,
            value,
        }
    }

    #[test]
    fn query_with_id_pred_captures_filter() {
        let catalog = catalog_messages();
        let op = DbOp::Query {
            table: "messages".to_string(),
            predicates: vec![eq_pred("channelId", json!(format!("channels:{UUID_A}")))],
            filters: vec![],
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

        // Precise filter, not a table-wildcard.
        assert!(rs.tables.is_empty(), "expected no table-wildcard: {rs:?}");
        let filters = rs
            .filters
            .get(&TableId::new("messages"))
            .expect("messages filter present");
        assert_eq!(filters.len(), 1);
        assert_eq!(
            filters[0].conds,
            vec![Cond {
                field: "channelId".to_string(),
                op: FilterOp::Eq,
                value: KeyValue::Uuid(uuid(UUID_A)),
            }]
        );

        // The decoded uuid drives precise change matching.
        assert!(
            rs.matches_change(&change_into(uuid(UUID_A))),
            "same-channel change must match"
        );
        assert!(
            !rs.matches_change(&change_into(uuid(UUID_B))),
            "foreign-channel change must be pruned"
        );
    }

    #[test]
    fn query_empty_predicates_is_table_wildcard() {
        let catalog = catalog_messages();
        let op = DbOp::Query {
            table: "messages".to_string(),
            predicates: vec![],
            filters: vec![],
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

        assert!(rs.tables.contains(&TableId::new("messages")));
        assert!(rs.filters.is_empty(), "no precise filter for full scan");
    }

    #[test]
    fn query_float_predicate_dropped_broadens() {
        // A table with a float column alongside the id column.
        let t = Table::from_columns(
            "messages",
            vec![
                col(
                    "channel_id",
                    "channelId",
                    PgTypeClass::Uuid,
                    Some("channels"),
                ),
                col("score", "score", PgTypeClass::Float8, None),
            ],
        );
        let mut catalog = Catalog::default();
        catalog.tables.insert("messages".to_string(), t);

        let op = DbOp::Query {
            table: "messages".to_string(),
            predicates: vec![
                eq_pred("channelId", json!(format!("channels:{UUID_A}"))),
                eq_pred("score", json!(1.5)),
            ],
            filters: vec![],
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

        let filters = rs
            .filters
            .get(&TableId::new("messages"))
            .expect("filter still present");
        // The float cond is dropped (json_to_key_value → None); only channelId remains.
        assert_eq!(filters[0].conds.len(), 1, "float cond dropped: {filters:?}");
        assert_eq!(filters[0].conds[0].field, "channelId");
    }

    #[test]
    fn raw_depends_on_every_table() {
        let catalog = catalog_messages_channels();
        let op = DbOp::Raw {
            sql: "SELECT count(*) FROM messages".to_string(),
            params: vec![],
        };

        let mut rs = ReadSet::new();
        capture_reads(&op, &catalog, &mut rs);

        assert!(rs.tables.contains(&TableId::new("messages")));
        assert!(rs.tables.contains(&TableId::new("channels")));
        assert!(
            rs.matches_change(&change_into(Uuid::nil())),
            "raw read must match an insert to messages"
        );
    }

    #[test]
    fn get_valid_uuid_is_key_invalid_is_table() {
        let catalog = catalog_messages();

        // Valid table-qualified id → key-level read.
        let mut rs = ReadSet::new();
        capture_reads(
            &DbOp::Get {
                table: "messages".to_string(),
                id: format!("messages:{UUID_A}"),
            },
            &catalog,
            &mut rs,
        );
        let keys = rs.keys.get(&TableId::new("messages")).expect("key present");
        assert!(keys.contains(&PrimaryKey::single(KeyValue::Uuid(uuid(UUID_A)))));
        assert!(rs.tables.is_empty(), "valid id is key, not table");

        // Non-uuid id → coarse table read.
        let mut rs2 = ReadSet::new();
        capture_reads(
            &DbOp::Get {
                table: "messages".to_string(),
                id: "not-a-uuid".to_string(),
            },
            &catalog,
            &mut rs2,
        );
        assert!(rs2.tables.contains(&TableId::new("messages")));
        assert!(rs2.keys.is_empty(), "invalid id falls back to table");
    }

    #[test]
    fn getmany_capture_matches_separate_gets() {
        // Slice 0 — the M3 precision invariant: a batched GetMany must capture the
        // SAME read-set as N separate Get ops, so batching is invisible to invalidation.
        let catalog = catalog_messages();
        let ids = vec![
            format!("messages:{UUID_A}"),
            format!("messages:{UUID_B}"),
            "not-a-uuid".to_string(),
        ];

        let mut batched = ReadSet::new();
        capture_reads(
            &DbOp::GetMany {
                table: "messages".to_string(),
                ids: ids.clone(),
            },
            &catalog,
            &mut batched,
        );

        let mut separate = ReadSet::new();
        for id in &ids {
            capture_reads(
                &DbOp::Get {
                    table: "messages".to_string(),
                    id: id.clone(),
                },
                &catalog,
                &mut separate,
            );
        }

        assert_eq!(batched.keys, separate.keys, "keys must match N gets");
        assert_eq!(batched.tables, separate.tables, "tables must match N gets");

        // Spelled out: two point keys + one table-wildcard (the unparseable id).
        let keys = batched.keys.get(&TableId::new("messages")).expect("keys");
        assert!(keys.contains(&PrimaryKey::single(KeyValue::Uuid(uuid(UUID_A)))));
        assert!(keys.contains(&PrimaryKey::single(KeyValue::Uuid(uuid(UUID_B)))));
        assert!(batched.tables.contains(&TableId::new("messages")));
    }

    #[test]
    fn getmany_empty_captures_nothing() {
        let catalog = catalog_messages();
        let mut rs = ReadSet::new();
        capture_reads(
            &DbOp::GetMany {
                table: "messages".to_string(),
                ids: vec![],
            },
            &catalog,
            &mut rs,
        );
        assert!(rs.keys.is_empty() && rs.tables.is_empty() && rs.filters.is_empty());
    }

    #[test]
    fn write_ops_capture_nothing() {
        let catalog = catalog_messages();
        let mut value = Map::new();
        value.insert("channelId".to_string(), json!(format!("channels:{UUID_A}")));

        let mut fields = Map::new();
        fields.insert("channelId".to_string(), json!(format!("channels:{UUID_B}")));

        let ops = vec![
            DbOp::Insert {
                table: "messages".to_string(),
                value: value.clone(),
            },
            DbOp::Patch {
                table: "messages".to_string(),
                id: format!("messages:{UUID_A}"),
                fields: fields.clone(),
            },
            DbOp::Delete {
                table: "messages".to_string(),
                id: format!("messages:{UUID_A}"),
            },
        ];

        for op in &ops {
            let mut rs = ReadSet::new();
            capture_reads(op, &catalog, &mut rs);
            assert!(rs.tables.is_empty(), "write captured a table: {op:?}");
            assert!(rs.filters.is_empty(), "write captured a filter: {op:?}");
            assert!(rs.keys.is_empty(), "write captured a key: {op:?}");
        }
    }

    #[test]
    fn json_to_key_value_coercions() {
        // id_ref string → Uuid (table-qualified decoded).
        let id_col = col(
            "channel_id",
            "channelId",
            PgTypeClass::Uuid,
            Some("channels"),
        );
        assert_eq!(
            json_to_key_value(&json!(format!("channels:{UUID_A}")), &id_col),
            Some(KeyValue::Uuid(uuid(UUID_A)))
        );

        // Int8 from a numeric JSON value.
        let int_col = col("n", "n", PgTypeClass::Int8, None);
        assert_eq!(
            json_to_key_value(&json!(42), &int_col),
            Some(KeyValue::Int(42))
        );
        // Int8 string-fallback.
        assert_eq!(
            json_to_key_value(&json!("42"), &int_col),
            Some(KeyValue::Int(42))
        );

        // Float8 → None (unorderable in the matcher, safely dropped).
        let float_col = col("f", "f", PgTypeClass::Float8, None);
        assert_eq!(json_to_key_value(&json!(1.5), &float_col), None);

        // Null → None regardless of column.
        assert_eq!(json_to_key_value(&Value::Null, &int_col), None);
    }

    #[test]
    fn dbop_serde_lowercase_kind_and_access() {
        // Lowercase `kind` tag deserializes the variant.
        let op: DbOp = serde_json::from_value(json!({
            "kind": "query",
            "table": "messages",
            "mode": "collect",
        }))
        .expect("query deserializes");
        assert!(matches!(op, DbOp::Query { .. }));
        // Read op → access reports the table, not a write.
        assert_eq!(op.access(), Some(("messages", false)));

        let insert: DbOp = serde_json::from_value(json!({
            "kind": "insert",
            "table": "messages",
            "value": {},
        }))
        .expect("insert deserializes");
        assert_eq!(insert.access(), Some(("messages", true)));

        // Raw is opaque → no access table.
        let raw: DbOp = serde_json::from_value(json!({
            "kind": "raw",
            "sql": "SELECT 1",
        }))
        .expect("raw deserializes");
        assert_eq!(raw.access(), None);
    }

    #[test]
    fn getmany_serde_lowercase_kind_and_access() {
        // Slice 0b — wire compat: the worker sends `{kind:"getmany",table,ids}`.
        let op: DbOp = serde_json::from_value(json!({
            "kind": "getmany",
            "table": "messages",
            "ids": [format!("messages:{UUID_A}"), format!("messages:{UUID_B}")],
        }))
        .expect("getmany deserializes");
        match &op {
            DbOp::GetMany { table, ids } => {
                assert_eq!(table, "messages");
                assert_eq!(ids.len(), 2);
            }
            other => panic!("expected GetMany, got {other:?}"),
        }
        // Batched read → access reports the table, not a write.
        assert_eq!(op.access(), Some(("messages", false)));
    }
}
