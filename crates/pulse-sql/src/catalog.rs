use std::collections::HashMap;

use serde::Deserialize;
use sqlx::postgres::PgPool;

use crate::naming::column_to_field;

/// Coarse Postgres type class — enough to (de)serialize values that we always
/// move across the wire cast to/from `text`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTypeClass {
    Uuid,
    Text,
    Int8,
    Float8,
    Bool,
    Timestamptz,
    Jsonb,
    Other,
}

impl PgTypeClass {
    fn from_udt(udt: &str) -> Self {
        match udt {
            "uuid" => PgTypeClass::Uuid,
            "text" | "varchar" | "bpchar" | "name" | "citext" => PgTypeClass::Text,
            "int2" | "int4" | "int8" => PgTypeClass::Int8,
            "float4" | "float8" | "numeric" => PgTypeClass::Float8,
            "bool" => PgTypeClass::Bool,
            "timestamptz" | "timestamp" => PgTypeClass::Timestamptz,
            "json" | "jsonb" => PgTypeClass::Jsonb,
            _ => PgTypeClass::Other,
        }
    }

    /// The SQL cast target used when binding params (`$n::<cast>`).
    pub fn cast(self) -> &'static str {
        match self {
            PgTypeClass::Uuid => "uuid",
            PgTypeClass::Text => "text",
            PgTypeClass::Int8 => "int8",
            PgTypeClass::Float8 => "float8",
            PgTypeClass::Bool => "bool",
            PgTypeClass::Timestamptz => "timestamptz",
            PgTypeClass::Jsonb => "jsonb",
            PgTypeClass::Other => "text",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    /// snake_case Postgres column.
    pub column: String,
    /// camelCase logical field.
    pub field: String,
    pub type_class: PgTypeClass,
    pub nullable: bool,
    /// If this column holds an id, the referenced table (`_id` references its
    /// own table). Drives table-qualified id encoding `"table:uuid"`.
    pub id_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    by_field: HashMap<String, usize>,
    by_column: HashMap<String, usize>,
}

impl Table {
    pub fn column_by_field(&self, field: &str) -> Option<&Column> {
        self.by_field.get(field).map(|&i| &self.columns[i])
    }

    pub fn column_by_name(&self, column: &str) -> Option<&Column> {
        self.by_column.get(column).map(|&i| &self.columns[i])
    }

    /// `col::text AS "col"` for every column — read uniformly as text.
    pub fn select_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("{0}::text AS \"{0}\"", c.column))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub tables: HashMap<String, Table>,
}

impl Catalog {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

// ── Schema field metadata (sent by the worker, from validator `describe()`) ───

/// Per-field validator description we care about: its kind and, for ids, the
/// referenced table.
#[derive(Debug, Clone, Deserialize)]
pub struct FieldMeta {
    pub kind: String,
    #[serde(default, rename = "table")]
    pub ref_table: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableSchema {
    /// camelCase field → meta.
    pub fields: HashMap<String, FieldMeta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SchemaMeta {
    pub tables: HashMap<String, TableSchema>,
}

#[derive(Debug, sqlx::FromRow)]
struct ColumnRow {
    table_name: String,
    column_name: String,
    udt_name: String,
    is_nullable: String,
}

/// Introspect `public` columns and merge in the schema's id references to build
/// the engine catalog.
pub async fn introspect(pool: &PgPool, schema: &SchemaMeta) -> Result<Catalog, sqlx::Error> {
    let rows: Vec<ColumnRow> = sqlx::query_as(
        r#"
        SELECT table_name, column_name, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut tables: HashMap<String, Vec<Column>> = HashMap::new();
    for row in rows {
        let field = column_to_field(&row.column_name);
        let mut id_ref = None;

        // `_id` always references its own table.
        if row.column_name == "_id" {
            id_ref = Some(row.table_name.clone());
        }
        // User-declared id fields reference their target table.
        if let Some(ts) = schema.tables.get(&row.table_name) {
            if let Some(fm) = ts.fields.get(&field) {
                if fm.kind == "id" {
                    id_ref = fm.ref_table.clone();
                }
            }
        }

        tables
            .entry(row.table_name.clone())
            .or_default()
            .push(Column {
                column: row.column_name.clone(),
                field,
                type_class: PgTypeClass::from_udt(&row.udt_name),
                nullable: row.is_nullable.eq_ignore_ascii_case("yes"),
                id_ref,
            });
    }

    let mut catalog = Catalog::default();
    for (name, columns) in tables {
        let by_field = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.field.clone(), i))
            .collect();
        let by_column = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.column.clone(), i))
            .collect();
        catalog.tables.insert(
            name.clone(),
            Table {
                name,
                columns,
                by_field,
                by_column,
            },
        );
    }
    Ok(catalog)
}

// ── id encoding ───────────────────────────────────────────────────────────────

/// Encode a raw uuid as a table-qualified id string `"table:uuid"`.
pub fn encode_id(table: &str, raw: &str) -> String {
    format!("{table}:{raw}")
}

/// Decode `"table:uuid"` → raw uuid. Tolerates a bare uuid (returns it as-is).
pub fn decode_id(value: &str) -> &str {
    match value.split_once(':') {
        Some((_table, raw)) => raw,
        None => value,
    }
}
