use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::change::{Change, ChangeOp, ChangeSet, KeyValue, PrimaryKey, RowValues, TableId};

/// One bound of an index range scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexBound {
    pub values: Vec<KeyValue>,
    /// Whether the bound itself is included.
    pub inclusive: bool,
}

/// An index-range read. Reserved for the CDC/true-index-range path; the live
/// matcher operates on `Filter` (predicate) form, not on these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRange {
    pub index: String,
    pub lower: Option<IndexBound>,
    pub upper: Option<IndexBound>,
}

/// A comparison operator on a captured query predicate. Mirrors `pulse_sql::PredOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    /// SQL `LIKE` (case-sensitive) — `%` = any run, `_` = one char.
    Like,
    /// SQL `ILIKE` (case-insensitive).
    Ilike,
}

/// One analyzed predicate: `field <op> value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cond {
    pub field: String,
    pub op: FilterOp,
    pub value: KeyValue,
}

/// A reactive aggregate function (mirror of `pulse_sql::AggFn`, kept here so the
/// reactor can maintain the scalar incrementally without a `pulse-sql` dep).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The aggregate a reactive query computes over its filtered rows. Lets the
/// reactor update the cached scalar from a change's membership/value delta
/// (incremental view maintenance) instead of re-running the query — when the
/// shape is one it can maintain precisely (else it falls back to re-execution).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aggregate {
    pub func: AggFunc,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub distinct: bool,
}

/// One analyzed read of a table. `conds` are AND-ed; an empty `conds` would be a
/// whole-table read (but we record those as `tables` wildcards instead).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub conds: Vec<Cond>,
    /// Value columns the result depends on, beyond filter membership. `None` = the
    /// full document (any change to a matching row matters). `Some(cols)` (e.g. an
    /// aggregate) = a value-only update to a matching row matters only if one of
    /// `cols` actually changed — so e.g. a reactive `count()` (empty `cols`)
    /// ignores updates that don't move a row in/out of the filter.
    #[serde(default)]
    pub read_cols: Option<Vec<String>>,
    /// When this read is a single scalar aggregate, the function + field so the
    /// reactor can maintain it incrementally. `None` for plain row reads.
    #[serde(default)]
    pub aggregate: Option<Aggregate>,
}

impl Filter {
    /// A filter whose result depends on the whole document (the common case).
    pub fn new(conds: Vec<Cond>) -> Self {
        Self {
            conds,
            read_cols: None,
            aggregate: None,
        }
    }

    /// A filter that depends only on `read_cols` values (plus membership) — used
    /// by aggregates for column-level invalidation.
    pub fn with_read_cols(conds: Vec<Cond>, read_cols: Vec<String>) -> Self {
        Self {
            conds,
            read_cols: Some(read_cols),
            aggregate: None,
        }
    }

    /// Whether the change's `old`/`new` images satisfy this filter's conditions —
    /// the `(old_m, new_m)` enter/leave/stay signal incremental aggregates use.
    pub fn membership(&self, change: &Change) -> (bool, bool) {
        self.membership_images(change.old.as_ref(), change.new.as_ref())
    }

    /// Membership before/after, computed directly from row images. Used when
    /// several changes to one row in a commit are coalesced into a single net
    /// transition (first pre-image, last post-image) before maintaining an
    /// aggregate, so a row inserted-then-deleted in the same tx counts as a no-op.
    pub fn membership_images(
        &self,
        old: Option<&RowValues>,
        new: Option<&RowValues>,
    ) -> (bool, bool) {
        let hit = |row: &RowValues| self.conds.iter().all(|c| eval(c, row));
        (old.is_some_and(hit), new.is_some_and(hit))
    }
}

/// What a reactive query read, at increasing precision. A change invalidates the
/// read-set if it touches a referenced table (coarse), an exact key, or a row
/// satisfying one of the analyzed predicate filters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSet {
    /// Coarse fallback: whole tables read (raw SQL / full scan / uncoercible predicate).
    pub tables: HashSet<TableId>,
    /// Exact rows read via point lookups (`get(id)`).
    pub keys: HashMap<TableId, HashSet<PrimaryKey>>,
    /// Analyzed query predicates (the precise path).
    pub filters: HashMap<TableId, Vec<Filter>>,
    /// Index ranges — reserved for CDC/range work; not consulted by the live matcher.
    pub ranges: HashMap<TableId, Vec<IndexRange>>,
}

fn eval(cond: &Cond, row: &RowValues) -> bool {
    match row.get(&cond.field) {
        // Value not captured (e.g. omitted/uncoercible column) → conservatively match.
        None => true,
        Some(v) => match cond.op {
            FilterOp::Eq => v == &cond.value,
            FilterOp::Neq => v != &cond.value,
            FilterOp::Gt => matches!(v.order(&cond.value), Some(Ordering::Greater)),
            FilterOp::Gte => matches!(
                v.order(&cond.value),
                Some(Ordering::Greater | Ordering::Equal)
            ),
            FilterOp::Lt => matches!(v.order(&cond.value), Some(Ordering::Less)),
            FilterOp::Lte => matches!(v.order(&cond.value), Some(Ordering::Less | Ordering::Equal)),
            FilterOp::Like => like_match(v, &cond.value, false),
            FilterOp::Ilike => like_match(v, &cond.value, true),
        },
    }
}

/// SQL `LIKE`/`ILIKE` match between two text values (`%` = any run incl. empty,
/// `_` = exactly one char). Non-text values never match.
fn like_match(value: &KeyValue, pattern: &KeyValue, case_insensitive: bool) -> bool {
    let (KeyValue::Text(s), KeyValue::Text(p)) = (value, pattern) else {
        return false;
    };
    if case_insensitive {
        like_is_match(&s.to_lowercase(), &p.to_lowercase())
    } else {
        like_is_match(s, p)
    }
}

/// SQL LIKE pattern match (no escape handling; `%` = any run, `_` = one char).
///
/// Runs on the matcher hot path (every change × every LIKE filter), so it avoids
/// the O(n·m)-space DP. Two layers:
///   1. zero-allocation O(n) fast paths for the patterns people actually write —
///      a literal, `foo%`, `%foo`, `%foo%` (no `_`, `%` only in end runs);
///   2. a greedy two-pointer fallback (LeetCode 44) for the general case —
///      O(1) extra space, O(n+m) typical.
fn like_is_match(s: &str, p: &str) -> bool {
    // (1) Fast paths — operate directly on &str, no allocation.
    if !p.contains(['%', '_']) {
        return s == p; // pure literal
    }
    if !p.contains('_') {
        let core = p.trim_matches('%');
        if !core.contains('%') {
            // The only wildcards are leading/trailing `%` runs (p has at least one
            // `%`, so at least one side is anchored by a wildcard).
            return match (p.starts_with('%'), p.ends_with('%')) {
                (true, true) => s.contains(core),
                (true, false) => s.ends_with(core),
                (false, true) => s.starts_with(core),
                (false, false) => like_greedy(s, p), // unreachable, but safe
            };
        }
    }
    // (2) General case: `_`, or `%` embedded between literals.
    like_greedy(s, p)
}

/// Greedy wildcard match in O(1) extra space. On a mismatch after a `%`, backtrack
/// the pattern to just past that `%` and advance the consumed-by-`%` cursor by one.
fn like_greedy(s: &str, p: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = p.chars().collect();
    let (n, m) = (s.len(), p.len());
    let (mut i, mut j) = (0usize, 0usize);
    let mut star: Option<usize> = None; // pattern index of the last `%`
    let mut matched = 0usize; // how much of `s` that `%` has absorbed
    while i < n {
        if j < m && (p[j] == '_' || p[j] == s[i]) {
            i += 1;
            j += 1;
        } else if j < m && p[j] == '%' {
            star = Some(j);
            matched = i;
            j += 1;
        } else if let Some(sj) = star {
            j = sj + 1;
            matched += 1;
            i = matched;
        } else {
            return false;
        }
    }
    // Trailing pattern must be all `%` to match the (now consumed) string.
    while j < m && p[j] == '%' {
        j += 1;
    }
    j == m
}

fn filter_matches(filter: &Filter, change: &Change) -> bool {
    let (old_m, new_m) = filter.membership(change);
    if !new_m && !old_m {
        return false; // the row is outside the filter in both images → irrelevant
    }
    match change.op {
        // Insert/Delete change set membership → always relevant when matched.
        ChangeOp::Insert | ChangeOp::Delete => true,
        ChangeOp::Update => {
            // A membership flip (a filter column crossed the boundary) always matters.
            if new_m != old_m {
                return true;
            }
            // Row stayed inside: only a change to a depended-on column matters.
            // `None` = full document → any change; `Some(cols)` (aggregate) → only
            // if one of those columns actually changed.
            match &filter.read_cols {
                None => true,
                Some(cols) => cols.iter().any(|c| value_changed(c, change)),
            }
        }
    }
}

/// Whether column `col` differs between the old and new images. If it's absent
/// from either image (omitted/uncaptured column, e.g. float/jsonb), we can't
/// confirm it's unchanged, so we conservatively report a change — never a miss.
fn value_changed(col: &str, change: &Change) -> bool {
    match (
        change.old.as_ref().and_then(|r| r.get(col)),
        change.new.as_ref().and_then(|r| r.get(col)),
    ) {
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
}

impl ReadSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: TableId) {
        self.tables.insert(table);
    }

    pub fn add_key(&mut self, table: TableId, key: PrimaryKey) {
        self.keys.entry(table).or_default().insert(key);
    }

    pub fn add_filter(&mut self, table: TableId, filter: Filter) {
        self.filters.entry(table).or_default().push(filter);
    }

    pub fn add_range(&mut self, table: TableId, range: IndexRange) {
        self.ranges.entry(table).or_default().push(range);
    }

    /// Does this single change invalidate the read-set?
    pub fn matches_change(&self, change: &Change) -> bool {
        if self.tables.contains(&change.table) {
            return true; // coarse / raw / full-scan reader
        }
        if let Some(keys) = self.keys.get(&change.table) {
            if keys.contains(&change.key) {
                return true; // point get(id) of this row
            }
        }
        if let Some(filters) = self.filters.get(&change.table) {
            if filters.iter().any(|f| filter_matches(f, change)) {
                return true;
            }
        }
        false
    }

    /// Does any change in the set invalidate the read-set?
    pub fn matches(&self, change_set: &ChangeSet) -> bool {
        change_set.changes.iter().any(|c| self.matches_change(c))
    }

    /// Tables referenced at any precision — used to build the coarse table index.
    pub fn referenced_tables(&self) -> HashSet<TableId> {
        let mut out = self.tables.clone();
        out.extend(self.keys.keys().cloned());
        out.extend(self.filters.keys().cloned());
        out.extend(self.ranges.keys().cloned());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::ChangeOp;
    use crate::lsn::Lsn;
    use proptest::collection::{hash_set, vec};
    use proptest::prelude::*;

    fn insert(table: &str, key: i64) -> Change {
        Change::point(
            TableId::new(table),
            PrimaryKey::single(KeyValue::Int(key)),
            ChangeOp::Insert,
        )
    }

    fn row(pairs: &[(&str, KeyValue)]) -> RowValues {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn eq(field: &str, value: KeyValue) -> Filter {
        Filter {
            conds: vec![Cond {
                field: field.to_string(),
                op: FilterOp::Eq,
                value,
            }],
            read_cols: None,
            aggregate: None,
        }
    }

    #[test]
    fn table_level_match() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let mut cs = ChangeSet::new(Lsn(1));
        cs.push(insert("messages", 7));
        assert!(rs.matches(&cs));
    }

    #[test]
    fn key_level_match_is_precise() {
        let mut rs = ReadSet::new();
        rs.add_key(
            TableId::new("messages"),
            PrimaryKey::single(KeyValue::Int(7)),
        );
        let hit = insert("messages", 7);
        let miss = insert("messages", 8);
        assert!(rs.matches_change(&hit));
        assert!(!rs.matches_change(&miss));
    }

    #[test]
    fn unrelated_table_does_not_match() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let mut cs = ChangeSet::new(Lsn(1));
        cs.push(insert("users", 1));
        assert!(!rs.matches(&cs));
    }

    #[test]
    fn filter_prunes_other_channel() {
        // Read messages WHERE channelId = A.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );

        let into_a = Change {
            new: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            ..insert("messages", 1)
        };
        let into_b = Change {
            new: Some(row(&[("channelId", KeyValue::Text("B".into()))])),
            ..insert("messages", 2)
        };
        assert!(rs.matches_change(&into_a)); // same channel → match
        assert!(!rs.matches_change(&into_b)); // foreign channel → pruned
    }

    #[test]
    fn neq_filter_prunes_equal_rows() {
        // Read todos WHERE done <> false (i.e. the done ones).
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![Cond {
                    field: "done".into(),
                    op: FilterOp::Neq,
                    value: KeyValue::Bool(false),
                }],
                read_cols: None,
                aggregate: None,
            },
        );
        let done = Change {
            new: Some(row(&[("done", KeyValue::Bool(true))])),
            ..insert("todos", 1)
        };
        let not_done = Change {
            new: Some(row(&[("done", KeyValue::Bool(false))])),
            ..insert("todos", 2)
        };
        assert!(rs.matches_change(&done)); // done=true ≠ false → in the result → match
        assert!(!rs.matches_change(&not_done)); // done=false → excluded → pruned
    }

    #[test]
    fn like_filter_matches_pattern_precisely() {
        let mk = |op: FilterOp| {
            let mut rs = ReadSet::new();
            rs.add_filter(
                TableId::new("todos"),
                Filter {
                    conds: vec![Cond {
                        field: "title".into(),
                        op,
                        value: KeyValue::Text("%ppl%".into()),
                    }],
                    read_cols: None,
                    aggregate: None,
                },
            );
            rs
        };
        let title = |s: &str| Change {
            new: Some(row(&[("title", KeyValue::Text(s.into()))])),
            ..insert("todos", 1)
        };
        let rs = mk(FilterOp::Like);
        assert!(rs.matches_change(&title("apple"))); // contains "ppl"
        assert!(!rs.matches_change(&title("grape"))); // does not
        assert!(!rs.matches_change(&title("APPLE"))); // case-sensitive LIKE
        let ci = mk(FilterOp::Ilike);
        assert!(ci.matches_change(&title("APPLE"))); // ILIKE is case-insensitive
    }

    /// Reference O(n·m) DP — the previous implementation, kept here as the oracle
    /// the optimized `like_is_match` (fast paths + greedy) must agree with exactly.
    fn like_dp(s: &str, p: &str) -> bool {
        let s: Vec<char> = s.chars().collect();
        let p: Vec<char> = p.chars().collect();
        let (n, m) = (s.len(), p.len());
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[0][0] = true;
        for j in 1..=m {
            if p[j - 1] == '%' {
                dp[0][j] = dp[0][j - 1];
            }
        }
        for i in 1..=n {
            for j in 1..=m {
                dp[i][j] = match p[j - 1] {
                    '%' => dp[i - 1][j] || dp[i][j - 1],
                    '_' => dp[i - 1][j - 1],
                    c => dp[i - 1][j - 1] && s[i - 1] == c,
                };
            }
        }
        dp[n][m]
    }

    #[test]
    fn like_explicit_cases() {
        let cases: &[(&str, &str, bool)] = &[
            ("", "", true),
            ("a", "", false),
            ("", "%", true),
            ("", "%%", true),
            ("", "_", false),
            ("abc", "abc", true),
            ("abc", "abd", false),
            ("abc", "a_c", true),
            ("abc", "a_", false),
            ("abc", "a%", true),  // prefix
            ("abc", "%c", true),  // suffix
            ("abc", "%b%", true), // contains
            ("abc", "%x%", false),
            ("abc", "a%c", true), // embedded %
            ("aXXc", "a%c", true),
            ("ac", "a%c", true), // % matches empty
            ("abc", "%", true),
            ("abc", "a%%c", true), // consecutive %
            ("abc", "_bc", true),
            ("abc", "ab_", true),
            ("abc", "___", true),
            ("abc", "____", false),
            ("a%b", "a%b", true),  // literal % in text vs % wildcard
            ("café", "ca%", true), // unicode
            ("café", "caf_", true),
        ];
        for (s, p, want) in cases {
            assert_eq!(like_is_match(s, p), *want, "LIKE {s:?} ~ {p:?}");
            assert_eq!(like_dp(s, p), *want, "DP oracle disagrees on {s:?} ~ {p:?}");
        }
    }

    /// Benchmark: optimized `like_is_match` vs the O(n·m)-space DP oracle on
    /// representative patterns. Run:
    ///   cargo test -p pulse-core --release -- --ignored --nocapture bench_like
    #[test]
    #[ignore = "benchmark; run explicitly with --ignored --nocapture"]
    fn bench_like() {
        use std::time::Instant;
        const ITERS: u32 = 1_000_000;
        let text = "the quick brown fox jumps over the lazy dog";
        let pats: &[(&str, &str)] = &[
            ("contains  ", "%brown%"),
            ("prefix    ", "the %"),
            ("suffix    ", "%dog"),
            ("embedded _", "the %fox%la_y%"),
        ];
        for (label, p) in pats {
            let timed = |f: &dyn Fn(&str, &str) -> bool| {
                let start = Instant::now();
                let mut hits = 0u64;
                for _ in 0..ITERS {
                    if f(std::hint::black_box(text), std::hint::black_box(p)) {
                        hits += 1;
                    }
                }
                (start.elapsed().as_nanos() as f64 / ITERS as f64, hits)
            };
            let (fast, _) = timed(&like_is_match);
            let (dp, _) = timed(&like_dp);
            println!(
                "{label} fast={fast:>7.1} ns/op  dp={dp:>7.1} ns/op  ({:.1}x)",
                dp / fast
            );
        }
    }

    /// Differential test: the optimized matcher must agree with the DP oracle on a
    /// broad cartesian product of strings and patterns (incl. tricky % / _ mixes).
    #[test]
    fn like_matches_dp_oracle_exhaustively() {
        let strings = ["", "a", "b", "ab", "ba", "aa", "abc", "abab", "aXbc"];
        let patterns = [
            "", "%", "_", "%%", "a", "a%", "%a", "%a%", "a_", "_a", "a%b", "%a%b%", "a_c", "___",
            "%_", "_%", "a%%b", "ab", "%ab%", "_b_",
        ];
        for s in strings {
            for p in patterns {
                assert_eq!(
                    like_is_match(s, p),
                    like_dp(s, p),
                    "mismatch vs oracle: {s:?} ~ {p:?}"
                );
            }
        }
    }

    #[test]
    fn multi_cond_filter_requires_all_to_match() {
        // done = false AND priority > 5 — a change must satisfy BOTH to invalidate.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![
                    Cond {
                        field: "done".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Bool(false),
                    },
                    Cond {
                        field: "priority".into(),
                        op: FilterOp::Gt,
                        value: KeyValue::Int(5),
                    },
                ],
                read_cols: None,
                aggregate: None,
            },
        );
        let both = Change {
            new: Some(row(&[
                ("done", KeyValue::Bool(false)),
                ("priority", KeyValue::Int(9)),
            ])),
            ..insert("todos", 1)
        };
        let only_one = Change {
            new: Some(row(&[
                ("done", KeyValue::Bool(false)),
                ("priority", KeyValue::Int(3)),
            ])),
            ..insert("todos", 2)
        };
        assert!(rs.matches_change(&both)); // both conds hold → in result → match
        assert!(!rs.matches_change(&only_one)); // priority fails → pruned
    }

    #[test]
    fn neq_with_absent_field_matches_conservatively() {
        // A change image that doesn't carry the filtered field must conservatively
        // invalidate (never a missed update) rather than be wrongly pruned.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![Cond {
                    field: "done".into(),
                    op: FilterOp::Neq,
                    value: KeyValue::Bool(false),
                }],
                read_cols: None,
                aggregate: None,
            },
        );
        let absent = Change {
            new: Some(row(&[("title", KeyValue::Text("x".into()))])),
            ..insert("todos", 1)
        };
        assert!(rs.matches_change(&absent));
    }

    #[test]
    fn filter_matches_via_old_image_on_move() {
        // A row leaving channel A (patched A→B) must still invalidate channel A.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let moved = Change {
            op: ChangeOp::Update,
            old: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            new: Some(row(&[("channelId", KeyValue::Text("B".into()))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&moved));
    }

    #[test]
    fn range_filter_orders() {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "_creationTime".into(),
                    op: FilterOp::Gte,
                    value: KeyValue::Int(100),
                }],
                read_cols: None,
                aggregate: None,
            },
        );
        let after = Change {
            new: Some(row(&[("_creationTime", KeyValue::Int(150))])),
            ..insert("messages", 1)
        };
        let before = Change {
            new: Some(row(&[("_creationTime", KeyValue::Int(50))])),
            ..insert("messages", 2)
        };
        assert!(rs.matches_change(&after));
        assert!(!rs.matches_change(&before));
    }

    fn cond(field: &str, op: FilterOp, value: KeyValue) -> Filter {
        Filter {
            conds: vec![Cond {
                field: field.to_string(),
                op,
                value,
            }],
            read_cols: None,
            aggregate: None,
        }
    }

    // ── Example-based: matcher correctness ────────────────────────────────

    #[test]
    fn delete_invalidates_filter_via_old_image() {
        // A Delete carries only the pre-image. The only-old branch of the OR in
        // `filter_matches` must still invalidate the channel the row left.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let deleted = Change {
            op: ChangeOp::Delete,
            new: None,
            old: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&deleted));
    }

    #[test]
    fn missing_field_matches_conservatively() {
        // Filter on channelId but the new image omits it → the None-arm of
        // `eval` returns true → conservative match (never miss an invalidation).
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let no_channel = Change {
            new: Some(row(&[("authorId", KeyValue::Int(7))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&no_channel));
    }

    #[test]
    fn gt_is_exclusive_gte_inclusive() {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("t"),
            cond("n", FilterOp::Gte, KeyValue::Int(100)),
        );
        let at_100 = Change {
            new: Some(row(&[("n", KeyValue::Int(100))])),
            ..insert("t", 1)
        };
        let at_99 = Change {
            new: Some(row(&[("n", KeyValue::Int(99))])),
            ..insert("t", 2)
        };
        assert!(rs.matches_change(&at_100)); // Gte includes the bound
        assert!(!rs.matches_change(&at_99));

        let mut rs_gt = ReadSet::new();
        rs_gt.add_filter(
            TableId::new("t"),
            cond("n", FilterOp::Gt, KeyValue::Int(100)),
        );
        let at_101 = Change {
            new: Some(row(&[("n", KeyValue::Int(101))])),
            ..insert("t", 3)
        };
        assert!(!rs_gt.matches_change(&at_100)); // Gt excludes the bound
        assert!(rs_gt.matches_change(&at_101));

        // Symmetric Lt / Lte.
        let mut rs_lte = ReadSet::new();
        rs_lte.add_filter(
            TableId::new("t"),
            cond("n", FilterOp::Lte, KeyValue::Int(100)),
        );
        assert!(rs_lte.matches_change(&at_100)); // Lte includes the bound
        assert!(!rs_lte.matches_change(&at_101));

        let mut rs_lt = ReadSet::new();
        rs_lt.add_filter(
            TableId::new("t"),
            cond("n", FilterOp::Lt, KeyValue::Int(100)),
        );
        assert!(!rs_lt.matches_change(&at_100)); // Lt excludes the bound
        assert!(rs_lt.matches_change(&at_99));
    }

    #[test]
    fn cross_variant_order_never_matches() {
        // Ordered ops across KeyValue variants → `order` returns None → no match.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("t"),
            cond("n", FilterOp::Gte, KeyValue::Int(100)),
        );
        let text_row = Change {
            new: Some(row(&[("n", KeyValue::Text("x".into()))])),
            ..insert("t", 1)
        };
        assert!(!rs.matches_change(&text_row));
    }

    #[test]
    #[allow(non_snake_case)]
    fn multi_cond_is_AND() {
        // [channelId = A AND authorId = 7]: a partial match must NOT invalidate.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![
                    Cond {
                        field: "channelId".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Text("A".into()),
                    },
                    Cond {
                        field: "authorId".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Int(7),
                    },
                ],
                read_cols: None,
                aggregate: None,
            },
        );
        let only_channel = Change {
            new: Some(row(&[
                ("channelId", KeyValue::Text("A".into())),
                ("authorId", KeyValue::Int(8)),
            ])),
            ..insert("messages", 1)
        };
        let both = Change {
            new: Some(row(&[
                ("channelId", KeyValue::Text("A".into())),
                ("authorId", KeyValue::Int(7)),
            ])),
            ..insert("messages", 2)
        };
        assert!(!rs.matches_change(&only_channel));
        assert!(rs.matches_change(&both));
    }

    #[test]
    fn empty_changeset_matches_nothing() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let cs = ChangeSet::new(Lsn(1));
        assert!(cs.is_empty());
        assert!(!rs.matches(&cs));
    }

    // ── Property-based ────────────────────────────────────────────────────

    fn kv_strategy() -> impl Strategy<Value = KeyValue> {
        prop_oneof![
            any::<i64>().prop_map(KeyValue::Int),
            "[a-c]{0,3}".prop_map(KeyValue::Text),
            any::<bool>().prop_map(KeyValue::Bool),
            Just(KeyValue::Null),
        ]
    }

    fn op_strategy() -> impl Strategy<Value = FilterOp> {
        prop_oneof![
            Just(FilterOp::Eq),
            Just(FilterOp::Gt),
            Just(FilterOp::Gte),
            Just(FilterOp::Lt),
            Just(FilterOp::Lte),
        ]
    }

    fn field_strategy() -> impl Strategy<Value = String> {
        prop::sample::select(vec!["f0".to_string(), "f1".to_string(), "f2".to_string()])
    }

    fn table_strategy() -> impl Strategy<Value = TableId> {
        prop::sample::select(vec!["t0".to_string(), "t1".to_string(), "t2".to_string()])
            .prop_map(TableId)
    }

    fn cond_strategy() -> impl Strategy<Value = Cond> {
        (field_strategy(), op_strategy(), kv_strategy()).prop_map(|(field, op, value)| Cond {
            field,
            op,
            value,
        })
    }

    fn filter_strategy() -> impl Strategy<Value = Filter> {
        vec(cond_strategy(), 0..3).prop_map(Filter::new)
    }

    fn row_strategy() -> impl Strategy<Value = RowValues> {
        vec((field_strategy(), kv_strategy()), 0..4).prop_map(|pairs| pairs.into_iter().collect())
    }

    fn opt_row_strategy() -> impl Strategy<Value = Option<RowValues>> {
        prop::option::of(row_strategy())
    }

    fn change_strategy() -> impl Strategy<Value = Change> {
        (
            table_strategy(),
            any::<i64>(),
            prop_oneof![
                Just(ChangeOp::Insert),
                Just(ChangeOp::Update),
                Just(ChangeOp::Delete)
            ],
            opt_row_strategy(),
            opt_row_strategy(),
        )
            .prop_map(|(table, k, op, new, old)| Change {
                table,
                key: PrimaryKey::single(KeyValue::Int(k)),
                op,
                new,
                old,
            })
    }

    fn readset_strategy() -> impl Strategy<Value = ReadSet> {
        (
            hash_set(table_strategy(), 0..3),
            vec((table_strategy(), vec(filter_strategy(), 0..3)), 0..3),
        )
            .prop_map(|(tables, filters)| {
                let mut rs = ReadSet::new();
                rs.tables = tables;
                for (t, fs) in filters {
                    for f in fs {
                        rs.add_filter(t.clone(), f);
                    }
                }
                rs
            })
    }

    fn changeset_strategy() -> impl Strategy<Value = ChangeSet> {
        vec(change_strategy(), 0..4).prop_map(|changes| ChangeSet {
            commit_lsn: Lsn(0),
            changes,
        })
    }

    proptest! {
        // P1: set-level matching is exactly the OR of per-change matching, and a
        // table wildcard always invalidates a change on that table.
        #[test]
        fn p1_matches_is_any_over_changes(rs in readset_strategy(), cs in changeset_strategy()) {
            let expected = cs.changes.iter().any(|c| rs.matches_change(c));
            prop_assert_eq!(rs.matches(&cs), expected);

            for c in &cs.changes {
                if rs.tables.contains(&c.table) {
                    prop_assert!(rs.matches_change(c));
                }
            }
        }

        // P3: `order` is antisymmetric within a variant, None across variants,
        // consistent with Eq, and no value satisfies both Gt(x) and Lt(x).
        #[test]
        fn p3_order_is_well_behaved(a in kv_strategy(), b in kv_strategy()) {
            match (a.order(&b), b.order(&a)) {
                (Some(ab), Some(ba)) => prop_assert_eq!(ab, ba.reverse()),
                (None, None) => {} // cross-variant (or two Nulls): symmetric None
                _ => prop_assert!(false, "order not symmetric: {:?} {:?}", a, b),
            }
            // Consistent with Eq within a variant.
            if let Some(ord) = a.order(&b) {
                prop_assert_eq!(ord == Ordering::Equal, a == b);
            }
            // No value can be both > x and < x.
            let gt = matches!(a.order(&b), Some(Ordering::Greater));
            let lt = matches!(a.order(&b), Some(Ordering::Less));
            prop_assert!(!(gt && lt));
        }
    }

    /// Build an Update change for `todos` with the given old/new row images.
    fn update(old: RowValues, new: RowValues) -> Change {
        Change {
            op: ChangeOp::Update,
            old: Some(old),
            new: Some(new),
            ..insert("todos", 1)
        }
    }

    #[test]
    fn count_filter_prunes_value_only_update() {
        // A reactive `count()` over active=true rows. Its result depends only on
        // membership (read_cols = empty): an update that touches a non-filter,
        // non-aggregated column of a row that stays inside the filter must NOT
        // re-run the count.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter::with_read_cols(
                vec![Cond {
                    field: "active".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Bool(true),
                }],
                vec![], // bare count(*) — no value column matters
            ),
        );
        // active stays true, only `title` changes → count unaffected → pruned.
        let touch_title = update(
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("a".into())),
            ]),
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("b".into())),
            ]),
        );
        assert!(!rs.matches_change(&touch_title));
    }

    #[test]
    fn count_filter_still_fires_on_membership_flip() {
        // The same count() must still re-run when a row crosses the filter
        // boundary (active true→false), since that changes the count.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter::with_read_cols(
                vec![Cond {
                    field: "active".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Bool(true),
                }],
                vec![],
            ),
        );
        let leaves = update(
            row(&[("active", KeyValue::Bool(true))]),
            row(&[("active", KeyValue::Bool(false))]),
        );
        assert!(rs.matches_change(&leaves));
    }

    #[test]
    fn aggregate_filter_fires_only_when_read_column_changes() {
        // sum(price) over active=true rows: depends on the `price` column. A
        // value-only update re-runs iff `price` actually changed.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter::with_read_cols(
                vec![Cond {
                    field: "active".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Bool(true),
                }],
                vec!["price".into()],
            ),
        );
        let price_changed = update(
            row(&[
                ("active", KeyValue::Bool(true)),
                ("price", KeyValue::Int(10)),
            ]),
            row(&[
                ("active", KeyValue::Bool(true)),
                ("price", KeyValue::Int(20)),
            ]),
        );
        let other_changed = update(
            row(&[
                ("active", KeyValue::Bool(true)),
                ("price", KeyValue::Int(10)),
            ]),
            row(&[
                ("active", KeyValue::Bool(true)),
                ("price", KeyValue::Int(10)),
            ]),
        );
        assert!(rs.matches_change(&price_changed)); // price moved → re-run sum
        assert!(!rs.matches_change(&other_changed)); // price unchanged → pruned
    }

    #[test]
    fn full_document_filter_fires_on_any_value_update() {
        // A plain row query (read_cols = None) depends on the whole document, so
        // any update to a row in the filter must invalidate.
        let mut rs = ReadSet::new();
        rs.add_filter(TableId::new("todos"), eq("active", KeyValue::Bool(true)));
        let touch_title = update(
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("a".into())),
            ]),
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("b".into())),
            ]),
        );
        assert!(rs.matches_change(&touch_title));
    }

    #[test]
    fn aggregate_filter_conservative_on_absent_read_column() {
        // If the aggregated column isn't carried in the change image (e.g. an
        // uncaptured float/jsonb), we can't prove it's unchanged → must fire.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter::with_read_cols(
                vec![Cond {
                    field: "active".into(),
                    op: FilterOp::Eq,
                    value: KeyValue::Bool(true),
                }],
                vec!["price".into()],
            ),
        );
        // images carry only `active`; `price` absent from both → conservative match.
        let absent_price = update(
            row(&[("active", KeyValue::Bool(true))]),
            row(&[("active", KeyValue::Bool(true))]),
        );
        assert!(rs.matches_change(&absent_price));
    }

    /// Micro-benchmark of the matching hot path: how fast a value-only update is
    /// rejected by a `count()` read-set (column-pruned) vs. a full-document
    /// read-set (must re-run). Dependency-free; run with:
    ///   cargo test -p pulse-core -- --ignored --nocapture bench_matching
    #[test]
    #[ignore = "benchmark; run explicitly with --ignored --nocapture"]
    fn bench_matching_hot_path() {
        use std::time::Instant;

        const ITERS: u32 = 2_000_000;
        let conds = vec![Cond {
            field: "active".into(),
            op: FilterOp::Eq,
            value: KeyValue::Bool(true),
        }];
        // A value-only update to a row that stays inside the filter.
        let change = update(
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("a".into())),
            ]),
            row(&[
                ("active", KeyValue::Bool(true)),
                ("title", KeyValue::Text("b".into())),
            ]),
        );

        let bench = |label: &str, filter: Filter| {
            let mut rs = ReadSet::new();
            rs.add_filter(TableId::new("todos"), filter);
            let start = Instant::now();
            let mut hits = 0u64;
            for _ in 0..ITERS {
                if rs.matches_change(std::hint::black_box(&change)) {
                    hits += 1;
                }
            }
            let elapsed = start.elapsed();
            let per = elapsed.as_nanos() as f64 / ITERS as f64;
            println!(
                "{label:<24} {ITERS} iters in {elapsed:?}  ({per:.1} ns/match, {hits} matched)"
            );
        };

        // count(): read_cols empty → value-only update is pruned (returns false fast).
        bench(
            "count() [pruned]",
            Filter::with_read_cols(conds.clone(), vec![]),
        );
        // full-doc query: read_cols None → must invalidate on every matching update.
        bench("full-doc [re-runs]", Filter::new(conds));
    }
}
