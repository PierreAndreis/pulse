use std::fmt;

use serde::{Deserialize, Serialize};

/// The kind of a procedure, mirroring `@pulse/contract`'s `ProcedureKind`.
/// Determines runtime, determinism rules, and routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProcedureKind {
    Reactive,
    Mutation,
    Analytical,
    Action,
}

impl ProcedureKind {
    /// Whether this kind establishes a live subscription.
    pub fn is_reactive(self) -> bool {
        matches!(self, ProcedureKind::Reactive)
    }

    /// Whether this kind may write to the database.
    pub fn is_write(self) -> bool {
        matches!(self, ProcedureKind::Mutation)
    }

    /// Whether determinism is enforced for re-execution.
    pub fn is_deterministic(self) -> bool {
        matches!(self, ProcedureKind::Reactive | ProcedureKind::Mutation)
    }
}

/// A dotted procedure path, e.g. `messages.list`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcedurePath(pub Vec<String>);

impl ProcedurePath {
    pub fn new(segments: impl IntoIterator<Item = impl Into<String>>) -> Self {
        ProcedurePath(segments.into_iter().map(Into::into).collect())
    }
}

impl fmt::Display for ProcedurePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

/// Stable identifier for a subscription: a hash of `(path, input, client)`.
/// Stored as a hex string so it is cheap to use as a map key and to send to the
/// client. Construction of the hash lives in `pulse-reactor`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(pub String);

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn procedure_kind_predicates() {
        assert!(ProcedureKind::Reactive.is_reactive());
        assert!(!ProcedureKind::Mutation.is_reactive());
        assert!(!ProcedureKind::Analytical.is_reactive());
        assert!(!ProcedureKind::Action.is_reactive());

        assert!(ProcedureKind::Mutation.is_write());
        assert!(!ProcedureKind::Reactive.is_write());
        assert!(!ProcedureKind::Analytical.is_write());
        assert!(!ProcedureKind::Action.is_write());

        assert!(ProcedureKind::Reactive.is_deterministic());
        assert!(ProcedureKind::Mutation.is_deterministic());
        assert!(!ProcedureKind::Analytical.is_deterministic());
        assert!(!ProcedureKind::Action.is_deterministic());
    }

    #[test]
    fn procedure_kind_serde_lowercase() {
        let json = serde_json::to_string(&ProcedureKind::Reactive).unwrap();
        assert_eq!(json, "\"reactive\"");
        let back: ProcedureKind = serde_json::from_str("\"mutation\"").unwrap();
        assert_eq!(back, ProcedureKind::Mutation);
    }

    #[test]
    fn procedure_path_display() {
        let path = ProcedurePath::new(["messages", "list"]);
        assert_eq!(path.to_string(), "messages.list");
        let single = ProcedurePath::new(["health"]);
        assert_eq!(single.to_string(), "health");
    }
}
