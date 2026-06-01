#!/usr/bin/env bash
# One-time repo hardening (requires admin on PierreAndreis/pulse + gh CLI).
# Applies: branch protection on main (PR review + passing CI), and tag protection
# on v* (only admins can create/move/delete release tags). This is the root
# safeguard — without it, anyone who can push a tag can trigger publish + release.
set -euo pipefail

REPO="${PULSE_REPO:-PierreAndreis/pulse}"
echo "Hardening $REPO …"

# main: require a reviewed PR + the three status checks before merge; no FF.
gh api -X POST "repos/$REPO/rulesets" \
  -f name='protect-main' -f target='branch' -f enforcement='active' \
  -F conditions='{"ref_name":{"include":["refs/heads/main"],"exclude":[]}}' \
  -F rules='[
    {"type":"pull_request","parameters":{"required_approving_review_count":1,"dismiss_stale_reviews_on_push":true,"require_code_owner_review":true,"require_last_push_approval":true,"required_review_thread_resolution":false}},
    {"type":"required_status_checks","parameters":{"strict_required_status_checks_policy":true,"required_status_checks":[{"context":"TypeScript (typecheck + unit)"},{"context":"Rust (fmt + clippy + test)"},{"context":"Engine + Postgres (end-to-end)"}]}},
    {"type":"non_fast_forward"}
  ]' \
  && echo "  ✓ main ruleset" || echo "  ! main ruleset (may already exist)"

# v* tags: restrict creation/update/deletion to admins (bypass actor = admin role).
gh api -X POST "repos/$REPO/rulesets" \
  -f name='protect-tags' -f target='tag' -f enforcement='active' \
  -F conditions='{"ref_name":{"include":["refs/tags/v*"],"exclude":[]}}' \
  -F rules='[{"type":"creation"},{"type":"update"},{"type":"deletion"},{"type":"non_fast_forward"}]' \
  -F bypass_actors='[{"actor_id":5,"actor_type":"RepositoryRole","bypass_mode":"always"}]' \
  && echo "  ✓ tag ruleset" || echo "  ! tag ruleset (may already exist)"

echo "Done. Next: Settings → Environments → 'production' → add required reviewers."
