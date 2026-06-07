#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL}"

# Veloz injects PORT; the engine reads PULSE_PORT.
export PULSE_PORT="${PORT:-${PULSE_PORT:-8787}}"

# Resolve the binary cached at build time (no network — it's already present).
PULSE_SERVER_BIN="$(node -e "import('@onveloz/pulse-engine').then(m=>m.ensureEngine()).then(p=>process.stdout.write(p||''))")"
export PULSE_SERVER_BIN
[ -x "$PULSE_SERVER_BIN" ] || { echo "engine binary missing: '$PULSE_SERVER_BIN'" >&2; exit 1; }

# Reset + (re)create the ephemeral presence tables so the live schema always
# matches the app (drop is safe — rows are short-lived and clients re-report).
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f /app/reset.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f /app/schema.sql

exec "$PULSE_SERVER_BIN"
