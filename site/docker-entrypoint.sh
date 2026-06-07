#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL}"

# Veloz injects PORT; the engine reads PULSE_PORT.
export PULSE_PORT="${PORT:-${PULSE_PORT:-8787}}"

# Resolve the binary cached at build time (no network — it's already present).
PULSE_SERVER_BIN="$(node -e "import('@onveloz/pulse-engine').then(m=>m.ensureEngine()).then(p=>process.stdout.write(p||''))")"
export PULSE_SERVER_BIN
[ -x "$PULSE_SERVER_BIN" ] || { echo "engine binary missing: '$PULSE_SERVER_BIN'" >&2; exit 1; }

# Idempotent schema provisioning (CREATE TABLE IF NOT EXISTS …) before boot.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f /app/schema.sql

exec "$PULSE_SERVER_BIN"
