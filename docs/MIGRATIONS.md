# Migrations

Pulse evolves your Postgres schema two ways, mirroring Prisma/Drizzle: a fast
no-files sync for prototyping, and versioned, reviewable migration files for a
team or production.

## Fast loop — `pulse db push`

`pulse dev` already syncs your schema to the database on boot (additive changes
auto-apply; destructive ones prompt). For an explicit one-off sync with no
migration files:

```bash
pulse db push                 # additive auto-applies; destructive prompts (or --force in CI)
```

No history is kept — best for early prototyping.

## Versioned migrations — `pulse migrate`

For a team / production, generate committed, editable SQL migrations:

```bash
pulse migrate dev <name>      # diff schema vs the last snapshot → migrations/NNNN_<name>.sql,
                              # then apply it (and any pending) to the dev DB, and regen the model
pulse migrate deploy          # apply pending files in order, no prompts (CI / production)
pulse migrate status          # list each migration: applied / pending / drifted
```

Layout (a `migrations/` directory next to your schema):

```
migrations/
  0000_init.sql               # editable SQL — destructive drops are commented out
  0001_add_score.sql
  meta/
    0000_init.snapshot.json    # the schema's "shape" recorded after each migration
    0001_add_score.snapshot.json
```

**How `generate` works.** It diffs the current schema against the **last
snapshot**, not the database, so it needs no DB and is deterministic. Additive
and type/nullability changes are written live; index removal and index
redefinition (an index that keeps its name but changes columns) are generated as
`drop`/`create`; destructive column and table drops are commented out by default,
so nothing is lost unless you uncomment them. Edit the SQL freely: turn a
drop+add into a rename, add a data backfill, etc. The file is the source of
truth.

**How `deploy` works.** It runs each migration file not yet recorded, in order,
each in a transaction, writing its name + content hash to a `_pulse_migrations`
journal table. It's idempotent (safe to re-run) and refuses if a migration that
was already applied has since been edited (hash drift): applied migrations are
immutable, so add a new one instead.

Commit `migrations/` to git. `app/_generated/` is regenerated on `migrate dev` /
`pulse dev` and is gitignored.

## Ad-hoc inspection

```bash
pulse migrate --diff          # print the live DB ↔ schema diff (no files, no apply)
pulse migrate --out ddl.sql   # dump the full CREATE-everything DDL
```
