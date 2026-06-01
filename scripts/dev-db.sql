-- Dev schema for examples-chat. In M7 this is generated from the TS schema by
-- `pulse migrate`; for M1 it is applied directly.
--
-- Conventions:
--   * every table has system columns `_id uuid` and `_creation_time bigint` (epoch ms)
--   * user fields are snake_case in Postgres; the engine maps camelCase <-> snake_case

create table if not exists channels (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  name text not null,
  is_private boolean not null
);

create table if not exists users (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  name text not null,
  email text not null,
  role text not null
);
create index if not exists users_by_email on users (email);

create table if not exists messages (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  author_id uuid not null,
  channel_id uuid not null,
  body text not null,
  edited_at bigint
);
create index if not exists messages_by_channel on messages (channel_id, _creation_time);

-- Per-client mutation watermark (used by M4/M5 rebase). Present early so the
-- mutation path can advance it transactionally once implemented.
create table if not exists pulse_clients (
  client_id text primary key,
  last_mutation_id bigint not null default 0
);

-- A counter for the M4 serializable read-modify-write test.
create table if not exists counters (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  name text not null,
  value bigint not null
);

-- Money-transfer surface (S2 deadlock / conservation tests). `transfer` debits
-- one account and credits another in a single mutation; concurrent opposing
-- transfers acquire the two rows in opposing order → 40P01 deadlock, retried.
create table if not exists accounts (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  name text not null,
  balance bigint not null default 0
);

create table if not exists transfers (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  from_id uuid not null,
  to_id uuid not null,
  amount bigint not null
);

-- Collaborative document (Yjs CRDT). `body` is the opaque merged Yjs state,
-- nullable so a fresh doc starts empty. Backs the notes.* collab handlers.
create table if not exists notes (
  _id uuid primary key default gen_random_uuid(),
  _creation_time bigint not null default (extract(epoch from now()) * 1000)::bigint,
  title text not null,
  body bytea
);

-- Seed a channel + user so the example has something to read immediately.
insert into channels (_id, name, is_private)
values ('00000000-0000-0000-0000-000000000001', 'general', false)
on conflict (_id) do nothing;

insert into users (_id, name, email, role)
values ('00000000-0000-0000-0000-000000000010', 'Demo User', 'demo@example.com', 'member')
on conflict (_id) do nothing;
