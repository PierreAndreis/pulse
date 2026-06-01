// The worker the Rust engine drives over NDJSON/stdio. It loads the user app
// (schema + handlers), reports a manifest, then for each `execute` runs the
// handler with an instrumented `ctx.db` whose operations are proxied back to the
// engine (which owns Postgres). See `pulse-jsruntime` for the engine side.
import { executeProcedure, PulseError } from "@onveloz/pulse-server";
import type { RegisteredProcedure } from "@onveloz/pulse-server";
import { ValidationError } from "@onveloz/pulse-schema";

const appPath = process.argv[2];
if (!appPath) {
  process.stderr.write("worker: missing app module path (argv[2])\n");
  process.exit(1);
}

// stdout is the NDJSON protocol channel. Route all user `console.*` output to
// stderr so handler logging can never corrupt the protocol stream. (A dedicated
// protocol fd would also guard against direct `process.stdout.write` by user
// code; that hardening is deferred.)
{
  const toStderr =
    (prefix: string) =>
    (...args: unknown[]): void => {
      const text = args
        .map((a) => (typeof a === "string" ? a : (() => { try { return JSON.stringify(a); } catch { return String(a); } })()))
        .join(" ");
      process.stderr.write(`${prefix}${text}\n`);
    };
  console.log = toStderr("");
  console.info = toStderr("");
  console.warn = toStderr("[warn] ");
  console.error = toStderr("[error] ");
  console.debug = toStderr("[debug] ");
}

// ── stdout (serialized writes) ────────────────────────────────────────────────
let writeChain: Promise<void> = Promise.resolve();
function send(msg: unknown): void {
  const line = JSON.stringify(msg) + "\n";
  writeChain = writeChain.then(
    () =>
      new Promise<void>((resolve, reject) => {
        process.stdout.write(line, (err) => (err ? reject(err) : resolve()));
      }),
  );
}
function log(message: string, level = "info"): void {
  send({ type: "log", level, message });
}

// ── db op correlation ─────────────────────────────────────────────────────────
let opSeq = 0;
const pendingOps = new Map<number, { resolve: (v: unknown) => void; reject: (e: unknown) => void }>();

function dbop(requestId: string, op: Record<string, unknown>): Promise<unknown> {
  const opId = ++opSeq;
  return new Promise((resolve, reject) => {
    pendingOps.set(opId, { resolve, reject });
    send({ type: "dbop", requestId, opId, op });
  });
}

function tableOf(id: unknown): string {
  const s = String(id);
  const i = s.indexOf(":");
  return i === -1 ? "" : s.slice(0, i);
}

function makeDb(requestId: string) {
  const call = (op: Record<string, unknown>) => dbop(requestId, op);

  function rangeBuilder(predicates: Array<Record<string, unknown>>) {
    const push = (op: string) => (field: string, value: unknown) => {
      predicates.push({ field, op, value });
      return rb;
    };
    const rb = { eq: push("eq"), gt: push("gt"), gte: push("gte"), lt: push("lt"), lte: push("lte") };
    return rb;
  }

  function query(table: string) {
    const predicates: Array<Record<string, unknown>> = [];
    let order: string | undefined;
    const builder = {
      withIndex(_indexName: string, fn?: (q: ReturnType<typeof rangeBuilder>) => unknown) {
        if (fn) fn(rangeBuilder(predicates));
        return builder;
      },
      order(direction: string) {
        order = direction;
        return builder;
      },
      take: (n: number) => call({ kind: "query", table, predicates, order, limit: n, mode: "take" }),
      collect: () => call({ kind: "query", table, predicates, order, mode: "collect" }),
      first: () => call({ kind: "query", table, predicates, order, mode: "first" }),
      unique: () => call({ kind: "query", table, predicates, order, mode: "unique" }),
    };
    return builder;
  }

  return {
    get: (id: unknown) => call({ kind: "get", table: tableOf(id), id }),
    query,
    insert: (table: string, value: unknown) => call({ kind: "insert", table, value }),
    patch: (id: unknown, fields: unknown) => call({ kind: "patch", table: tableOf(id), id, fields }),
    replace: (id: unknown, value: unknown) => call({ kind: "replace", table: tableOf(id), id, value }),
    delete: (id: unknown) => call({ kind: "delete", table: tableOf(id), id }),
    // Collaborative (CRDT / Yjs) fields. `update`/result are base64 Yjs bytes;
    // the engine merges via `yrs` server-side. Users never call these directly —
    // the client's CollabHandle does (see @onveloz/pulse-client).
    getCollab: (id: unknown, field: string) =>
      call({ kind: "getcollab", table: tableOf(id), id, field }) as Promise<string>,
    applyCollab: (id: unknown, field: string, update: string) =>
      call({ kind: "applycollab", table: tableOf(id), id, field, update }) as Promise<string>,
  };
}

/** Tagged-template raw SQL accessor for the analytical path. */
function makeSql(requestId: string) {
  return (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
    let sql = "";
    for (let i = 0; i < strings.length; i++) {
      sql += strings[i];
      if (i < values.length) sql += `$${i + 1}`;
    }
    return dbop(requestId, { kind: "raw", sql, params: values });
  };
}

const SELF_URL = process.env.PULSE_SELF_URL ?? "";

/** Action re-entry handles: call another procedure via the engine's /rpc. */
function makeActionRunner(headers: Headers) {
  const run = async (path: string, input: unknown): Promise<unknown> => {
    if (!SELF_URL) throw new PulseError("INTERNAL", null, "PULSE_SELF_URL not set");
    const auth = headers.get("authorization");
    const res = await fetch(`${SELF_URL.replace(/\/$/, "")}/rpc`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(auth ? { authorization: auth } : {}),
      },
      body: JSON.stringify({ path: path.split("."), input }),
    });
    const body = (await res.json()) as { result?: unknown; error?: { code: string; data?: unknown; message?: string } };
    if (!res.ok || body.error) {
      const err = body.error ?? { code: "INTERNAL" };
      throw new PulseError(err.code, err.data ?? null, err.message);
    }
    return body.result;
  };
  return {
    runQuery: (path: string, input?: unknown) => run(path, input),
    runMutation: (path: string, input?: unknown) => run(path, input),
    runAction: (path: string, input?: unknown) => run(path, input),
  };
}

// ── app loading + manifest ────────────────────────────────────────────────────
const procedures = new Map<string, RegisteredProcedure>();

interface SchemaLike {
  tables: unknown;
  describe(): Record<string, { fields: Record<string, FieldDesc> }>;
}
type FieldDesc = { kind: string; table?: string; inner?: FieldDesc };

function isProcedure(v: unknown): v is RegisteredProcedure {
  return !!v && typeof v === "object" && (v as { "~brand"?: string })["~brand"] === "pulse.procedure";
}
function isSchema(v: unknown): v is SchemaLike {
  return !!v && typeof v === "object" && "tables" in v && typeof (v as SchemaLike).describe === "function";
}

function fieldMeta(desc: FieldDesc): { kind: string; table?: string } {
  let d: FieldDesc = desc;
  while (d && d.kind === "optional" && d.inner) d = d.inner;
  return d.kind === "id" ? { kind: "id", table: d.table } : { kind: d?.kind ?? "any" };
}

async function start(): Promise<void> {
  const mod: Record<string, unknown> = await import(appPath!);
  let schema: SchemaLike | null = null;

  // Collect procedures from top-level exports and one level of nested namespace
  // objects (e.g. `export * as counters from ...`), so handlers can be grouped
  // by module without name collisions. Registration keys off each procedure's
  // own contract `path`, not its export name.
  const collect = (value: unknown, depth: number): void => {
    if (isProcedure(value)) {
      procedures.set(value.path.join("."), value);
    } else if (isSchema(value)) {
      schema = value;
    } else if (value && typeof value === "object" && depth > 0) {
      for (const inner of Object.values(value as Record<string, unknown>)) {
        collect(inner, depth - 1);
      }
    }
  };
  for (const value of Object.values(mod)) collect(value, 1);
  if (!schema && isSchema(mod.default)) schema = mod.default;

  const described = schema ? schema.describe() : {};
  const schemaMeta = {
    tables: Object.fromEntries(
      Object.entries(described).map(([table, td]) => [
        table,
        {
          fields: Object.fromEntries(
            Object.entries(td.fields).map(([f, desc]) => [f, fieldMeta(desc)]),
          ),
        },
      ]),
    ),
  };

  send({
    type: "manifest",
    procedures: [...procedures.values()].map((p) => ({ path: p.path, kind: p.def.kind })),
    schema: schemaMeta,
  });
  send({ type: "ready" });
}

// ── message handling ──────────────────────────────────────────────────────────
interface ExecuteMsg {
  type: "execute";
  requestId: string;
  path: string[];
  input: unknown;
  headers?: Record<string, string>;
}
interface DbResultMsg {
  type: "dbresult";
  opId: number;
  ok: boolean;
  value?: unknown;
  error?: string;
}

async function handleExecute(msg: ExecuteMsg): Promise<void> {
  const proc = procedures.get(msg.path.join("."));
  if (!proc) {
    send({
      type: "complete",
      requestId: msg.requestId,
      ok: false,
      error: { code: "NOT_FOUND", message: `no procedure ${msg.path.join(".")}` },
    });
    return;
  }
  // Validate input against the contract before running the handler.
  let input = msg.input;
  const inputValidator = proc.def.input;
  if (inputValidator) {
    try {
      input = inputValidator.parse(msg.input);
    } catch (e) {
      if (e instanceof ValidationError) {
        send({
          type: "complete",
          requestId: msg.requestId,
          ok: false,
          error: { code: "BAD_REQUEST", data: null, message: e.message },
        });
        return;
      }
      throw e;
    }
  }

  const headers = new Headers(msg.headers ?? {});
  let baseCtx: Record<string, unknown>;
  if (proc.def.kind === "action") {
    // Actions are non-transactional + non-deterministic: instead of ctx.db they
    // get re-entry handles that call back into the engine over /rpc (each
    // runMutation is its own atomic mutation). The action's auth header is
    // forwarded so identity propagates to the called procedures.
    baseCtx = { headers, requestId: msg.requestId, ...makeActionRunner(headers) };
  } else {
    baseCtx = {
      headers,
      requestId: msg.requestId,
      db: makeDb(msg.requestId),
      sql: makeSql(msg.requestId),
    };
  }

  try {
    const result = await executeProcedure(proc, baseCtx, input);
    send({ type: "complete", requestId: msg.requestId, ok: true, result: result ?? null });
  } catch (e) {
    if (e instanceof PulseError) {
      send({
        type: "complete",
        requestId: msg.requestId,
        ok: false,
        error: { code: e.code, data: e.data ?? null, message: e.message },
      });
    } else {
      const message = e instanceof Error ? e.message : String(e);
      send({
        type: "complete",
        requestId: msg.requestId,
        ok: false,
        error: { code: "INTERNAL", data: null, message },
      });
    }
  }
}

function handleDbResult(msg: DbResultMsg): void {
  const pending = pendingOps.get(msg.opId);
  if (!pending) return;
  pendingOps.delete(msg.opId);
  if (msg.ok) pending.resolve(msg.value ?? null);
  else pending.reject(new PulseError("INTERNAL", null, msg.error ?? "db op failed"));
}

function handleLine(line: string): void {
  if (!line.trim()) return;
  let msg: { type?: string };
  try {
    msg = JSON.parse(line);
  } catch {
    log(`unparseable line: ${line}`, "error");
    return;
  }
  if (msg.type === "execute") void handleExecute(msg as unknown as ExecuteMsg);
  else if (msg.type === "dbresult") handleDbResult(msg as unknown as DbResultMsg);
  else log(`unknown message type ${msg.type}`, "warn");
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let idx: number;
  while ((idx = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, idx);
    buffer = buffer.slice(idx + 1);
    handleLine(line);
  }
});
process.stdin.on("end", () => process.exit(0));

await start();
