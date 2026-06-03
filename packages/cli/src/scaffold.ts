// Generates a complete, ready-to-run Pulse app as an in-memory file map. Pure
// (no fs) so it's unit-testable; bin.ts writes the map to disk.

import { installCmd, runCmd, type PackageManager } from "./pm.js";

/** Which starter to lay down: a working todos demo, or a clean slate. */
export type Template = "todos" | "minimal";

export interface ScaffoldOptions {
  /** Package manager to phrase the README's run commands for. Default: pnpm. */
  pm?: PackageManager;
  /** Starter template. Default: todos. */
  template?: Template;
  /** Wire Better Auth (sign-in/up + JWT-protected handlers). Default: false. */
  auth?: boolean;
}

/** Produce the full set of files for a new Pulse app named `name`, pinning the
 *  @onveloz/pulse-* deps to `version` (the CLI's own version, injected by
 *  bin.ts) so a scaffolded app matches the CLI that generated it. */
export function scaffoldApp(
  name: string,
  version: string,
  opts: ScaffoldOptions = {},
): Record<string, string> {
  const PKG = version;
  const pm = opts.pm ?? "pnpm";
  const template = opts.template ?? "todos";
  const auth = opts.auth ?? false;
  const safe = name.replace(/[^a-z0-9-]/gi, "-").toLowerCase() || "pulse-app";

  const files: Record<string, string> = {};

  files["package.json"] =
    JSON.stringify(
      {
        name: safe,
        version: "0.0.0",
        private: true,
        type: "module",
        scripts: {
          // One command runs the whole stack: codegen, schema sync, the engine,
          // and Vite (via --start). Just `pnpm dev`.
          dev: "pulse dev app/app.ts --start vite",
          db: "docker compose up -d",
          build: "tsc && vite build",
          typecheck: "tsc --noEmit",
        },
        dependencies: {
          "@onveloz/pulse-client": `^${PKG}`,
          "@onveloz/pulse-contract": `^${PKG}`,
          "@onveloz/pulse-react": `^${PKG}`,
          "@onveloz/pulse-schema": `^${PKG}`,
          "@onveloz/pulse-server": `^${PKG}`,
          react: "^19.0.0",
          "react-dom": "^19.0.0",
        },
        devDependencies: {
          "@onveloz/pulse-bundler": `^${PKG}`,
          "@onveloz/pulse-cli": `^${PKG}`,
          "@onveloz/pulse-engine": `^${PKG}`,
          "@types/react": "^19.0.0",
          "@types/react-dom": "^19.0.0",
          "@vitejs/plugin-react": "^6.0.2",
          typescript: "^5.9.3",
          vite: "^8.0.15",
        },
        // No PM-specific install-script allowlist is needed: the engine has no
        // postinstall — `pulse dev` downloads the binary on first run — so
        // install is identical across npm, pnpm, yarn, and bun.
      },
      null,
      2,
    ) + "\n";

  files["vite.config.ts"] = `import { definePulseApp } from "@onveloz/pulse-bundler";

// One line — React, an SSE-safe dev server, and SDK resolution, all configured.
export default definePulseApp();
`;

  files["tsconfig.json"] =
    JSON.stringify(
      {
        compilerOptions: {
          target: "ES2022",
          lib: ["ES2022", "DOM", "DOM.Iterable"],
          module: "ESNext",
          moduleResolution: "Bundler",
          jsx: "react-jsx",
          strict: true,
          noUncheckedIndexedAccess: true,
          verbatimModuleSyntax: true,
          isolatedModules: true,
          esModuleInterop: true,
          skipLibCheck: true,
          resolveJsonModule: true,
          noEmit: true,
        },
        include: ["src", "app", "vite.config.ts"],
      },
      null,
      2,
    ) + "\n";

  files["docker-compose.yml"] = `services:
  postgres:
    image: postgres:16
    container_name: ${safe}-pg
    environment:
      POSTGRES_USER: pulse
      POSTGRES_PASSWORD: pulse
      POSTGRES_DB: pulse
    command: ["postgres", "-c", "wal_level=logical"]
    ports: ["54329:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pulse"]
      interval: 2s
      timeout: 3s
      retries: 30
`;

  files["index.html"] = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${safe}</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
`;

  if (template === "todos") {
    files["app/schema.ts"] = `import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export default defineSchema({
  todos: defineTable({
    title: v.string(),
    done: v.boolean(),
  }),
});
`;

    files["app/contract.ts"] = `import { oc } from "@onveloz/pulse-contract";
import { v } from "@onveloz/pulse-schema";

export const contract = {
  todos: {
    list: oc.reactive().output(v.array(v.doc("todos"))),
    add: oc.mutation().input(v.object({ title: v.string() })).output(v.doc("todos")),
    toggle: oc.mutation().input(v.object({ id: v.id("todos") })).output(v.null()),
  },
};
`;

    files["app/todos.ts"] = `import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import "./_generated/dataModel.js";

const os = implement(contract);

export const list = os.todos.list.handler(async ({ ctx }) =>
  ctx.db.query("todos").collect(),
);

export const add = os.todos.add.handler(async ({ ctx, input }) => {
  const id = await ctx.db.insert("todos", { title: input.title, done: false });
  const doc = await ctx.db.get(id);
  if (!doc) throw new Error("insert failed");
  return doc;
});

export const toggle = os.todos.toggle.handler(async ({ ctx, input }) => {
  const doc = await ctx.db.get(input.id);
  if (doc) await ctx.db.patch(input.id, { done: !doc.done });
  return null;
});
`;

    files["app/app.ts"] = `export { default as schema } from "./schema.js";
export * as todos from "./todos.js";
`;
  } else {
    // minimal: a clean slate — one empty table to codegen against, an empty
    // contract, and no handlers. Add tables to the schema, surface them in the
    // contract, and write a handlers module that `implement(contract)`s them.
    files["app/schema.ts"] = `import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export default defineSchema({
  // Add your first table, e.g.
  // messages: defineTable({ body: v.string() }),
});
`;

    files["app/contract.ts"] = `import type { oc } from "@onveloz/pulse-contract";

// Describe your queries + mutations here, e.g.
//   messages: {
//     list: oc.reactive().output(v.array(v.doc("messages"))),
//   }
export const contract = {};
`;

    files["app/app.ts"] = `export { default as schema } from "./schema.js";
// Re-export your handler modules here, e.g. \`export * as messages from "./messages.js";\`
`;
  }

  files["src/client.ts"] = `import { createClient } from "@onveloz/pulse-client";
import type { contract } from "../app/contract.js";

export const pulse = createClient<typeof contract>({
  url: import.meta.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787",
  headers: () => ({ authorization: "Bearer demo" }),
});
`;

  // Ambient Vite types so `import.meta.env` typechecks in a standalone app.
  files["src/vite-env.d.ts"] = `/// <reference types="vite/client" />\n`;

  files["src/main.tsx"] = `import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App.js";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
`;

  if (template === "minimal") {
    files["src/App.tsx"] = `export function App() {
  return (
    <main style={{ maxWidth: 520, margin: "60px auto", fontFamily: "system-ui" }}>
      <h1>${safe}</h1>
      <p>
        Your Pulse app is running. Add a table in <code>app/schema.ts</code>,
        describe it in <code>app/contract.ts</code>, write its handlers, then
        subscribe with <code>pulse</code> from <code>src/client.ts</code>.
      </p>
    </main>
  );
}
`;
  } else {
    files["src/App.tsx"] = `import { useEffect, useState } from "react";
import type { Doc } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

type Todo = Doc<"todos">;

export function App() {
  const [todos, setTodos] = useState<Todo[]>([]);
  const [title, setTitle] = useState("");

  useEffect(() => pulse.todos.list.subscribe({}, (t) => setTodos(t as Todo[])), []);

  async function add() {
    if (!title.trim()) return;
    const text = title;
    setTitle("");
    await pulse.$local.mutate(["todos", "add"], { title: text }, {
      optimistic: (s) => {
        const cur = s.getQuery<Todo>(["todos", "list"], undefined);
        s.setQuery(["todos", "list"], undefined, [
          ...cur,
          { _id: s.tempId("todos") as Todo["_id"], _creationTime: Date.now(), title: text, done: false },
        ]);
      },
    });
  }

  return (
    <main style={{ maxWidth: 520, margin: "60px auto", fontFamily: "system-ui" }}>
      <h1>${safe}</h1>
      <div style={{ display: "flex", gap: 8 }}>
        <input
          value={title}
          placeholder="New todo…"
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && add()}
          style={{ flex: 1, padding: 8 }}
        />
        <button onClick={add}>Add</button>
      </div>
      <ul>
        {todos.map((t) => (
          <li key={t._id} onClick={() => pulse.todos.toggle.call({ id: t._id })}
              style={{ textDecoration: t.done ? "line-through" : "none", cursor: "pointer" }}>
            {t.title}
          </li>
        ))}
      </ul>
    </main>
  );
}
`;
  }

  const dev = runCmd(pm, "dev");
  const editLine =
    template === "todos"
      ? "Edit `app/schema.ts`, `app/contract.ts`, and `app/todos.ts` to build your app.\n" +
        "`src/App.tsx` shows reactive subscribe + optimistic offline-safe mutate."
      : "Add a table in `app/schema.ts`, describe it in `app/contract.ts`, write its\n" +
        "handlers, and re-export them from `app/app.ts`. `src/client.ts` is your typed client.";
  files["README.md"] = `# ${safe}

A Pulse app — reactive + offline-first on standard Postgres.

\`\`\`bash
${installCmd(pm)}
${runCmd(pm, "db")}          # start Postgres (Docker)
${dev}         # codegen + schema sync + engine (:8787) + Vite (:5273)
\`\`\`

\`${dev}\` runs everything: it generates the typed data model, syncs your
schema to the database (safe additive changes auto-apply; risky/destructive ones
are printed for review via \`pulse migrate app/schema.ts --diff\`), starts the
Pulse engine, and launches Vite alongside it.

${editLine}
`;

  // House rules for coding agents — the Pulse mental model in one file, so an
  // agent builds correctly without spelunking. CLAUDE.md imports it verbatim.
  files["AGENTS.md"] = agentsDoc(safe, template, auth, pm);
  files["CLAUDE.md"] = `# ${safe}\n\nThis project's agent instructions live in AGENTS.md — read it before changing files.\n\n@AGENTS.md\n`;

  // Deployment: the Pulse backend (engine + the bun worker that runs handlers).
  // Tested end-to-end (build → run against Postgres → authed/unauthed RPC).
  files["Dockerfile"] = `# syntax=docker/dockerfile:1

# The Pulse backend: the engine + the bun worker that runs your app's handlers.
# Serves the API (:8787). Pair it with your \`vite build\` frontend, hosted
# statically and pointed at this server's public URL via VITE_PULSE_ENGINE_URL.
# Requires a Postgres reachable at DATABASE_URL.
#
# Debian trixie-slim runs the prebuilt gnu engine binary on any arch. For the
# smallest image, swap to Alpine (the engine also ships static musl binaries):
#   FROM node:22-alpine
#   COPY --from=oven/bun:1-alpine /usr/local/bin/bun /usr/local/bin/bun
FROM node:22-trixie-slim

# bun runs the worker — it executes your app's TypeScript directly.
COPY --from=oven/bun:1 /usr/local/bin/bun /usr/local/bin/bun

WORKDIR /app

# Install deps first (better layer caching). Includes the Pulse CLI + engine,
# which are devDependencies, so install everything.
COPY package.json ./
RUN npm install --no-audit --no-fund

# App source.
COPY . .

# Generate the typed data model the handlers import, then bake the engine binary
# for this platform into the image so startup needs no network.
RUN npx pulse gen app/schema.ts \\
 && node -e "import('@onveloz/pulse-engine').then(m=>m.ensureEngine()).then(p=>{if(!p){console.error('no prebuilt engine for this platform');process.exit(1)}console.log('cached engine:',p)})"

ENV PULSE_PORT=8787 PULSE_WORKER_BIN=bun
EXPOSE 8787

# \`pulse dev\` without --start = codegen + safe schema sync (additive changes
# auto-apply; destructive ones are refused) + run the engine and worker. Provide
# DATABASE_URL at runtime${auth ? " (and PULSE_JWKS_URL pointing at your hosted Better Auth issuer)" : ""}.
CMD ["npx", "pulse", "dev", "app/app.ts"]
`;
  files[".dockerignore"] = `node_modules
dist
app/_generated
schema.sql
.env
.env.local
.git
*.log
.DS_Store
`;

  files[".gitignore"] = `node_modules
dist
schema.sql
app/_generated
*.log
.DS_Store
`;

  if (auth) applyAuth(files, { safe, PKG, pm, template });

  return files;
}

/**
 * Layer Better Auth onto the generated file map. Design decisions:
 *
 *  - **Better Auth owns its tables in a separate `auth` Postgres schema.** Pulse
 *    tables are hard-wired to `_id`/`_creation_time`, which is incompatible with
 *    Better Auth's `id`/`createdAt` shape — so we let Better Auth migrate its own
 *    tables, isolated in the `auth` schema (seeded by a docker init script) so
 *    `pulse dev`'s `public`-scoped schema sync never sees or drops them.
 *  - **The issuer runs inside the Vite dev server.** A tiny Vite plugin mounts
 *    Better Auth's node handler at `/api/auth/*`; its `jwt()` plugin exposes a
 *    JWKS at `/api/auth/jwks`. `@onveloz/pulse-auth`'s `pulseAuth({ jwksUrl })`
 *    verifies bearer tokens against it inside the worker, exposing `ctx.userId`.
 *  - **App tables key off `ctx.userId`** (the verified JWT subject) — the Convex
 *    model — rather than a foreign key into Better Auth's tables.
 */
function applyAuth(
  files: Record<string, string>,
  o: { safe: string; PKG: string; pm: PackageManager; template: Template },
): void {
  const { safe, PKG, pm, template } = o;

  // 1. package.json: auth deps + a migrate script that creates Better Auth's tables.
  const pkg = JSON.parse(files["package.json"]!) as {
    dependencies: Record<string, string>;
    devDependencies: Record<string, string>;
    scripts: Record<string, string>;
  };
  pkg.dependencies["@onveloz/pulse-auth"] = `^${PKG}`;
  pkg.dependencies["better-auth"] = "^1.6.14";
  pkg.dependencies["pg"] = "^8.13.0";
  pkg.devDependencies["@types/pg"] = "^8.11.0";
  // Creates Better Auth's tables in the `auth` schema. Re-run after changing auth plugins.
  pkg.scripts["auth:migrate"] = "npx @better-auth/cli@latest migrate --config app/auth.ts -y";
  files["package.json"] = JSON.stringify(pkg, null, 2) + "\n";

  // 2. Seed the `auth` schema on first DB init so Better Auth's migrate has a home.
  files["db/init.sql"] = `-- Better Auth owns its tables here, isolated from Pulse's \`public\` schema so
-- \`pulse dev\` never introspects (or drops) them. Runs once, on first DB init.
create schema if not exists auth;
`;
  files["docker-compose.yml"] = files["docker-compose.yml"]!.replace(
    /(\n    ports: \["54329:5432"\])/,
    `$1\n    volumes:\n      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`,
  );

  // 3. The Better Auth server: email/password + the jwt plugin (JWKS issuer).
  files["app/auth.ts"] = `import { betterAuth } from "better-auth";
import { jwt } from "better-auth/plugins";
import { Pool } from "pg";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgres://pulse:pulse@127.0.0.1:54329/pulse";

// Better Auth keeps its tables in the \`auth\` schema (see db/init.sql) so Pulse's
// schema sync — which only manages \`public\` — never touches them.
const pool = new Pool({ connectionString: databaseUrl, options: "-c search_path=auth,public" });

export const auth = betterAuth({
  database: pool,
  baseURL: process.env.BETTER_AUTH_URL ?? "http://127.0.0.1:5273",
  // Set BETTER_AUTH_SECRET in .env for anything but local dev (see .env.example).
  secret: process.env.BETTER_AUTH_SECRET ?? "pulse-dev-secret-change-in-production-0001",
  emailAndPassword: { enabled: true },
  // The jwt plugin issues signed JWTs + exposes a JWKS at /api/auth/jwks, which
  // the Pulse worker verifies bearer tokens against.
  plugins: [jwt()],
  trustedOrigins: ["http://127.0.0.1:5273", "http://localhost:5273"],
});
`;

  // 4. The verify side: pulseAuth checks the bearer JWT against the JWKS, then
  //    every \`.use(authed)\` handler gets \`ctx.userId\` (the token subject).
  files["app/authed.ts"] = `import { pulseAuth } from "@onveloz/pulse-auth";

// In dev the issuer is the Vite-hosted Better Auth server; point this at your
// deployed auth origin's JWKS in production.
const jwksUrl = process.env.PULSE_JWKS_URL ?? "http://127.0.0.1:5273/api/auth/jwks";

export const { authed } = pulseAuth({ jwksUrl });
`;

  // 5. Host the Better Auth handler inside the Vite dev server at /api/auth/*.
  files["vite.config.ts"] = `import { definePulseApp } from "@onveloz/pulse-bundler";
import { toNodeHandler } from "better-auth/node";
import { auth } from "./app/auth.js";

// Mount Better Auth's routes on the dev server, so /api/auth/* (sign-in, sign-up,
// and the JWKS the worker verifies against) is served from the same origin as the app.
export default definePulseApp({
  vite: {
    plugins: [
      {
        name: "pulse-better-auth",
        configureServer(server) {
          const handler = toNodeHandler(auth);
          server.middlewares.use((req, res, next) => {
            if (req.url?.startsWith("/api/auth")) {
              handler(req, res);
              return;
            }
            next();
          });
        },
      },
    ],
  },
});
`;

  // 6. The Better Auth browser client (React) + the jwt client plugin so we can
  //    mint a bearer token for the Pulse client.
  files["src/auth-client.ts"] = `import { createAuthClient } from "better-auth/react";
import { jwtClient } from "better-auth/client/plugins";

export const authClient = createAuthClient({
  baseURL: "http://127.0.0.1:5273",
  plugins: [jwtClient()],
});
`;

  // 7. Attach a fresh bearer token (from Better Auth) to every Pulse request.
  files["src/client.ts"] = `import { createClient } from "@onveloz/pulse-client";
import { bearerAuth } from "@onveloz/pulse-auth";
import type { contract } from "../app/contract.js";
import { authClient } from "./auth-client.js";

export const pulse = createClient<typeof contract>({
  url: import.meta.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787",
  // Called per request: returns the current JWT (null when signed out).
  headers: bearerAuth(async () => {
    const { data } = await authClient.token();
    return data?.token ?? null;
  }),
});
`;

  // 8. Auth-scoped data: todos belong to the signed-in user (\`ownerId\`).
  if (template === "todos") {
    files["app/schema.ts"] = `import { defineSchema, defineTable, v } from "@onveloz/pulse-schema";

export default defineSchema({
  todos: defineTable({
    ownerId: v.string(), // the signed-in user's id (JWT subject)
    title: v.string(),
    done: v.boolean(),
  }),
});
`;

    files["app/todos.ts"] = `import { implement } from "@onveloz/pulse-server";
import { contract } from "./contract.js";
import { authed } from "./authed.js";
import "./_generated/dataModel.js";

const os = implement(contract);

// Every handler runs behind \`authed\`, so \`ctx.userId\` is the verified caller.
export const list = os.todos.list.use(authed).handler(async ({ ctx }) => {
  const all = await ctx.db.query("todos").collect();
  return all.filter((t) => t.ownerId === ctx.userId);
});

export const add = os.todos.add.use(authed).handler(async ({ ctx, input }) => {
  const id = await ctx.db.insert("todos", {
    ownerId: ctx.userId,
    title: input.title,
    done: false,
  });
  const doc = await ctx.db.get(id);
  if (!doc) throw new Error("insert failed");
  return doc;
});

export const toggle = os.todos.toggle.use(authed).handler(async ({ ctx, input, errors }) => {
  const doc = await ctx.db.get(input.id);
  if (!doc || doc.ownerId !== ctx.userId) throw errors.NOT_FOUND();
  await ctx.db.patch(input.id, { done: !doc.done });
  return null;
});
`;
  }

  // 9. The UI: a sign-in / sign-up gate around the app.
  files["src/App.tsx"] = authApp(safe, template);

  // 10. .env.example + ignore real env files.
  files[".env.example"] = `# Generate a real secret for anything but local dev: \`openssl rand -base64 32\`
BETTER_AUTH_SECRET=pulse-dev-secret-change-in-production-0001
BETTER_AUTH_URL=http://127.0.0.1:5273
DATABASE_URL=postgres://pulse:pulse@127.0.0.1:54329/pulse
`;
  files[".gitignore"] = files[".gitignore"]! + ".env\n.env.local\n";

  // 11. README: the extra migrate step + how auth is wired.
  const install = installCmd(pm);
  files["README.md"] = `# ${safe}

A Pulse app — reactive + offline-first on standard Postgres, with Better Auth.

\`\`\`bash
${install}
${runCmd(pm, "db")}            # start Postgres (Docker)
${runCmd(pm, "auth:migrate")}  # create Better Auth's tables (auth schema)
${runCmd(pm, "dev")}           # engine (:8787) + Vite (:5273), all of it
\`\`\`

Open http://127.0.0.1:5273, create an account, and you're in.

## How auth is wired

- \`app/auth.ts\` — the Better Auth server (email/password + a \`jwt()\` plugin that
  exposes a JWKS at \`/api/auth/jwks\`). It runs inside the Vite dev server
  (\`vite.config.ts\`) and keeps its tables in a separate \`auth\` Postgres schema.
- \`app/authed.ts\` — \`pulseAuth({ jwksUrl })\` verifies the bearer JWT in the
  worker; every handler that does \`.use(authed)\` gets \`ctx.userId\`.
- \`src/auth-client.ts\` / \`src/client.ts\` — the browser signs in via Better Auth
  and attaches a fresh JWT to every Pulse request with \`bearerAuth\`.
- \`app/todos.ts\` — todos are scoped to the signed-in user via \`ownerId\`.

In production, host \`app/auth.ts\` behind a real server, set \`BETTER_AUTH_SECRET\`
+ \`BETTER_AUTH_URL\` (see \`.env.example\`), and point \`PULSE_JWKS_URL\` at its JWKS.
`;
}

/** The sign-in/up gated App. For the todos template the authed area is the todo
 *  list; for minimal it's a welcome panel. */
function authApp(safe: string, template: Template): string {
  const authedArea =
    template === "todos"
      ? `<Todos />`
      : `<p>You're signed in. Build your app from <code>app/schema.ts</code>.</p>`;

  const todosComponent =
    template === "todos"
      ? `import type { Doc } from "@onveloz/pulse-schema";

type Todo = Doc<"todos">;

function Todos() {
  const [todos, setTodos] = useState<Todo[]>([]);
  const [title, setTitle] = useState("");

  useEffect(() => pulse.todos.list.subscribe({}, (t) => setTodos(t as Todo[])), []);

  async function add() {
    if (!title.trim()) return;
    const text = title;
    setTitle("");
    await pulse.todos.add.call({ title: text });
  }

  return (
    <>
      <div style={{ display: "flex", gap: 8 }}>
        <input
          value={title}
          placeholder="New todo…"
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && add()}
          style={{ flex: 1, padding: 8 }}
        />
        <button onClick={add}>Add</button>
      </div>
      <ul>
        {todos.map((t) => (
          <li
            key={t._id}
            onClick={() => pulse.todos.toggle.call({ id: t._id })}
            style={{ textDecoration: t.done ? "line-through" : "none", cursor: "pointer" }}
          >
            {t.title}
          </li>
        ))}
      </ul>
    </>
  );
}

`
      : "";

  return `import { useEffect, useState } from "react";
import { authClient } from "./auth-client.js";
import { pulse } from "./client.js";

${todosComponent}function Auth() {
  const [mode, setMode] = useState<"sign-in" | "sign-up">("sign-in");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    setError(null);
    const res =
      mode === "sign-up"
        ? await authClient.signUp.email({ name, email, password })
        : await authClient.signIn.email({ email, password });
    if (res.error) setError(res.error.message ?? "Something went wrong");
  }

  return (
    <div style={{ display: "grid", gap: 8 }}>
      <h2>{mode === "sign-up" ? "Create an account" : "Sign in"}</h2>
      {mode === "sign-up" && (
        <input placeholder="Name" value={name} onChange={(e) => setName(e.target.value)} style={{ padding: 8 }} />
      )}
      <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} style={{ padding: 8 }} />
      <input
        placeholder="Password"
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
        style={{ padding: 8 }}
      />
      <button onClick={submit}>{mode === "sign-up" ? "Sign up" : "Sign in"}</button>
      {error && <p style={{ color: "crimson" }}>{error}</p>}
      <button
        onClick={() => setMode(mode === "sign-up" ? "sign-in" : "sign-up")}
        style={{ background: "none", border: "none", color: "#555", cursor: "pointer" }}
      >
        {mode === "sign-up" ? "Have an account? Sign in" : "Need an account? Sign up"}
      </button>
    </div>
  );
}

export function App() {
  const { data: session, isPending } = authClient.useSession();

  return (
    <main style={{ maxWidth: 520, margin: "60px auto", fontFamily: "system-ui" }}>
      <h1>${safe}</h1>
      {isPending ? (
        <p>Loading…</p>
      ) : session ? (
        <>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>{session.user.email}</span>
            <button onClick={() => authClient.signOut()}>Sign out</button>
          </div>
          ${authedArea}
        </>
      ) : (
        <Auth />
      )}
    </main>
  );
}
`;
}

/** The agent house-rules doc — the Pulse build model, tailored to the template
 *  and whether auth is wired. Kept tight and accurate so an agent can build a
 *  feature end-to-end (schema → contract → handler → client) without guessing. */
function agentsDoc(safe: string, template: Template, auth: boolean, pm: PackageManager): string {
  const dev = runCmd(pm, "dev");
  const db = runCmd(pm, "db");
  const handlerExample =
    template === "todos"
      ? "`app/todos.ts`"
      : "a handler module (e.g. `app/messages.ts`)";

  const authRules = auth
    ? `

## Auth (Better Auth is wired)

- Protect a handler by chaining \`.use(authed)\` (from \`app/authed.ts\`); inside it,
  \`ctx.userId\` is the verified caller (the JWT subject). Scope user data by it.
- Better Auth runs inside the Vite dev server (\`app/auth.ts\`, mounted in
  \`vite.config.ts\` at \`/api/auth/*\`). Its tables live in a SEPARATE \`auth\`
  Postgres schema — **never** add user/session/account tables to \`app/schema.ts\`.
- The browser signs in via \`src/auth-client.ts\` and \`src/client.ts\` attaches a
  fresh JWT to every Pulse request with \`bearerAuth\`. Sign-in UI is \`src/App.tsx\`.
- After changing Better Auth plugins, re-run \`${runCmd(pm, "auth:migrate")}\`.`
    : "";

  return `# ${safe} — agent instructions

Pulse is a reactive, offline-first backend on standard Postgres. You build a
feature by declaring a **schema**, describing a typed **contract**, implementing
**handlers**, and reading it reactively from the **client**. Keep those in sync.

## Run it

- \`${dev}\` runs EVERYTHING: typed codegen, schema sync, the engine (:8787), and
  Vite (:5273). Do not run codegen by hand — \`pulse dev\` regenerates it.
- \`${db}\` starts Postgres (Docker).${auth ? `\n- \`${runCmd(pm, "auth:migrate")}\` (once, after the DB is up) creates Better Auth's tables.` : ""}

## Project layout

- \`app/schema.ts\` — tables via \`defineTable({ ... }, v.*)\`. Every table auto-gets
  \`_id\` and \`_creationTime\`; don't declare those.
- \`app/contract.ts\` — the typed API surface: \`oc.reactive()\` (live query),
  \`oc.mutation()\` (write), \`oc.action()\` (side-effects), each with
  \`.input(...)\` / \`.output(...)\` validators.
- ${handlerExample} and friends — implement the contract:
  \`const os = implement(contract)\` then
  \`os.<ns>.<proc>.handler(async ({ ctx, input }) => ...)\`. Use \`ctx.db\`
  (\`query\`, \`get\`, \`insert\`, \`patch\`, \`delete\`).
- \`app/app.ts\` — re-exports the schema + every handler module. Add new modules here.
- \`app/_generated/\` — GENERATED by \`pulse dev\`. Never edit. Handler modules import
  \`./_generated/dataModel.js\` for their typed data model.
- \`src/\` — the React client. \`src/client.ts\` exports the typed \`pulse\` client.

## How to add a feature (the loop)

1. Add the table to \`app/schema.ts\`.
2. Describe its queries/mutations in \`app/contract.ts\`.
3. Implement them in a handler module, then re-export it from \`app/app.ts\`.
4. Read it on the client: \`pulse.<ns>.<proc>.subscribe({ input }, cb)\` for live
   reads, \`pulse.<ns>.<proc>.call(input)\` for writes. \`pulse dev\` picks it all up.

## Rules

- Relative imports use the \`.js\` extension (e.g. \`import { contract } from "./contract.js"\`) — required by \`verbatimModuleSyntax\`.
- Types come from \`@onveloz/pulse-schema\`: \`Doc<"todos">\`, \`Id<"todos">\`.
- A reactive query with no input is subscribed as \`subscribe({}, cb)\` (an options object, not \`undefined\`).
- One mutation handler runs in a single serializable transaction — keep related writes inside the same handler so they commit atomically.
- Schema edits: \`pulse dev\` auto-applies SAFE additive changes (new tables/columns/indexes). Risky or destructive changes are NOT auto-applied — review them with \`pulse migrate app/schema.ts --diff\` and apply by hand.
- Never edit \`app/_generated/**\` or \`schema.sql\`; they're generated.${authRules}

## Deploy

- \`Dockerfile\` builds the Pulse backend (engine + bun worker). Run it with a
  \`DATABASE_URL\`${auth ? " and \`PULSE_JWKS_URL\` (your hosted Better Auth issuer)" : ""}; it serves the API on :8787.
- The frontend is separate: \`${runCmd(pm, "build")}\` produces static assets — host
  them anywhere and set \`VITE_PULSE_ENGINE_URL\` to the backend's public URL.
- \`pulse deploy app/app.ts\` also emits a self-contained bundle (schema DDL + run script).
`;
}
