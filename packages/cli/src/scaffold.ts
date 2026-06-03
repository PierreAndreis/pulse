// Generates a complete, ready-to-run Pulse app as an in-memory file map. Pure
// (no fs) so it's unit-testable; bin.ts writes the map to disk.

/** Produce the full set of files for a new Pulse app named `name`, pinning the
 *  @onveloz/pulse-* deps to `version` (the CLI's own version, injected by
 *  bin.ts) so a scaffolded app matches the CLI that generated it. */
export function scaffoldApp(name: string, version: string): Record<string, string> {
  const PKG = version;
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

  files["src/client.ts"] = `import { createClient } from "@onveloz/pulse-client";
import type { contract } from "../app/contract.js";

export const pulse = createClient<typeof contract>({
  url: import.meta.env.VITE_PULSE_ENGINE_URL ?? "http://127.0.0.1:8787",
  headers: () => ({ authorization: "Bearer demo" }),
});
`;

  files["src/main.tsx"] = `import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App.js";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
`;

  files["src/App.tsx"] = `import { useEffect, useState } from "react";
import type { Doc } from "@onveloz/pulse-schema";
import { pulse } from "./client.js";

type Todo = Doc<"todos">;

export function App() {
  const [todos, setTodos] = useState<Todo[]>([]);
  const [title, setTitle] = useState("");

  useEffect(() => pulse.todos.list.subscribe(undefined, (t) => setTodos(t as Todo[])), []);

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

  files["README.md"] = `# ${safe}

A Pulse app — reactive + offline-first on standard Postgres.

\`\`\`bash
pnpm install
pnpm db          # start Postgres (Docker)
pnpm dev         # codegen + schema sync + engine (:8787) + Vite (:5273)
\`\`\`

\`pnpm dev\` runs everything: it generates the typed data model, syncs your
schema to the database (safe additive changes auto-apply; risky/destructive ones
are printed for review via \`pulse migrate app/schema.ts --diff\`), starts the
Pulse engine, and launches Vite alongside it.

Edit \`app/schema.ts\`, \`app/contract.ts\`, and \`app/todos.ts\` to build your app.
\`src/App.tsx\` shows reactive subscribe + optimistic offline-safe mutate.
`;

  files[".gitignore"] = `node_modules
dist
schema.sql
app/_generated
*.log
.DS_Store
`;

  return files;
}
