// Two-client multiplayer E2E against the live site (or BASE_URL).
// Each browser is independent (own sessionStorage => own clientId).
import { chromium } from "playwright";

const BASE = process.env.BASE_URL ?? "https://pulse.runveloz.com";
const results = [];
const ok = (name, pass, info = "") => {
  results.push({ name, pass, info });
  console.log(`${pass ? "✅" : "❌"} ${name}${info ? ` — ${info}` : ""}`);
};

async function poll(page, fn, { timeout = 8000, every = 200 } = {}) {
  const end = Date.now() + timeout;
  for (;;) {
    const v = await page.evaluate(fn);
    if (v && !(Array.isArray(v) && v.length === 0)) return v;
    if (Date.now() > end) return null;
    await new Promise((r) => setTimeout(r, every));
  }
}

// Browser-side helpers (stringified into evaluate).
const H = {
  cid: () => sessionStorage.getItem("pulse:cid"),
  // remote cursor name labels currently rendered (floating cursors)
  remoteNames: () =>
    [...document.querySelectorAll("span")]
      .map((s) => s.textContent?.trim() || "")
      .filter((t) => /(Otter|Fox|Falcon|Panda|Lynx|Heron|Koala|Wolf|Tapir|Gecko|Moth|Crane|Bison|Orca|Raven|Newt|Quail|Yak|Ibis|Seal)/.test(t)),
  panelCount: () => {
    const d = [...document.querySelectorAll("div")].find((d) => /^\d+ here$/.test((d.textContent || "").trim().replace(/\s+/g, " ")));
    return d ? Number((d.textContent || "").trim().match(/\d+/)[0]) : null;
  },
  hasText: (t) => [...document.querySelectorAll("span,div")].some((e) => (e.textContent || "").includes(t)),
  hasEmote: () => [...document.querySelectorAll("span")].some((s) => ["🎉", "❤️", "🔥", "👍", "😮"].includes((s.textContent || "").trim())),
};

const run = async () => {
  const a = await chromium.launch();
  const b = await chromium.launch();
  const pa = await (await a.newContext({ viewport: { width: 1000, height: 800 } })).newPage();
  const pb = await (await b.newContext({ viewport: { width: 1000, height: 800 } })).newPage();
  const errsA = [];
  const errsB = [];
  pa.on("console", (m) => m.type() === "error" && errsA.push(m.text()));
  pb.on("console", (m) => m.type() === "error" && errsB.push(m.text()));

  await pa.goto(BASE, { waitUntil: "domcontentloaded" });
  await pb.goto(BASE, { waitUntil: "domcontentloaded" });

  // Move each cursor so it's "real" (not the centered default).
  await pa.mouse.move(200, 200);
  await pa.mouse.move(260, 240, { steps: 5 });
  await pb.mouse.move(700, 500);
  await pb.mouse.move(640, 460, { steps: 5 });

  const cidA = await pa.evaluate(H.cid);
  const cidB = await pb.evaluate(H.cid);
  ok("distinct client ids", cidA && cidB && cidA !== cidB, `${cidA} vs ${cidB}`);

  // Cross-visibility: B sees A and vice-versa.
  const bSeesA = await poll(pb, H.remoteNames);
  ok("B sees a remote cursor", !!bSeesA && bSeesA.length >= 1, JSON.stringify(bSeesA));
  const aSeesB = await poll(pa, H.remoteNames);
  ok("A sees a remote cursor", !!aSeesB && aSeesB.length >= 1, JSON.stringify(aSeesB));

  // Time-to-first-sight (fresh client). Closed afterwards so it doesn't add
  // noise to later steps.
  const t0 = Date.now();
  const c = await chromium.launch();
  const pc = await (await c.newContext({ viewport: { width: 1000, height: 800 } })).newPage();
  await pc.goto(BASE, { waitUntil: "domcontentloaded" });
  await pc.mouse.move(400, 400);
  const cSees = await poll(pc, H.remoteNames, { timeout: 10000 });
  ok("fresh client sees others quickly", !!cSees, `${Date.now() - t0}ms, ${JSON.stringify(cSees)}`);
  await c.close();

  // Panel counts (each page counts the other + self).
  const cntA = await poll(pa, H.panelCount);
  ok("panel count >= 2 on A", (cntA ?? 0) >= 2, `count=${cntA}`);

  // Low-latency tracking: B sweeps; A should reflect several distinct positions
  // within a couple seconds (poll cadence ~200ms).
  const seen = new Set();
  for (let i = 0; i < 8; i++) {
    await pb.mouse.move(300 + i * 35, 300 + i * 12, { steps: 2 });
    await pb.waitForTimeout(180);
    const pos = await pa.evaluate((cid) => document.querySelector(`div[data-cid="${cid}"]`)?.style.transform ?? null, cidB);
    if (pos) seen.add(pos);
  }
  ok("A tracks B's movement (low latency)", seen.size >= 4, `${seen.size} distinct positions for B`);

  // Own cursor must NOT render to self (desktop: no self arrow/name of own name).
  const ownNameA = await pa.evaluate(() => {
    const ADJ = ["Swift","Calm","Bold","Bright","Brave","Clever","Cosmic","Fuzzy","Gentle","Jolly","Lucky","Mellow","Nimble","Quiet","Rapid","Sly","Sunny","Witty","Zen","Electric"];
    const ANIMAL = ["Otter","Falcon","Panda","Fox","Lynx","Heron","Koala","Wolf","Tapir","Gecko","Moth","Crane","Bison","Orca","Raven","Newt","Quail","Yak","Ibis","Seal"];
    let h=0; const id=sessionStorage.getItem("pulse:cid"); for(const c of id) h=(h*31+c.charCodeAt(0))>>>0;
    return `${ADJ[h%ADJ.length]} ${ANIMAL[(h>>>5)%ANIMAL.length]}`;
  });
  const aRenders = await pa.evaluate(H.remoteNames);
  ok("own cursor not shown to self", !aRenders.includes(ownNameA), `own=${ownNameA} rendered=${JSON.stringify(aRenders)}`);

  // Chat: A says hello -> B sees it.
  await pa.mouse.move(280, 260, { steps: 2 }); // ensure ownPos for the bubble anchor
  await pa.waitForTimeout(150);
  await pa.keyboard.press("/");
  await pa.waitForTimeout(350); // let the input mount + focus
  await pa.keyboard.type("hello-from-A", { delay: 30 });
  await pa.waitForTimeout(300);
  await pa.keyboard.press("Enter");
  await pa.waitForTimeout(300);
  const bSeesChat = await poll(pb, () => [...document.querySelectorAll("span")].some((s) => (s.textContent || "").includes("hello-from-A")));
  ok("B sees A's chat bubble", !!bSeesChat);
  const aSeesOwnChat = await pa.evaluate(() => [...document.querySelectorAll("span")].some((s) => (s.textContent || "").includes("hello-from-A")));
  ok("A sees own chat bubble", aSeesOwnChat);

  // Reaction: A presses 1 -> B sees an emote.
  await pa.mouse.move(300, 300);
  await pa.keyboard.press("1");
  const bSeesEmote = await poll(pb, H.hasEmote, { timeout: 4000 });
  ok("B sees A's reaction", !!bSeesEmote);

  // Follow: B follows A specifically (its panel row, by exact client id — names
  // can collide), then A navigates.
  const followClicked = await pb.evaluate((cid) => {
    const row = [...document.querySelectorAll(`div[data-cid="${cid}"]`)].find((d) => d.querySelector('button[title="Follow"]'));
    const btn = row?.querySelector('button[title="Follow"]');
    if (btn) { btn.click(); return true; }
    return false;
  }, cidA);
  ok("B can click Follow", followClicked);
  await pb.waitForTimeout(300);
  const bFollowing = await pb.evaluate(() => [...document.querySelectorAll("div")].some((d) => /^Following/.test((d.textContent || "").trim())));
  ok("B shows Following banner", bFollowing);

  // A navigates to docs (client-side); B should be pulled along.
  await pa.evaluate(() => {
    const a = [...document.querySelectorAll("a")].find((x) => x.getAttribute("href") === "/docs.html");
    a?.click();
  });
  await pa.waitForTimeout(500);
  await pa.mouse.move(420, 420, { steps: 3 }); // publish presence on docs
  await pa.mouse.move(440, 300, { steps: 3 });
  const aOnDocs = await pa.evaluate(() => location.pathname);
  const bPulled = await poll(pb, () => location.pathname === "/docs.html", { timeout: 6000 });
  ok("A navigated to docs", aOnDocs === "/docs.html", aOnDocs);
  ok("B was pulled to docs by follow", !!bPulled, await pb.evaluate(() => location.pathname));

  console.log("\n--- console errors A ---", [...new Set(errsA)].slice(0, 8));
  console.log("--- console errors B ---", [...new Set(errsB)].slice(0, 8));

  await a.close(); await b.close();
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
};

run().catch((e) => { console.error("E2E crashed:", e); process.exit(2); });
