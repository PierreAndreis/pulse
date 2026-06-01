/** Lightweight latency/throughput measurement for load tests. */

export interface Stats {
  count: number;
  errors: number;
  /** Wall-clock span of the run, ms. */
  wallMs: number;
  /** Throughput, completed ops per second. */
  opsPerSec: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  mean: number;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx]!;
}

export function summarize(latencies: number[], errors: number, wallMs: number): Stats {
  const sorted = [...latencies].sort((a, b) => a - b);
  const sum = sorted.reduce((s, x) => s + x, 0);
  return {
    count: latencies.length,
    errors,
    wallMs,
    opsPerSec: wallMs > 0 ? (latencies.length / wallMs) * 1000 : 0,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted.length ? sorted[sorted.length - 1]! : 0,
    mean: sorted.length ? sum / sorted.length : 0,
  };
}

export function fmt(label: string, s: Stats): string {
  return (
    `${label}: ${s.count} ops, ${s.errors} err, ${Math.round(s.opsPerSec)} ops/s | ` +
    `p50=${s.p50.toFixed(1)} p95=${s.p95.toFixed(1)} p99=${s.p99.toFixed(1)} max=${s.max.toFixed(1)}ms`
  );
}

/**
 * Run `task` `total` times with at most `concurrency` in flight, timing each.
 * Returns per-op latencies (ms) and an error count; never throws on task error.
 */
export async function runLoad(
  total: number,
  concurrency: number,
  task: (i: number) => Promise<unknown>,
): Promise<{ latencies: number[]; errors: number; wallMs: number }> {
  const latencies: number[] = [];
  let errors = 0;
  let next = 0;
  const start = performance.now();

  async function worker(): Promise<void> {
    for (;;) {
      const i = next++;
      if (i >= total) return;
      const t0 = performance.now();
      try {
        await task(i);
        latencies.push(performance.now() - t0);
      } catch {
        errors++;
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, total) }, worker));
  return { latencies, errors, wallMs: performance.now() - start };
}

/** Time a single async call in ms. */
export async function timed<T>(fn: () => Promise<T>): Promise<{ ms: number; value: T }> {
  const t0 = performance.now();
  const value = await fn();
  return { ms: performance.now() - t0, value };
}
