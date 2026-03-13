import { getLogger } from '../utils/logger.ts';

const log = getLogger('runner/validators');

type RpcLike = {
  getJson: <T = any>(path: string, params?: Record<string, string | number | boolean | undefined>) => Promise<T>;
};

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let t: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    t = setTimeout(() => reject(new Error(`timeout: ${label} after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(t));
}

function parseCount(v: unknown): number | null {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

export async function fetchCometValidatorsAtHeight(
  rpc: RpcLike,
  height: number,
  timeoutMs: number,
): Promise<any[]> {
  const perPage = 100;
  const maxPages = 200;
  const out: any[] = [];
  const seen = new Set<string>();
  let total: number | null = null;

  for (let page = 1; page <= maxPages; page++) {
    const raw = await withTimeout(
      rpc.getJson('/validators', { height, page, per_page: perPage }),
      timeoutMs,
      `fetchValidators@${height}#${page}`,
    );
    const payload = (raw as any)?.result ?? raw ?? {};
    const vals = Array.isArray((payload as any)?.validators) ? (payload as any).validators : [];
    for (const v of vals) {
      const key = String(v?.address ?? v?.pub_key?.value ?? JSON.stringify(v));
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(v);
    }

    if (total == null) {
      total = parseCount((payload as any)?.total) ?? parseCount((payload as any)?.count);
    }
    if (vals.length === 0) break;
    if (total != null && out.length >= total) break;
    if (vals.length < perPage) break;
  }

  if (out.length === 0) {
    log.debug(`[validators] empty set at height=${height}`);
  }
  return out;
}
