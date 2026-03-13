/**
 * Retries missing blocks recorded in the database.
 */
import { assembleBlockJsonFromParts } from '../assemble/blockJson.ts';
import { listMissingBlocks } from '../db/missing_blocks.js';
import { getPgPool } from '../db/pg.js';
import { getLogger } from '../utils/logger.ts';
import { fetchCometValidatorsAtHeight } from './validators.ts';

const log = getLogger('runner/retryMissing');

type RetryOptions = {
  concurrency: number;
  caseMode: 'camel' | 'snake';
  blockTimeoutMs?: number;
  maxBlockRetries?: number;
  limit?: number;
};

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let t: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    t = setTimeout(() => reject(new Error(`timeout: ${label} after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(t));
}

export async function retryMissingBlocks(
  rpc: any,
  pool: any,
  sink: any,
  opts: RetryOptions,
): Promise<{ attempted: number }> {
  if (typeof (sink as any).recordMissingBlock !== 'function') return { attempted: 0 };

  const {
    concurrency,
    caseMode,
    blockTimeoutMs = 30_000,
    maxBlockRetries = 3,
    limit = 1000,
  } = opts;

  const pg = getPgPool();
  const missing = await listMissingBlocks(pg, limit);
  if (missing.length === 0) return { attempted: 0 };

  log.info(`[missing] retrying ${missing.length} block(s)`);

  const attempts = new Map<number, number>();
  const retryQueue = [...missing];
  let inFlight = 0;
  let attempted = 0;

  async function processHeight(h: number) {
    try {
      const [b, br] = await Promise.all([
        withTimeout(rpc.fetchBlock(h), blockTimeoutMs, `fetchBlock@${h}`),
        withTimeout(rpc.fetchBlockResults(h), blockTimeoutMs, `fetchBlockResults@${h}`),
      ]);
      const validatorsPromise = fetchCometValidatorsAtHeight(rpc as any, h, blockTimeoutMs);
      const txsB64: string[] = b?.block?.data?.txs ?? [];
      const decoded = await Promise.all(
        txsB64.map((x, i) => withTimeout(pool.submit(x), blockTimeoutMs, `decode#${i}@${h}`)),
      );
      let validators: any[] = [];
      try {
        validators = await validatorsPromise;
      } catch (err: any) {
        log.warn(`[missing] validators fetch failed for height ${h}: ${String(err?.message ?? err)}`);
      }
      const assembled = await withTimeout(
        assembleBlockJsonFromParts(rpc, b, br, decoded, caseMode, validators),
        blockTimeoutMs,
        `assemble@${h}`,
      );
      await sink.write(assembled as any);
      if (typeof (sink as any).resolveMissingBlock === 'function') {
        await (sink as any).resolveMissingBlock(h);
      }
    } catch (e: any) {
      const n = (attempts.get(h) ?? 0) + 1;
      attempts.set(h, n);
      if (n <= maxBlockRetries) {
        retryQueue.push(h);
        log.warn(`retry ${n}/${maxBlockRetries} for missing height ${h}: ${String(e?.message ?? e)}`);
      } else {
        await (sink as any).recordMissingBlock(h, String(e?.message ?? e));
        log.error(`giving up missing height ${h}: ${String(e?.message ?? e)}`);
      }
    } finally {
      attempted++;
    }
  }

  await new Promise<void>((resolve) => {
    const maybeSpawn = () => {
      while (inFlight < concurrency && retryQueue.length > 0) {
        const h = retryQueue.shift() as number;
        inFlight++;
        processHeight(h).finally(() => {
          inFlight--;
          if (retryQueue.length === 0 && inFlight === 0) {
            resolve();
          } else {
            setImmediate(maybeSpawn);
          }
        });
      }
    };
    maybeSpawn();
  });

  return { attempted };
}
