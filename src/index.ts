/**
 * Entry point for the Cosmos indexer application.
 */
// src/index.ts
import { EventEmitter } from 'node:events';
import { getConfig, printConfig } from './config.ts';
import { createRpcClientFromConfig } from './rpc/client.ts';
import { createTxDecodePool } from './decode/txPool.ts';
import { createSink } from './sink/index.ts';
import { closePgPool, createPgPool, getPgPool } from './db/pg.ts';
import { getProgress } from './db/progress.ts';
import { insertWrapperSettings } from './sink/pg/inserters/zigchain.ts';
import { insertTokenRegistry } from './sink/pg/inserters/tokens.ts';
import { loadProtoRoot, decodeAnyWithRoot } from './decode/dynamicProto.ts';
import { base64ToBytes } from './utils/bytes.ts';
import { buildTokenRegistryRow } from './utils/token-registry.js';
import path from 'node:path';
import { getLogger } from './utils/logger.ts';
import { syncRange } from './runner/syncRange.ts';
import { retryMissingBlocks } from './runner/retryMissing.ts';
import { followLoop } from './runner/follow.ts';
import { bootstrapGenesis } from './scripts/genesis-bootstrap.ts';
import { runReconcileCycle } from './sink/pg/reconcile.ts';
import { ensureCorePartitions } from './db/partitions.js';

EventEmitter.defaultMaxListeners = 0;
const log = getLogger('index');

function normalizeNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeNonNegativeInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n < 0 || n > 2147483647) return null;
  return n;
}

// fetchAllStakingValidatorsViaAbci removed — now uses unified helper from staking_abci.ts

async function main() {
  const cfg = getConfig();
  log.info(`[start] Cosmos Indexer v1.1.0-reconcile-refactor starting...`);
  printConfig(cfg);

  let decodePool: any = null;
  let sink: any = null;
  let reconcileTimer: NodeJS.Timeout | null = null;

  // Cleanup handler for signals and catch blocks
  let isCleaningUp = false;
  const gracefulCleanup = async (exitCode: number) => {
    if (isCleaningUp) return;
    isCleaningUp = true;
    await cleanup(decodePool, sink, reconcileTimer);
    process.exit(exitCode);
  };

  process.on('SIGINT', () => {
    log.info('SIGINT received. Cleaning up...');
    gracefulCleanup(0);
  });
  process.on('SIGTERM', () => {
    log.info('SIGTERM received. Cleaning up...');
    gracefulCleanup(0);
  });

  try {
    // 🛡️ INITIALIZE DB POOL EARLY
    if (cfg.sinkKind === 'postgres') {
      createPgPool({ ...cfg.pg, applicationName: 'cosmos-indexer' });
    }

    // 🛡️ GENESIS BOOTSTRAP (Auto-run if genesis.json exists)
    const genesisPath = process.env.GENESIS_PATH || '/usr/src/app/genesis.json';
    await bootstrapGenesis(genesisPath);

    const rpc = createRpcClientFromConfig(cfg);
    const status = await rpc.fetchStatus();

    let startFrom = cfg.from as number | undefined;
    let lastProgress: number | null = null;
    const wantResume =
      !startFrom || cfg.resume === true || (typeof cfg.from === 'string' && cfg.from.toLowerCase() === 'resume');

    if (wantResume) {
      if (cfg.sinkKind === 'postgres') {
        const pool = getPgPool();
        try {
          const last = await getProgress(pool, cfg.pg?.progressId ?? 'default');
          lastProgress = last;
          const earliest = Number(status['sync_info']['earliest_block_height']);
          const explicitFrom = typeof cfg.from === 'number' ? cfg.from : (cfg.firstBlock as number | undefined);
          startFrom = last != null ? last + 1 : (explicitFrom ?? earliest);
          log.info(`[resume] last_height=${last ?? 'null'} → start from ${startFrom}`);
        } catch (err) {
          log.warn(`[resume] check failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      } else {
        log.warn('[resume] requested, but sinkKind is not postgres — skipping.');
      }
    }

    let endHeight = cfg.to as number | undefined;
    if (!endHeight) {
      endHeight = Number(status['sync_info']['latest_block_height']);
      log.info(`[config] --to not provided → using latest height ${endHeight}`);
    }
    if (startFrom == null || endHeight == null) throw new Error('Both startFrom and endHeight must be resolved.');
    log.info(`[start] from ${startFrom} to ${endHeight} (incl.)`);
    if (cfg.sinkKind === 'postgres' && lastProgress == null && startFrom > 1) {
      log.warn(
        `[start] starting above height 1 (from=${startFrom}) with no existing progress. Historical bank/factory state before this height will be missing; prefer FIRST_BLOCK=1 for fully accurate balances.`,
      );
    }

    const protoDir = process.env.PROTO_DIR || path.join(process.cwd(), 'protos');
    log.info(`[proto] dir = ${protoDir}`);

    const poolSize = Math.max(1, Math.min(cfg.concurrency ?? 12, 12));
    decodePool = createTxDecodePool(poolSize, { protoDir });

    sink = createSink({
      kind: cfg.sinkKind,
      outPath: cfg.outPath,
      flushEvery: cfg.flushEvery ?? 1,
      pg: cfg.pg,
      batchSizes: {
        blocks: cfg.pg?.batchBlocks,
        txs: cfg.pg?.batchTxs,
        msgs: cfg.pg?.batchMsgs,
        events: cfg.pg?.batchEvents,
        attrs: cfg.pg?.batchAttrs,
      },
      rpcUrl: cfg.rpcUrl,
    });
    await sink.init();


    // 🛡️ INITIAL SYNC: Fetch network parameters
    try {
      const pool = getPgPool();
      const client = await pool.connect();
      try {
        await ensureCorePartitions(client, startFrom, startFrom);

        const modules = [
          { name: 'auth', path: '/cosmos.auth.v1beta1.Query/Params', resp: 'cosmos.auth.v1beta1.QueryParamsResponse' },
          { name: 'bank', path: '/cosmos.bank.v1beta1.Query/Params', resp: 'cosmos.bank.v1beta1.QueryParamsResponse' },
          { name: 'staking', path: '/cosmos.staking.v1beta1.Query/Params', resp: 'cosmos.staking.v1beta1.QueryParamsResponse' },
          { name: 'distribution', path: '/cosmos.distribution.v1beta1.Query/Params', resp: 'cosmos.distribution.v1beta1.QueryParamsResponse' },
          { name: 'mint', path: '/cosmos.mint.v1beta1.Query/Params', resp: 'cosmos.mint.v1beta1.QueryParamsResponse' },
          { name: 'slashing', path: '/cosmos.slashing.v1beta1.Query/Params', resp: 'cosmos.slashing.v1beta1.QueryParamsResponse' },
          { name: 'factory', path: '/zigchain.factory.Query/Params', resp: 'zigchain.factory.QueryParamsResponse' },
          { name: 'gov', path: '/cosmos.gov.v1.Query/Params', resp: 'cosmos.gov.v1.QueryParamsResponse' },
        ];

        const rows = [];
        for (const mod of modules) {
          try {
            const abci = await rpc.queryAbci(mod.path);
            if (abci?.value) {
              const decoded = await decodePool.decodeGeneric(mod.resp, abci.value);
              const params = decoded.params || decoded.voting_params || decoded.tally_params || decoded.deposit_params || decoded;
              if (params) {
                // ✅ ENHANCEMENT: Update governance parameters helper for accurate fallbacks


                rows.push({
                  height: startFrom,
                  time: new Date(),
                  module: mod.name,
                  param_key: '_all',
                  old_value: null,
                  new_value: params
                });
              }
            }
          } catch (e) {
            log.debug(`[start] skip module ${mod.name}`);
          }
        }

        if (rows.length > 0) {
          const { flushNetworkParams } = await import('./sink/pg/flushers/params.js');
          await client.query('BEGIN');
          await flushNetworkParams(client, rows);
          await client.query('COMMIT');
          log.info(`[start] synced parameters for ${rows.length} modules`);
        }
      } finally {
        client.release();
      }
    } catch (err) {
      log.warn('[start] params sync failed');
    }

    // 🛡️ INITIAL SYNC: Fetch tokenwrapper module info snapshot
    try {
      const pool = getPgPool();
      const client = await pool.connect();
      try {
        const abci = await rpc.queryAbci('/zigchain.tokenwrapper.Query/ModuleInfo');
        if (abci?.value) {
          const decoded = await decodePool.decodeGeneric('zigchain.tokenwrapper.QueryModuleInfoResponse', abci.value);
          const row = {
            denom: normalizeNonEmptyString(decoded?.denom),
            native_client_id: normalizeNonEmptyString(decoded?.native_client_id),
            counterparty_client_id: normalizeNonEmptyString(decoded?.counterparty_client_id),
            native_port: normalizeNonEmptyString(decoded?.native_port),
            counterparty_port: normalizeNonEmptyString(decoded?.counterparty_port),
            native_channel: normalizeNonEmptyString(decoded?.native_channel),
            counterparty_channel: normalizeNonEmptyString(decoded?.counterparty_channel),
            decimal_difference: normalizeNonNegativeInt(decoded?.decimal_difference),
            updated_at_height: startFrom,
          };

          if (!row.denom) {
            log.warn('[start] tokenwrapper module info returned empty denom; skipping wrapper_settings bootstrap');
          } else {
            await client.query('BEGIN');
            await insertWrapperSettings(client, [row]);
            const registryRow = buildTokenRegistryRow({
              denom: row.denom,
              height: Number(startFrom ?? 0),
              txHash: null,
              source: 'wrapper_settings',
            });
            if (registryRow) {
              await insertTokenRegistry(client, [registryRow]);
            }
            await client.query('COMMIT');
            log.info(`[start] synced tokenwrapper settings for denom=${row.denom}`);
          }
        }
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw e;
      } finally {
        client.release();
      }
    } catch (err) {
      log.warn('[start] tokenwrapper module info sync failed');
    }


    if (cfg.sinkKind === 'postgres') {
      log.info('[start] prioritizing missing block recovery…');
      let totalAttempted = 0;
      for (; ;) {
        const { attempted } = await retryMissingBlocks(rpc, decodePool, sink, {
          concurrency: Math.max(1, Math.min(cfg.concurrency ?? 8, 8)),
          caseMode: cfg.caseMode,
          limit: 5000, // Large batch per pass
        });
        totalAttempted += attempted;
        if (attempted === 0) break;
        log.info(`[start] missing blocks recovery in progress: ${totalAttempted} attempted…`);
      }
      if (totalAttempted > 0) {
        log.info(`[start] missing block recovery complete: ${totalAttempted} blocks processed`);
      }
    }

    const backfill = await syncRange(rpc, decodePool, sink, {
      from: startFrom,
      to: endHeight,
      concurrency: cfg.concurrency,
      progressEveryBlocks: cfg.progressEveryBlocks,
      progressIntervalSec: cfg.progressIntervalSec,
      caseMode: cfg.caseMode,
    });

    log.info(`[done-range] processed ${backfill.processed} blocks`);

    if (cfg.sinkKind === 'postgres' && cfg.reconcileMode !== 'off') {
      try {
        const pool = getPgPool();
        const client = await pool.connect();
        try {
          const protoRoot = await loadProtoRoot(protoDir);
          await runReconcileCycle(client, rpc, protoRoot, {
            mode: cfg.reconcileMode ?? 'full-once-then-negative',
            maxLagBlocks: cfg.reconcileMaxLagBlocks ?? 10_000,
            fullBatchSize: cfg.reconcileFullBatchSize ?? 200,
            stateId: cfg.reconcileStateId ?? 'default',
          });
        } finally {
          client.release();
        }
      } catch (err) {
        log.error('[reconcile] initial cycle error:', err instanceof Error ? err.message : String(err));
      }
    }

    if (cfg.follow !== false) {
      const pollMs = cfg.followIntervalMs ?? 1500;

      if (cfg.sinkKind === 'postgres' && cfg.reconcileMode !== 'off') {
        const periodicMode = cfg.reconcileMode === 'full-once-then-negative' ? 'negative-only' : cfg.reconcileMode;
        let reconcileRunning = false;
        reconcileTimer = setInterval(async () => {
          if (reconcileRunning) {
            log.warn('[reconcile] previous cycle still running, skipping this tick');
            return;
          }
          reconcileRunning = true;
          try {
            const pool = getPgPool();
            const client = await pool.connect();
            try {
              const protoRoot = await loadProtoRoot(protoDir);
              await runReconcileCycle(client, rpc, protoRoot, {
                mode: periodicMode,
                maxLagBlocks: cfg.reconcileMaxLagBlocks ?? 10_000,
                fullBatchSize: cfg.reconcileFullBatchSize ?? 200,
                stateId: cfg.reconcileStateId ?? 'default',
              });
            } finally {
              client.release();
            }
          } catch (err) {
            log.error('[reconcile] loop error:', err instanceof Error ? err.message : String(err));
          } finally {
            reconcileRunning = false;
          }
        }, cfg.reconcileIntervalMs ?? 24 * 60 * 60 * 1000); // 1 day cleanup/reconcile period
      }

      await followLoop(rpc, decodePool, sink, {
        startNext: endHeight + 1,
        pollMs,
        concurrency: cfg.concurrency,
        caseMode: cfg.caseMode,
        missingRetryIntervalMs: cfg.missingRetryIntervalMs,
      });
    }

    await cleanup(decodePool, sink, reconcileTimer);
  } catch (e) {
    const msg = e instanceof Error ? e.stack || e.message : String(e);
    log.error(msg);
    await gracefulCleanup(1);
  }
}

async function cleanup(decodePool: any, sink: any, reconcileTimer: NodeJS.Timeout | null = null) {
  log.info('Shutdown initiated…');
  try {
    if (reconcileTimer) {
      clearInterval(reconcileTimer);
    }
    if (decodePool) await decodePool.close();
    if (sink) {
      await sink.close?.(); // close() handles flushing internally
    }
    log.info('Shutdown complete.');
  } catch (err) {
    log.error('Shutdown error:', err);
  }
}

main();
