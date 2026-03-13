import { PoolClient } from 'pg';
import { getLogger } from '../../utils/logger.js';
import { insertBalanceDeltas } from './inserters/bank.js';
import { RpcClient } from '../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../decode/dynamicProto.js';
import type { ReconcileMode } from '../../types.js';

const log = getLogger('sink/reconcile');

export type ReconcileRunOptions = {
  mode: ReconcileMode;
  maxLagBlocks: number;
  fullBatchSize: number;
  stateId: string;
};

type RpcHeights = {
  latest: number;
  earliest: number;
};

async function ensureReconcileStateTable(client: PoolClient): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS core.reconcile_state (
      state_id TEXT PRIMARY KEY,
      full_done BOOLEAN NOT NULL DEFAULT FALSE,
      full_height BIGINT NULL,
      full_completed_at TIMESTAMPTZ NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function isFullReconcileDone(client: PoolClient, stateId: string): Promise<boolean> {
  const res = await client.query<{ full_done: boolean }>(
    `SELECT full_done FROM core.reconcile_state WHERE state_id = $1`,
    [stateId],
  );
  return Boolean(res.rows[0]?.full_done);
}

async function markFullReconcileDone(client: PoolClient, stateId: string, height: number): Promise<void> {
  await client.query(
    `
      INSERT INTO core.reconcile_state (
        state_id, full_done, full_height, full_completed_at, updated_at
      )
      VALUES ($1, TRUE, $2, now(), now())
      ON CONFLICT (state_id)
      DO UPDATE SET
        full_done = TRUE,
        full_height = GREATEST(COALESCE(core.reconcile_state.full_height, 0), EXCLUDED.full_height),
        full_completed_at = EXCLUDED.full_completed_at,
        updated_at = now()
    `,
    [stateId, height],
  );
}

function asBigInt(v: unknown): bigint {
  if (v == null) return 0n;
  try {
    return BigInt(String(v).trim());
  } catch {
    return 0n;
  }
}

function isPrunedStateError(err: any): boolean {
  const msg = String(err?.message ?? err).toLowerCase();
  return (
    msg.includes('abci error 38') ||
    msg.includes('version does not exist') ||
    msg.includes('failed to load state at height') ||
    msg.includes('pruned')
  );
}

function normalizeDbBalances(raw: any): Map<string, bigint> {
  const out = new Map<string, bigint>();
  if (!raw || typeof raw !== 'object') return out;
  for (const [denom, amount] of Object.entries(raw)) {
    out.set(String(denom), asBigInt(amount));
  }
  return out;
}

function normalizeRpcBalances(coins: any[]): Map<string, bigint> {
  const out = new Map<string, bigint>();
  for (const c of coins) {
    const denom = String(c?.denom ?? '').trim();
    if (!denom) continue;
    out.set(denom, asBigInt(c?.amount));
  }
  return out;
}

async function getIndexedHeight(client: PoolClient): Promise<number> {
  const hRes = await client.query('SELECT max(height) as h FROM core.blocks');
  const currentHeight = Number(hRes.rows[0]?.h || 0);
  return Number.isFinite(currentHeight) ? currentHeight : 0;
}

async function getRpcHeights(rpc: RpcClient): Promise<RpcHeights> {
  const st = await rpc.fetchStatus();
  const latest = Number(st?.sync_info?.latest_block_height ?? st?.latest_block_height ?? 0);
  const earliest = Number(st?.sync_info?.earliest_block_height ?? st?.earliest_block_height ?? 0);
  return {
    latest: Number.isFinite(latest) ? latest : 0,
    earliest: Number.isFinite(earliest) ? earliest : 0,
  };
}

/**
 * Entry point for reconcile scheduling.
 * - `off`: no-op
 * - `negative-only`: run fast negative-only reconcile
 * - `full-once-then-negative`: run full pass + negative reconcile for this cycle
 */
export async function runReconcileCycle(
  client: PoolClient,
  rpc: RpcClient,
  protoRoot: Root,
  opts: ReconcileRunOptions,
): Promise<void> {
  const mode = opts.mode ?? 'full-once-then-negative';
  const stateId = String(opts.stateId ?? 'default').trim() || 'default';
  if (mode === 'off') return;

  const currentHeight = await getIndexedHeight(client);
  if (currentHeight <= 0) {
    log.warn('[reconcile] Skipping: no indexed block height available yet.');
    return;
  }

  const heights = await getRpcHeights(rpc);
  const lag = Math.max(0, heights.latest - currentHeight);
  if (heights.earliest > 0 && currentHeight < heights.earliest) {
    log.warn(
      `[reconcile] Skipping: indexed height ${currentHeight} is older than RPC earliest ${heights.earliest} (latest ${heights.latest}).`,
    );
    return;
  }
  if (lag > opts.maxLagBlocks) {
    log.info(
      `[reconcile] Skipping: lag ${lag} > maxLag ${opts.maxLagBlocks} (indexed=${currentHeight}, latest=${heights.latest}).`,
    );
    return;
  }

  if (mode === 'full-once-then-negative') {
    await ensureReconcileStateTable(client);
    const fullAlreadyDone = await isFullReconcileDone(client, stateId);
    if (fullAlreadyDone) {
      log.info(`[reconcile/full] Skipping full reconciliation; already completed for stateId="${stateId}".`);
    } else {
      log.info(
        `[reconcile/full] Starting full reconciliation at height ${currentHeight} (batch=${opts.fullBatchSize}, stateId="${stateId}").`,
      );
      const summary = await reconcileAllKnownBalances(client, rpc, protoRoot, currentHeight, opts.fullBatchSize);
      if (summary.failedOther > 0) {
        log.warn(
          `[reconcile/full] completed with failed rows for stateId="${stateId}": failedOther=${summary.failedOther} skippedPruned=${summary.skippedPruned}. Full reconcile will retry on next startup.`,
        );
      } else {
        await markFullReconcileDone(client, stateId, currentHeight);
      }
    }
  }

  await reconcileNegativeBalances(client, rpc, protoRoot, currentHeight);
}

/**
 * Reconciles all known accounts/denoms in `bank.balances_current` against RPC state at `targetHeight`.
 * Intended to run once near tip.
 */
async function reconcileAllKnownBalances(
  client: PoolClient,
  rpc: RpcClient,
  protoRoot: Root,
  targetHeight: number,
  batchSize: number,
): Promise<{ scannedAccounts: number; correctedRows: number; skippedPruned: number; failedOther: number }> {
  let cursor = '';
  let scannedAccounts = 0;
  let correctedRows = 0;
  let skippedPruned = 0;
  let failedOther = 0;

  const snapshotTable = 'reconcile_balances_snapshot';
  await client.query(`DROP TABLE IF EXISTS ${snapshotTable}`);
  await client.query(
    `
      CREATE TEMP TABLE ${snapshotTable} (
        account TEXT PRIMARY KEY,
        balances JSONB NOT NULL
      )
    `,
  );
  await client.query(
    `
      INSERT INTO ${snapshotTable} (account, balances)
      SELECT account, balances
      FROM bank.balances_current
    `,
  );

  try {
    for (; ;) {
      const res = await client.query(
        `
          SELECT account, balances
          FROM ${snapshotTable}
          WHERE account > $1
          ORDER BY account ASC
          LIMIT $2
        `,
        [cursor, batchSize],
      );
      if (!res.rowCount) break;

      const correctionDeltas: any[] = [];
      for (const row of res.rows) {
        const account = String(row.account);
        cursor = account;
        scannedAccounts++;

        try {
          const dbBalances = normalizeDbBalances(row.balances);
          const rpcBalances = await fetchAllBalancesViaAbci(rpc, protoRoot, account, targetHeight);

          const denoms = new Set<string>([...dbBalances.keys(), ...rpcBalances.keys()]);
          for (const denom of denoms) {
            const dbBal = dbBalances.get(denom) ?? 0n;
            const rpcBal = rpcBalances.get(denom) ?? 0n;
            const diff = rpcBal - dbBal;
            if (diff === 0n) continue;
            log.info(
              `[reconcile/full] Correcting ${account} (${denom}) at height ${targetHeight}: DB=${dbBal}, RPC=${rpcBal}, Delta=${diff}`,
            );
            correctionDeltas.push({
              height: targetHeight,
              account,
              denom,
              delta: diff.toString(),
              tx_hash: 'reconcile_full_auto_heal',
              msg_index: -1,
              event_index: 0,
            });
          }
        } catch (err: any) {
          if (isPrunedStateError(err)) {
            skippedPruned++;
            log.warn(`[reconcile/full] Skipping ${account} at height ${targetHeight}: ${err.message}`);
          } else {
            failedOther++;
            log.error(`[reconcile/full] Failed ${account} at height ${targetHeight}: ${err.message}`);
          }
        }
      }

      if (correctionDeltas.length > 0) {
        await insertBalanceDeltas(client, correctionDeltas, { mode: 'merge' });
        correctedRows += correctionDeltas.length;
      }

      log.info(
        `[reconcile/full] progress accounts=${scannedAccounts} corrected=${correctedRows} skippedPruned=${skippedPruned} failedOther=${failedOther} cursor=${cursor}`,
      );

      if (res.rows.length < batchSize) break;
    }
  } finally {
    await client.query(`DROP TABLE IF EXISTS ${snapshotTable}`);
  }

  log.info(
    `[reconcile/full] completed accounts=${scannedAccounts} corrected=${correctedRows} skippedPruned=${skippedPruned} failedOther=${failedOther}`,
  );

  return { scannedAccounts, correctedRows, skippedPruned, failedOther };
}

/**
 * Reconciles negative balances by fetching the true state from RPC and inserting a correction delta.
 * If `targetHeight` is omitted, uses current DB max height.
 */
export async function reconcileNegativeBalances(
  client: PoolClient,
  rpc: RpcClient,
  protoRoot: Root,
  targetHeight?: number,
) {
  log.info('[reconcile] Starting reconciliation of negative balances...');

  const res = await client.query(`
    SELECT account, key as denom, value as current_balance_str
    FROM bank.balances_current, jsonb_each_text(balances)
    WHERE (value::NUMERIC < 0);
  `);

  if (res.rowCount === 0) {
    log.info('[reconcile] No negative balances found. System is healthy.');
    return;
  }

  let currentHeight = Number(targetHeight ?? 0);
  if (!Number.isFinite(currentHeight) || currentHeight <= 0) {
    currentHeight = await getIndexedHeight(client);
  }
  if (!Number.isFinite(currentHeight) || currentHeight <= 0) {
    log.warn('[reconcile] Skipping reconciliation: no indexed block height available yet.');
    return;
  }

  log.info(`[reconcile] Found ${res.rowCount} negative entries. Processing at height ${currentHeight}...`);

  const correctionDeltas: any[] = [];
  let skippedPruned = 0;
  let failedOther = 0;

  for (const row of res.rows) {
    const { account, denom, current_balance_str } = row;

    try {
      const trueBalance = await fetchBalanceViaAbci(rpc, protoRoot, account, denom, currentHeight);
      const currentDbBalance = asBigInt(current_balance_str);
      const diff = trueBalance - currentDbBalance;

      if (diff !== 0n) {
        log.info(
          `[reconcile] Correcting ${account} (${denom}) at height ${currentHeight}: DB=${currentDbBalance}, RPC=${trueBalance}, Delta=${diff}`,
        );
        correctionDeltas.push({
          height: currentHeight,
          account,
          denom,
          delta: diff.toString(),
          tx_hash: 'reconcile_auto_heal',
          msg_index: -1,
          event_index: 0,
        });
      }
    } catch (err: any) {
      if (isPrunedStateError(err)) {
        skippedPruned++;
        log.warn(`[reconcile] Skipping ${account} (${denom}) at height ${currentHeight}: ${err.message}`);
      } else {
        failedOther++;
        log.error(`[reconcile] Failed to reconcile ${account} (${denom}) at height ${currentHeight}: ${err.message}`);
      }
    }
  }

  if (correctionDeltas.length > 0) {
    await insertBalanceDeltas(client, correctionDeltas, { mode: 'merge' });
    log.info(`[reconcile] Successfully applied ${correctionDeltas.length} corrections.`);
  }

  log.info(
    `[reconcile] summary negatives=${res.rowCount} corrected=${correctionDeltas.length} skippedPruned=${skippedPruned} failedOther=${failedOther}`,
  );
}

// Fetch one denom balance via ABCI query at a specific height.
async function fetchBalanceViaAbci(
  rpc: RpcClient,
  root: Root,
  address: string,
  denom: string,
  height: number,
): Promise<bigint> {
  const ReqType = root.lookupType('cosmos.bank.v1beta1.QueryBalanceRequest');
  const ResType = 'cosmos.bank.v1beta1.QueryBalanceResponse';

  const reqMsg = ReqType.create({ address, denom });
  const reqBytes = ReqType.encode(reqMsg).finish();
  const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

  const path = '/cosmos.bank.v1beta1.Query/Balance';
  let response: any;
  try {
    const params = {
      path: `"${path}"`,
      data: reqHex,
      height: String(height),
    };
    const j = await rpc.getJson('/abci_query', params);
    response = j.result?.response ?? j;
  } catch {
    response = await rpc.queryAbci(path, reqHex, height);
  }

  if (!response) {
    log.warn(`[reconcile] Empty ABCI response for ${address}/${denom}`);
    return 0n;
  }
  if (response.code !== 0) {
    throw new Error(`ABCI Error ${response.code}: ${response.log}`);
  }
  if (!response.value) {
    return 0n;
  }

  const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), root);
  const balance = (decoded as any).balance;
  return asBigInt(balance?.amount);
}

// Fetch all-denom balances via ABCI Query/AllBalances at a specific height, handling pagination.
async function fetchAllBalancesViaAbci(
  rpc: RpcClient,
  root: Root,
  address: string,
  height: number,
): Promise<Map<string, bigint>> {
  const ReqType = root.lookupType('cosmos.bank.v1beta1.QueryAllBalancesRequest');
  const ResType = 'cosmos.bank.v1beta1.QueryAllBalancesResponse';
  const path = '/cosmos.bank.v1beta1.Query/AllBalances';

  const allCoins: any[] = [];
  let nextKey: string | null = null;

  for (; ;) {
    const reqPayload: any = {
      address,
      pagination: { limit: 1000 },
    };
    if (nextKey) {
      reqPayload.pagination.key = Buffer.from(nextKey, 'base64');
    }

    const reqMsg = ReqType.create(reqPayload);
    const reqBytes = ReqType.encode(reqMsg).finish();
    const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

    let response: any;
    try {
      const params = {
        path: `"${path}"`,
        data: reqHex,
        height: String(height),
      };
      const j = await rpc.getJson('/abci_query', params);
      response = j.result?.response ?? j;
    } catch {
      response = await rpc.queryAbci(path, reqHex, height);
    }

    if (!response) break;
    if (response.code !== 0) {
      throw new Error(`ABCI Error ${response.code}: ${response.log}`);
    }
    if (!response.value) break;

    const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), root) as any;
    const balances = Array.isArray(decoded?.balances) ? decoded.balances : [];
    allCoins.push(...balances);

    const page = decoded?.pagination ?? {};
    const nk = page?.next_key ?? page?.nextKey ?? null;
    nextKey = typeof nk === 'string' && nk.length > 0 ? nk : null;
    if (!nextKey) break;
  }

  return normalizeRpcBalances(allCoins);
}
