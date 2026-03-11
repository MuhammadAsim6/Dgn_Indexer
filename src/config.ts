// src/config.ts
import { SinkKind } from './sink/types.js';
import { ArgMap, Config, ReconcileMode } from './types.js';
import { loadDotEnvIfPresent } from './config/dotenv.js';
import { parseArgv } from './config/argv.js';
import { asBool, asLogLevel, asPgMode, asPositiveInt, asString } from './config/parsers.js';
import { validateConfig } from './config/validate.js';
export { printConfig } from './config/printer.js';

/**
 * Build and return the runtime configuration.
 */
export function getConfig(): Config {
  loadDotEnvIfPresent();
  const args: ArgMap = parseArgv();

  const rpcUrl = asString('RPC_URL', (args.rpcUrl as string) ?? process.env.RPC_URL ?? 'http://127.0.0.1:26657');

  const fromRawArg = args.from as string | boolean | undefined;
  const fromEnv = process.env.FROM;
  const from =
    typeof fromRawArg === 'string'
      ? asPositiveInt('from', fromRawArg)
      : fromEnv
        ? asPositiveInt('from', fromEnv)
        : undefined;

  const toRawArg = args.to as string | boolean | undefined;
  const toEnv = process.env.TO;
  const wantsLatest =
    (typeof toRawArg === 'string' && toRawArg.toLowerCase() === 'latest') ||
    (typeof toEnv === 'string' && toEnv.toLowerCase() === 'latest');

  const to = wantsLatest
    ? undefined
    : typeof toRawArg === 'string'
      ? asPositiveInt('to', toRawArg)
      : toEnv
        ? asPositiveInt('to', toEnv)
        : undefined;

  const resolveLatestTo = wantsLatest;

  const resume = asBool('resume', args['resume'] ?? process.env.RESUME ?? false, false);

  const shards = asPositiveInt('shards', (args.shards as string) ?? process.env.SHARDS ?? 1);
  const shardId = asPositiveInt('shard-id', (args['shard-id'] as string) ?? process.env.SHARD_ID ?? 0);
  if (shards <= 0) throw new Error(`shards must be >= 1, got ${shards}`);
  if (shardId < 0 || shardId >= shards) throw new Error(`shard-id must be in [0..${shards - 1}], got ${shardId}`);

  const concurrency = asPositiveInt('concurrency', (args.concurrency as string) ?? process.env.CONCURRENCY ?? 48);
  const timeoutMs = asPositiveInt('timeout-ms', (args['timeout-ms'] as string) ?? process.env.TIMEOUT_MS ?? 5000);
  const rps = asPositiveInt('rps', (args.rps as string) ?? process.env.RPS ?? 150);
  const retries = asPositiveInt('retries', (args.retries as string) ?? process.env.RETRIES ?? 3);
  const backoffMs = asPositiveInt('backoff-ms', (args['backoff-ms'] as string) ?? process.env.BACKOFF_MS ?? 250);
  const backoffJitter = Number((args['backoff-jitter'] as string) ?? process.env.BACKOFF_JITTER ?? 0.3);
  if (!(backoffJitter >= 0 && backoffJitter <= 1))
    throw new Error(`backoff-jitter must be in [0..1], got ${backoffJitter}`);

  const logLevel = asLogLevel(args['log-level'] ?? process.env.LOG_LEVEL ?? 'info');

  if (!rpcUrl.startsWith('http://') && !rpcUrl.startsWith('https://')) {
    throw new Error(`RPC_URL must start with http:// or https://, got "${rpcUrl}"`);
  }
  if (from !== undefined && to !== undefined && !resolveLatestTo && to < from) {
    throw new Error(`to (${to}) must be >= from (${from})`);
  }

  const caseFrom = (args.case as string | undefined) ?? process.env.CASE ?? process.env.CASE_MODE ?? 'snake';
  const caseMode = caseFrom.toLowerCase() === 'camel' ? 'camel' : 'snake';

  const progressEveryBlocks = Math.max(
    1,
    Number(args['progress-every-blocks'] ?? process.env.PROGRESS_EVERY_BLOCKS ?? 1000),
  );
  const progressIntervalSec = Math.max(
    1,
    Number(args['progress-interval-sec'] ?? process.env.PROGRESS_INTERVAL_SEC ?? 15),
  );

  const sinkKind = String(args['sink'] ?? process.env.SINK ?? 'stdout') as SinkKind;
  const outPath = args['out'] ? String(args['out']) : process.env.OUT || undefined;
  const flushEvery = args['flush-every']
    ? Number(args['flush-every'])
    : process.env.FLUSH_EVERY
      ? Number(process.env.FLUSH_EVERY)
      : undefined;
  const firstBlock = args['first-block']
    ? Number(args['first-block'])
    : process.env.FIRST_BLOCK
      ? Number(process.env.FIRST_BLOCK)
      : 5200792;

  const follow = asBool('follow', args['follow'] ?? process.env.FOLLOW ?? false, false);
  const followIntervalMs = asPositiveInt(
    'follow-interval-ms',
    (args['follow-interval-ms'] as string) ?? process.env.FOLLOW_INTERVAL_MS ?? 5000,
  );
  const missingRetryIntervalRaw = (args['missing-retry-interval-ms'] as string) ?? process.env.MISSING_RETRY_INTERVAL_MS;
  const missingRetryIntervalMs =
    missingRetryIntervalRaw !== undefined ? Number(missingRetryIntervalRaw) : 600_000;
  if (!Number.isFinite(missingRetryIntervalMs) || missingRetryIntervalMs < 0) {
    throw new Error(`missing-retry-interval-ms must be >= 0, got ${missingRetryIntervalRaw}`);
  }

  const reconcileModeRaw = String(
    (args['reconcile-mode'] as string | undefined) ?? process.env.RECONCILE_MODE ?? 'full-once-then-negative',
  )
    .trim()
    .toLowerCase();
  const reconcileModeMap: Record<string, ReconcileMode> = {
    off: 'off',
    'negative-only': 'negative-only',
    negative: 'negative-only',
    'full-once-then-negative': 'full-once-then-negative',
    full: 'full-once-then-negative',
  };
  const reconcileMode = reconcileModeMap[reconcileModeRaw];
  if (!reconcileMode) {
    throw new Error(
      `reconcile-mode must be one of: off, negative-only, full-once-then-negative (got "${reconcileModeRaw}")`,
    );
  }

  const reconcileIntervalMs = asPositiveInt(
    'reconcile-interval-ms',
    (args['reconcile-interval-ms'] as string | undefined) ?? process.env.RECONCILE_INTERVAL_MS ?? 300_000,
  );
  const reconcileMaxLagBlocks = asPositiveInt(
    'reconcile-max-lag-blocks',
    (args['reconcile-max-lag-blocks'] as string | undefined) ?? process.env.RECONCILE_MAX_LAG_BLOCKS ?? 10_000,
  );
  const reconcileFullBatchSize = asPositiveInt(
    'reconcile-full-batch-size',
    (args['reconcile-full-batch-size'] as string | undefined) ?? process.env.RECONCILE_FULL_BATCH_SIZE ?? 200,
  );
  const reconcileStateId = asString(
    'reconcile-state-id',
    (args['reconcile-state-id'] as string | undefined) ?? process.env.RECONCILE_STATE_ID ?? 'default',
  );

  const raw = {
    rpcUrl,
    from,
    to,
    shards,
    shardId,
    concurrency,
    timeoutMs,
    rps,
    retries,
    backoffMs,
    backoffJitter,
    logLevel,
    resolveLatestTo,
    caseMode,
    progressEveryBlocks,
    progressIntervalSec,
    sinkKind,
    outPath,
    flushEvery,
    resume,
    firstBlock,
    follow,
    followIntervalMs,
    missingRetryIntervalMs,
    reconcileMode,
    reconcileIntervalMs,
    reconcileMaxLagBlocks,
    reconcileFullBatchSize,
    reconcileStateId,
    pg: {
      host: (args['pg-host'] as string | undefined) ?? process.env.PG_HOST,
      port: args['pg-port'] ? Number(args['pg-port']) : Number(process.env.PG_PORT ?? 5432),
      user: (args['pg-user'] as string | undefined) ?? process.env.PG_USER ?? process.env.PGUSERNAME,
      password: (args['pg-pass'] as string | undefined) ?? process.env.PG_PASS ?? process.env.PG_PASSWORD,
      database: (args['pg-db'] as string | undefined) ?? process.env.PG_DB ?? process.env.PGDATABASE,
      ssl: asBool('pg-ssl', args['pg-ssl'] ?? process.env.PG_SSL ?? false, false),
      mode: asPgMode(args['pg-mode'] ?? process.env.PG_MODE) ?? 'batch-insert',
      batchBlocks: args['pg-batch-blocks']
        ? Number(args['pg-batch-blocks'])
        : Number(process.env.PG_BATCH_BLOCKS ?? 1000),
      batchTxs: args['pg-batch-txs'] ? Number(args['pg-batch-txs']) : Number(process.env.PG_BATCH_TXS ?? 2000),
      poolSize: asPositiveInt('pg-pool-size', (args['pg-pool-size'] as string) ?? process.env.PG_POOL_SIZE ?? 16, 16),
      progressId:
        (args['pg-progress-id'] as string | undefined) ??
        (process.env.PG_PROGRESS_ID as string | undefined) ??
        'default',
    },
  };

  return validateConfig(raw);
}
