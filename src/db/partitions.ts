import { PoolClient } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('db/partitions');
// Unique Lock ID for Postgres (Hex for 'part') to prevent race conditions
const PARTITION_LOCK_ID = 0x70617274;

/**
 * Ensures partitions exist for the given height range.
 * Uses "Smart Partitioning" logic to create tables only when needed.
 * All tables now use RANGE partitioning by height for easy archival.
 */
export async function ensureCorePartitions(client: PoolClient, minHeight: number, maxHeight: number): Promise<void> {
  // Full list of partitioned tables (Range by Height)
  const tables = [
    // Core - ✅ events now uses RANGE (not HASH)
    ['core', 'blocks'], ['core', 'transactions'], ['core', 'messages'], ['core', 'event_attrs'],
    ['core', 'events'],  // ✅ CHANGED: Now RANGE partitioned for archival support
    ['core', 'validator_set'], ['core', 'validator_missed_blocks'], ['core', 'network_params'],

    // Modules
    ['bank', 'transfers'], ['bank', 'balance_deltas'],
    ['tokens', 'cw20_transfers'],
    ['wasm', 'executions'], ['wasm', 'events'], ['wasm', 'event_attrs'], ['wasm', 'contract_migrations'],
    ['wasm', 'dex_swaps'], ['wasm', 'admin_changes'],

    // Zigchain
    ['zigchain', 'dex_swaps'], ['zigchain', 'dex_liquidity'],
    ['zigchain', 'wrapper_events'],
    ['tokens', 'factory_supply_events']
  ];

  // 🔒 Lock: Ensure only one worker checks partitions at a time
  await client.query(`SELECT pg_advisory_lock($1)`, [PARTITION_LOCK_ID]);

  try {
    // Table-specific range sizes (must match util.height_part_ranges)
    const RANGE_SIZES: Record<string, number> = {
      'core.events': 100000,
      'core.event_attrs': 100000,
    };
    const DEFAULT_RANGE_SIZE = 500000;

    // Ensure Range partitions for min, max, and min-1 (safety buffer for boundary blocks)
    for (const [schema, table] of tables) {
      const key = `${schema}.${table}`;
      const rangeSize = RANGE_SIZES[key] || DEFAULT_RANGE_SIZE;

      // 1. Ensure partition for minHeight (start of range)
      await client.query(
        `SELECT util.ensure_partition_for_height($1, $2, $3)`,
        [schema, table, minHeight]
      );

      // 2. Ensure partition for minHeight - 1 (for operations like missed_blocks referring back)
      if (minHeight > 0) {
        const prevHeight = minHeight - 1;
        if (Math.floor(prevHeight / rangeSize) !== Math.floor(minHeight / rangeSize)) {
          await client.query(
            `SELECT util.ensure_partition_for_height($1, $2, $3)`,
            [schema, table, prevHeight]
          );
        }
      }

      // 3. Ensure partition for maxHeight (end of range)
      // Only if maxHeight is in a different partition bucket than both minHeight and minHeight-1
      const minBucket = Math.floor(minHeight / rangeSize);
      const maxBucket = Math.floor(maxHeight / rangeSize);
      if (minBucket !== maxBucket) {
        await client.query(
          `SELECT util.ensure_partition_for_height($1, $2, $3)`,
          [schema, table, maxHeight]
        );
      }
    }
  } catch (err: any) {
    log.error(`Failed to ensure partitions: ${err.message}`);
    throw err;
  } finally {
    // 🔓 Unlock
    await client.query(`SELECT pg_advisory_unlock($1)`, [PARTITION_LOCK_ID]);
  }
}

export async function ensureIbcPartitions(client: PoolClient, minSeq: number, maxSeq: number): Promise<void> {
  // IBC packets and transfers are now partitioned by height in ensureCorePartitions.
  // Keeping this function as a stub to avoid breaking callers, but it does nothing.
  return;
}

