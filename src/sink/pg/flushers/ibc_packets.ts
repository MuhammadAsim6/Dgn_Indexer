import type { PoolClient } from 'pg';
import { insertIbcPackets } from '../inserters/ibc_packets.js';

/**
 * Flushes IBC packets.
 * Critical: Uses the Inserter that handles TEXT conversion for sequences/timeouts.
 */
export async function flushIbcPackets(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  // Delegate to the Safe Inserter
  await insertIbcPackets(client, rows);
}