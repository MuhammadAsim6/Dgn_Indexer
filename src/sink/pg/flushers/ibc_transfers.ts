import type { PoolClient } from 'pg';
import { insertIbcTransfers } from '../inserters/ibc_transfers.js';

/**
 * Flushes IBC transfer rows.
 */
export async function flushIbcTransfers(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  await insertIbcTransfers(client, rows);
}
