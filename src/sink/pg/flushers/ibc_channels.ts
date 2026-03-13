import type { PoolClient } from 'pg';
import { insertIbcChannels } from '../inserters/ibc_channels.js';

/**
 * Flushes IBC channels.
 */
export async function flushIbcChannels(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  await insertIbcChannels(client, rows);
}
