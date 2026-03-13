import type { PoolClient } from 'pg';
import { insertWasmEvents } from '../inserters/wasm_events.js';

/**
 * Flushes WASM specific events using the centralized Inserter logic.
 */
export async function flushWasmEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  // Delegate to Inserter
  await insertWasmEvents(client, rows);
}