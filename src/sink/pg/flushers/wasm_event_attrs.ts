import type { PoolClient } from 'pg';
import { insertWasmEventAttrs } from '../inserters/wasm_event_attrs.js';

/**
 * Flushes WASM event attributes.
 */
export async function flushWasmEventAttrs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  await insertWasmEventAttrs(client, rows);
}
