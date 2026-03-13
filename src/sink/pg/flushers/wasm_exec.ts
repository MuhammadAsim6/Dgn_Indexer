import type { PoolClient } from 'pg';
import { insertWasmExec } from '../inserters/wasm_exec.js';

/**
 * Flushes WASM Contract Executions using the centralized Inserter logic.
 */
export async function flushWasmExec(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  // Delegate to Inserter
  await insertWasmExec(client, rows);
}