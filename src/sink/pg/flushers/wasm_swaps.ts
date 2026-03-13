import type { PoolClient } from 'pg';
import { insertWasmSwaps } from '../inserters/wasm_swaps.js';

/**
 * Flush WASM swap data to the database.
 */
export async function flushWasmSwaps(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Set timeouts for safety
    await client.query(`SET LOCAL statement_timeout = '30s'`);
    await client.query(`SET LOCAL lock_timeout = '5s'`);

    await insertWasmSwaps(client, rows);
}
