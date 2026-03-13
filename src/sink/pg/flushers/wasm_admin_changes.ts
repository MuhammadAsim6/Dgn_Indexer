import type { PoolClient } from 'pg';
import { insertWasmAdminChanges } from '../inserters/wasm_admin_changes.js';

/**
 * Flushes WASM admin changes for security auditing.
 */
export async function flushWasmAdminChanges(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    await client.query(`SET LOCAL statement_timeout = '30s'`);
    await client.query(`SET LOCAL lock_timeout = '5s'`);

    await insertWasmAdminChanges(client, rows);
}
