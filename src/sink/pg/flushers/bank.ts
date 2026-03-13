import type { PoolClient } from 'pg';
import { insertBalanceDeltas } from '../inserters/bank.js';

/**
 * Flushes balance deltas to the database.
 */
export async function flushBalanceDeltas(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Safety timeout
    await client.query(`SET LOCAL statement_timeout = '300s'`);

    await insertBalanceDeltas(client, rows);
}
