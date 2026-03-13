import type { PoolClient } from 'pg';
import { insertNetworkParams } from '../inserters/params.js';

/**
 * Flushes network parameters to the database.
 */
export async function flushNetworkParams(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Safety timeout
    await client.query(`SET LOCAL statement_timeout = '30s'`);

    await insertNetworkParams(client, rows);
}
