import type { PoolClient } from 'pg';
import { insertIbcDenoms } from '../inserters/ibc_denoms.js';

export async function flushIbcDenoms(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    await client.query('SET statement_timeout = 30000');
    await client.query('SET lock_timeout = 10000');
    await insertIbcDenoms(client, rows);
}
