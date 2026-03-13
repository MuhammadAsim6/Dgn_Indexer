import type { PoolClient } from 'pg';
import { insertIbcClients } from '../inserters/ibc_clients.js';

export async function flushIbcClients(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    await client.query('SET statement_timeout = 30000');
    await client.query('SET lock_timeout = 10000');
    await insertIbcClients(client, rows);
}
