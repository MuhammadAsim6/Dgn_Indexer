import type { PoolClient } from 'pg';
import { insertIbcConnections } from '../inserters/ibc_connections.js';

export async function flushIbcConnections(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    await client.query('SET statement_timeout = 30000');
    await client.query('SET lock_timeout = 10000');
    await insertIbcConnections(client, rows);
}
