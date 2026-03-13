import type { PoolClient } from 'pg';
import { insertUnknownMessages } from '../inserters/unknown_msgs.js';

/**
 * Flush unknown/undecoded messages to the quarantine table.
 */
export async function flushUnknownMessages(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    await client.query(`SET LOCAL statement_timeout = '30s'`);
    await client.query(`SET LOCAL lock_timeout = '5s'`);

    await insertUnknownMessages(client, rows);
}
