import type { PoolClient } from 'pg';
import { insertTokenRegistry } from '../inserters/tokens.js';

export async function flushTokenRegistry(client: PoolClient, rows: any[]): Promise<void> {
    if (rows.length > 0) {
        await insertTokenRegistry(client, rows);
    }
}
