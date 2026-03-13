import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertIbcClients(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by client_id to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
    const mergedMap = new Map<string, any>();
    for (const row of rows) {
        const key = row.client_id;
        const existing = mergedMap.get(key);
        if (!existing) {
            mergedMap.set(key, { ...row });
        } else {
            for (const [k, v] of Object.entries(row)) {
                if (v !== null && v !== undefined) existing[k] = v;
            }
        }
    }
    const finalRows = Array.from(mergedMap.values());

    const cols = ['client_id', 'chain_id', 'client_type', 'updated_at_height', 'updated_at_time'];
    const { text, values } = makeMultiInsert(
        'ibc.clients',
        cols,
        finalRows,
        `ON CONFLICT (client_id) DO UPDATE SET
       chain_id = COALESCE(EXCLUDED.chain_id, ibc.clients.chain_id),
       client_type = COALESCE(EXCLUDED.client_type, ibc.clients.client_type),
       updated_at_height = EXCLUDED.updated_at_height,
       updated_at_time = EXCLUDED.updated_at_time
    `
    );
    await client.query(text, values);
}
