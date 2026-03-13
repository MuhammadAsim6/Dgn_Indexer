import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertIbcConnections(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by connection_id
    const mergedMap = new Map<string, any>();
    for (const row of rows) {
        const key = row.connection_id;
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

    const cols = ['connection_id', 'client_id', 'counterparty_connection_id', 'counterparty_client_id', 'state'];
    const { text, values } = makeMultiInsert(
        'ibc.connections',
        cols,
        finalRows,
        `ON CONFLICT (connection_id) DO UPDATE SET
       client_id = COALESCE(EXCLUDED.client_id, ibc.connections.client_id),
       counterparty_connection_id = COALESCE(EXCLUDED.counterparty_connection_id, ibc.connections.counterparty_connection_id),
       counterparty_client_id = COALESCE(EXCLUDED.counterparty_client_id, ibc.connections.counterparty_client_id),
       state = EXCLUDED.state
    `
    );
    await client.query(text, values);
}
