import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertIbcDenoms(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by hash to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
    const seenHashes = new Set<string>();
    const finalRows: any[] = [];
    for (const row of rows) {
        if (!seenHashes.has(row.hash)) {
            seenHashes.add(row.hash);
            finalRows.push(row);
        }
    }

    const cols = ['hash', 'full_path', 'base_denom'];
    const { text, values } = makeMultiInsert(
        'ibc.denoms',
        cols,
        finalRows,
        `ON CONFLICT (hash) DO NOTHING`
    );
    await client.query(text, values);
}
