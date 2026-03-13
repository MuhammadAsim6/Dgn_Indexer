import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmAdminChanges(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'old_admin', 'new_admin', 'action'];
    const { text, values } = makeMultiInsert(
        'wasm.admin_changes',
        cols,
        rows,
        'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING'
    );
    await client.query(text, values);
}
