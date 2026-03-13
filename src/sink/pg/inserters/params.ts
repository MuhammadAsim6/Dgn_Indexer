import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts network parameter changes into core.network_params.
 */
export async function insertNetworkParams(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['height', 'time', 'module', 'param_key', 'old_value', 'new_value'];
    const { text, values } = makeMultiInsert(
        'core.network_params',
        cols,
        rows,
        'ON CONFLICT (height, module, param_key) DO NOTHING',
        { old_value: 'jsonb', new_value: 'jsonb' }
    );
    await client.query(text, values);
}
