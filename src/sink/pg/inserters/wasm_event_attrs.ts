import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';
import { MAX_ATTR_VALUE_SIZE } from '../parsing.js';

export async function insertWasmEventAttrs(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'event_index', 'attr_index', 'key', 'value'];

    // ✅ Truncate large attribute values
    const safeRows = rows.map(r => ({
        ...r,
        value: typeof r.value === 'string' && r.value.length > MAX_ATTR_VALUE_SIZE
            ? r.value.substring(0, MAX_ATTR_VALUE_SIZE) + '...[TRUNCATED]'
            : r.value
    }));

    await execBatchedInsert(
        client,
        'wasm.event_attrs',
        cols,
        safeRows,
        'ON CONFLICT (height, tx_hash, msg_index, event_index, attr_index) DO NOTHING',
        {},
        { maxRows: 500 } // Attributes are smaller than JSON blobs, so 500 is safe
    );
}
