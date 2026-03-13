import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Insert unknown/undecoded messages into quarantine table.
 * These are messages with missing proto definitions that couldn't be fully decoded.
 */
export async function insertUnknownMessages(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'tx_hash',
        'msg_index',
        'height',
        'type_url',
        'raw_value',
        'signer'
    ];

    const { text, values } = makeMultiInsert(
        'core.unknown_messages',
        cols,
        rows,
        'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING'
    );

    await client.query(text, values);
}
