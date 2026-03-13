import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export async function insertFactorySupplyEvents(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = [
        'height', 'tx_hash', 'msg_index', 'event_index', 'denom', 'action',
        'amount', 'sender', 'recipient', 'metadata'
    ];
    await execBatchedInsert(
        client,
        'tokens.factory_supply_events',
        cols,
        rows,
        'ON CONFLICT (height, tx_hash, msg_index, event_index, denom, action) DO NOTHING'
    );
}
