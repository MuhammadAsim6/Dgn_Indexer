import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

/**
 * Insert WASM swap events from CosmWasm AMM contracts (e.g., Astroport-style).
 * Extracts detailed swap data from WASM event attributes.
 */
export async function insertWasmSwaps(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'tx_hash',
        'msg_index',
        'event_index',
        'contract',
        'sender',
        'receiver',
        'offer_asset',
        'ask_asset',
        'offer_amount',
        'return_amount',
        'spread_amount',
        'commission_amount',
        'maker_fee_amount',
        'fee_share_amount',
        'reserves',
        'pair_id',
        'effective_price',
        'price_impact',
        'total_fee',
        'fee_denom',
        'block_height',
        'timestamp'
    ];



    await execBatchedInsert(
        client,
        'wasm.dex_swaps',
        cols,
        rows,
        'ON CONFLICT (tx_hash, msg_index, event_index, block_height) DO NOTHING'
    );
}


