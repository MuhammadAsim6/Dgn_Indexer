/**
 * DEX Trades Inserter — Maps zigchain.dex_swaps rows into dex.trades.
 * Resolves pool_id from dex.pools and calculates price_in_quote.
 */
import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('sink/pg/inserters/dex_trades');

// Cache: pair_contract → pool_id (populated during flush)
let poolCache: Map<string, number> | null = null;

async function ensurePoolCache(client: PoolClient): Promise<Map<string, number>> {
    if (poolCache && poolCache.size > 0) return poolCache;
    poolCache = new Map();
    const { rows } = await client.query(`SELECT pool_id, pair_contract FROM dex.pools`);
    for (const row of rows) {
        poolCache.set(row.pair_contract, Number(row.pool_id));
    }
    return poolCache;
}

// Also cache base/quote denoms per pool to determine direction
type PoolMeta = { pool_id: number; base_denom: string; quote_denom: string };
let poolMetaCache: Map<string, PoolMeta> | null = null;

async function ensurePoolMetaCache(client: PoolClient): Promise<Map<string, PoolMeta>> {
    if (poolMetaCache && poolMetaCache.size > 0) return poolMetaCache;
    poolMetaCache = new Map();
    const { rows } = await client.query(`
    SELECT p.pool_id, p.pair_contract,
           tb.denom AS base_denom, tq.denom AS quote_denom
    FROM dex.pools p
    LEFT JOIN tokens.registry tb ON tb.token_id = p.base_token_id
    LEFT JOIN tokens.registry tq ON tq.token_id = p.quote_token_id
  `);
    for (const row of rows) {
        poolMetaCache.set(row.pair_contract, {
            pool_id: Number(row.pool_id),
            base_denom: row.base_denom,
            quote_denom: row.quote_denom,
        });
    }
    return poolMetaCache;
}

export function invalidatePoolCaches(): void {
    poolCache = null;
    poolMetaCache = null;
}

/**
 * Build dex.trades rows from zigchain.dex_swaps rows.
 * Should be called AFTER zigchain tables are flushed so dex.pools is populated.
 */
export async function insertDexTrades(
    client: PoolClient,
    zigSwaps: any[],
    wasmSwaps: any[],
    zigLiquidity: any[],
    blockTimes: Map<number, Date>,
    newPoolsAdded = false,
): Promise<void> {
    const trades: any[] = [];

    // ✅ FIX #11: Only invalidate caches when new pools were added
    if (newPoolsAdded) invalidatePoolCaches();
    const meta = await ensurePoolMetaCache(client);

    // 1. Map zigchain.dex_swaps → dex.trades
    for (const s of zigSwaps) {
        const poolInfo = meta.get(String(s.pool_id));
        if (!poolInfo) continue; // Pool not registered yet, skip

        const tokenInAmount = BigInt(String(s.token_in_amount ?? '0'));
        const tokenOutAmount = BigInt(String(s.token_out_amount ?? '0'));

        // Determine direction: buying base = buy, selling base = sell
        let direction: 'buy' | 'sell' | null = null;
        if (s.token_in_denom === poolInfo.quote_denom) direction = 'buy';    // paying quote for base = buy
        else if (s.token_in_denom === poolInfo.base_denom) direction = 'sell'; // selling base for quote = sell

        // Calculate price_in_quote
        let priceInQuote: string | null = null;
        if (tokenInAmount > 0n && tokenOutAmount > 0n) {
            // ✅ FIX: Avoid BigInt integer division truncation — convert to Number first
            if (direction === 'buy') {
                // price = quote_paid / base_received
                priceInQuote = (Number(tokenInAmount) / Number(tokenOutAmount)).toPrecision(18);
            } else if (direction === 'sell') {
                // price = quote_received / base_sold
                priceInQuote = (Number(tokenOutAmount) / Number(tokenInAmount)).toPrecision(18);
            }
        }

        const height = Number(s.block_height);
        const createdAt = blockTimes.get(height) ?? s.timestamp ?? new Date();

        trades.push({
            pool_id: poolInfo.pool_id,
            action: 'swap',
            direction,
            source_kind: 'native_swap',
            msg_index: s.msg_index ?? -1,
            event_index: s.event_index ?? -1,
            offer_asset_denom: s.token_in_denom ?? null,
            ask_asset_denom: s.token_out_denom ?? null,
            offer_amount_base: s.token_in_amount ?? null,
            return_amount_base: s.token_out_amount ?? null,
            height,
            tx_hash: s.tx_hash,
            signer: s.sender_address ?? null,
            memo: s.memo ?? null,
            is_degenter: (s.memo || '').toLowerCase().includes('degenter.io'),
            created_at: createdAt,
            price_in_quote: priceInQuote,
            price_in_zig: null,
            price_in_usd: null,
            value_in_zig: null,
            value_in_usd: null,
        });
    }

    // 2. Map wasm.dex_swaps → dex.trades
    for (const s of wasmSwaps) {
        const pairContract = String(s.pair_contract ?? s.contract ?? '');
        const poolInfo = meta.get(pairContract);

        // Derive direction for WASM trades (Point 4)
        let direction: 'buy' | 'sell' | null = null;
        if (poolInfo) {
            if (s.offer_asset === poolInfo.quote_denom) direction = 'buy';    // paying quote for base
            else if (s.offer_asset === poolInfo.base_denom) direction = 'sell'; // selling base for quote
        }

        // Derive price if not provided
        let priceInQuote: string | null = s.effective_price || s.price_in_quote ? String(s.effective_price ?? s.price_in_quote) : null;
        if (!priceInQuote && poolInfo && direction) {
            const offerAmount = BigInt(String(s.offer_amount ?? '0'));
            const returnAmount = BigInt(String(s.return_amount ?? '0'));
            if (offerAmount > 0n && returnAmount > 0n) {
                // ✅ FIX: Avoid BigInt integer division truncation
                if (direction === 'buy') {
                    priceInQuote = (Number(offerAmount) / Number(returnAmount)).toPrecision(18);
                } else if (direction === 'sell') {
                    priceInQuote = (Number(returnAmount) / Number(offerAmount)).toPrecision(18);
                }
            }
        }

        const height = Number(s.height ?? s.block_height);
        const createdAt = blockTimes.get(height) ?? new Date();

        trades.push({
            pool_id: poolInfo?.pool_id ?? null,
            action: 'swap',
            direction,
            source_kind: 'wasm_swap',
            msg_index: s.msg_index ?? -1,
            event_index: s.event_index ?? -1,
            offer_asset_denom: s.offer_asset ?? null,
            ask_asset_denom: s.ask_asset ?? s.return_asset ?? null,
            offer_amount_base: s.offer_amount ?? null,
            return_amount_base: s.return_amount ?? null,
            height,
            tx_hash: s.tx_hash,
            signer: s.sender ?? s.signer ?? null,
            memo: s.memo ?? null,
            is_degenter: (s.memo || '').toLowerCase().includes('degenter.io'),
            created_at: createdAt,
            price_in_quote: priceInQuote,
            price_in_zig: null,
            price_in_usd: null,
            value_in_zig: null,
            value_in_usd: null,
        });
    }

    // 3. Map zigchain.dex_liquidity → dex.trades
    for (const l of zigLiquidity) {
        const poolInfo = meta.get(String(l.pool_id));
        const height = Number(l.block_height);
        const createdAt = blockTimes.get(height) ?? l.timestamp ?? new Date();
        const actionType = String(l.action_type ?? '').toUpperCase();

        trades.push({
            pool_id: poolInfo?.pool_id ?? null,
            action: actionType === 'ADD' ? 'provide_liquidity' : 'withdraw_liquidity',
            direction: null,
            source_kind: 'liquidity',
            msg_index: l.msg_index ?? -1,
            event_index: -1,
            offer_asset_denom: null,
            ask_asset_denom: null,
            offer_amount_base: l.amount_0 ?? null,
            return_amount_base: l.amount_1 ?? null,
            height,
            tx_hash: l.tx_hash,
            signer: l.sender_address ?? null,
            memo: l.memo ?? null,
            is_degenter: (l.memo || '').toLowerCase().includes('degenter.io'),
            created_at: createdAt,
            price_in_quote: null,
            price_in_zig: null,
            price_in_usd: null,
            value_in_zig: null,
            value_in_usd: null,
        });
    }

    if (trades.length === 0) return;

    const cols = [
        'pool_id', 'action', 'direction', 'source_kind', 'msg_index', 'event_index',
        'offer_asset_denom', 'ask_asset_denom', 'offer_amount_base', 'return_amount_base',
        'height', 'tx_hash', 'signer', 'memo', 'is_degenter', 'created_at',
        'price_in_quote', 'price_in_zig', 'price_in_usd', 'value_in_zig', 'value_in_usd',
    ];

    await execBatchedInsert(
        client,
        'dex.trades',
        cols,
        trades,
        'ON CONFLICT (tx_hash, source_kind, msg_index, event_index, created_at) DO NOTHING',

    );

    log.debug(`[dex-trades] inserted ${trades.length} trades (native=${zigSwaps.length} wasm=${wasmSwaps.length} lp=${zigLiquidity.length})`);
}
