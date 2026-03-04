import type { PoolClient } from 'pg';

/**
 * Inserts unified trade rows into dex.trades from existing swap + LP snapshots.
 * Resolves pool_id via sub-SELECT on dex.pools(pair_contract).
 * Direction derived: if offer_asset == pool base denom → 'sell', else 'buy'.
 * LP events have direction = NULL.
 *
 * ON CONFLICT (tx_hash, source_kind, msg_index, event_index) DO NOTHING
 * ensures full idempotency on re-index / retry.
 */
export async function insertDexTrades(
    client: PoolClient,
    nativeSwaps: any[],
    wasmSwaps: any[],
    liquidityRows: any[],
): Promise<void> {
    // Build unified rows from the three sources
    const rows: TradeRow[] = [];

    // 1. Native swaps (from zigchain.dex_swaps snapshot)
    for (const s of (nativeSwaps ?? [])) {
        rows.push({
            pair_contract: s.pool_id,           // zigchain pool_id string
            source_kind: 'native_swap',
            msg_index: s.msg_index ?? 0,
            event_index: s.event_index ?? -1,
            action: 'swap',
            offer_asset_denom: s.token_in_denom ?? null,
            ask_asset_denom: s.token_out_denom ?? null,
            offer_amount_base: s.token_in_amount ?? null,
            return_amount_base: s.token_out_amount ?? null,
            height: s.block_height,
            tx_hash: s.tx_hash,
            signer: s.sender_address ?? null,
            created_at: s.timestamp ?? new Date().toISOString(),
            // ✅ FIX A: price_in_quote is computed in SQL via pool base_token_id lookup.
            // Do NOT pass raw effective_price here — it is direction-ambiguous (always return/offer).
            // The SQL CASE expression in insertDexTrades resolves direction correctly.
            price_in_quote: null,
        });
    }

    // 2. WASM swaps (from wasm.dex_swaps snapshot)
    for (const s of (wasmSwaps ?? [])) {
        rows.push({
            pair_contract: s.contract,           // WASM contract address
            source_kind: 'wasm_swap',
            msg_index: s.msg_index ?? 0,
            event_index: s.event_index ?? -1,
            action: 'swap',
            offer_asset_denom: s.offer_asset ?? null,
            ask_asset_denom: s.ask_asset ?? null,
            offer_amount_base: s.offer_amount ?? null,
            return_amount_base: s.return_amount ?? null,
            height: s.block_height,
            tx_hash: s.tx_hash,
            signer: s.sender ?? null,
            created_at: s.timestamp ?? new Date().toISOString(),
            // ✅ FIX A: price_in_quote computed in SQL; raw effective_price is not direction-safe.
            price_in_quote: null,
        });
    }

    // 3. Liquidity events (from zigchain.dex_liquidity snapshot)
    for (const l of (liquidityRows ?? [])) {
        const actionType = String(l.action_type ?? '').toUpperCase();
        // ✅ FIX: Skip unknown action types instead of defaulting to withdraw
        const mappedAction = actionType === 'ADD' ? 'provide_liquidity'
            : actionType === 'REMOVE' ? 'withdraw_liquidity'
                : null;
        if (!mappedAction) continue;
        rows.push({
            pair_contract: l.pool_id,
            source_kind: 'liquidity',
            msg_index: l.msg_index ?? 0,
            event_index: -1,
            action: mappedAction,
            offer_asset_denom: null,
            ask_asset_denom: null,
            offer_amount_base: l.amount_0 ?? null,
            return_amount_base: l.amount_1 ?? null,
            height: l.block_height,
            tx_hash: l.tx_hash,
            signer: l.sender_address ?? null,
            created_at: l.timestamp ?? new Date().toISOString(),
            price_in_quote: null,
        });
    }

    // Batch in chunks of 100 to avoid exceeding parameter limits
    const COLS_PER_ROW = 13; // price_in_quote is computed in SQL; removed from params
    const CHUNK = 100;
    for (let start = 0; start < rows.length; start += CHUNK) {
        const chunk = rows.slice(start, Math.min(start + CHUNK, rows.length));
        const chunkValues: any[] = [];
        const chunkPlaceholders: string[] = [];

        for (let ci = 0; ci < chunk.length; ci++) {
            const r = chunk[ci]!;
            const b = ci * COLS_PER_ROW;
            chunkValues.push(
                r.pair_contract,      // $b+1
                r.source_kind,        // $b+2
                r.msg_index,          // $b+3
                r.event_index,        // $b+4
                r.action,             // $b+5
                r.offer_asset_denom,  // $b+6
                r.ask_asset_denom,    // $b+7
                r.offer_amount_base,  // $b+8
                r.return_amount_base, // $b+9
                r.height,             // $b+10
                r.tx_hash,            // $b+11
                r.signer,             // $b+12
                r.created_at,         // $b+13
            );
            const poolSel = `(SELECT pool_id FROM dex.pools WHERE pair_contract = $${b + 1} LIMIT 1)`;
            // ✅ FIX A: Single base denom lookup shared by both direction and price_in_quote.
            // baseDenomSel runs the pool JOIN exactly once per row in the generated SQL.
            const baseDenomSel = `(SELECT tr.denom
               FROM dex.pools p
               JOIN tokens.registry tr ON tr.token_id = p.base_token_id
               WHERE p.pair_contract = $${b + 1} LIMIT 1)`;
            const dirSel = r.action === 'swap'
                ? `(CASE
                     WHEN ${baseDenomSel} IS NULL      THEN NULL::dex.trade_direction
                     WHEN $${b + 6} = ${baseDenomSel} THEN 'sell'::dex.trade_direction
                     ELSE                                   'buy'::dex.trade_direction
                   END)`
                : 'NULL::dex.trade_direction';
            // price_in_quote: sell → return/offer, buy → offer/return, LP/unknown → NULL
            const priceSel = r.action === 'swap'
                ? `(CASE
                     WHEN ${baseDenomSel} IS NULL THEN NULL
                     WHEN $${b + 8}::numeric IS NULL OR $${b + 9}::numeric IS NULL THEN NULL
                     WHEN $${b + 8}::numeric = 0      OR $${b + 9}::numeric = 0     THEN NULL
                     WHEN $${b + 6} = ${baseDenomSel}
                       THEN ($${b + 9}::numeric / $${b + 8}::numeric)
                     ELSE   ($${b + 8}::numeric / $${b + 9}::numeric)
                   END)`
                : 'NULL::numeric';
            chunkPlaceholders.push(
                `(${poolSel},$${b + 5}::dex.trade_action,${dirSel},` +
                `$${b + 2},$${b + 3},$${b + 4},` +
                `$${b + 6},$${b + 7},$${b + 8},$${b + 9},` +
                `$${b + 10},$${b + 11},$${b + 12},$${b + 13},${priceSel})`
            );
        }

        await client.query(
            `INSERT INTO dex.trades
             (pool_id, action, direction,
              source_kind, msg_index, event_index,
              offer_asset_denom, ask_asset_denom, offer_amount_base, return_amount_base,
              height, tx_hash, signer, created_at, price_in_quote)
           VALUES ${chunkPlaceholders.join(', ')}
           ON CONFLICT (tx_hash, source_kind, msg_index, event_index, created_at) DO NOTHING`,
            chunkValues,
        );
    }
}

type TradeRow = {
    pair_contract: string;
    source_kind: string;
    msg_index: number;
    event_index: number;
    action: string;
    offer_asset_denom: string | null;
    ask_asset_denom: string | null;
    offer_amount_base: string | null;
    return_amount_base: string | null;
    height: number;
    tx_hash: string;
    signer: string | null;
    created_at: string;
    price_in_quote: number | null;
};
