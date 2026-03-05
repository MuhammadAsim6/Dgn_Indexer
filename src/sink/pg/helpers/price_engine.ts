/**
 * Price Engine — Derives dex.prices from latest dex.trades.
 * Runs inline after trades insertion in flushAll().
 */
import type { PoolClient } from 'pg';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('sink/pg/helpers/price_engine');

/**
 * After trades are inserted, derive the latest price for each pool
 * and upsert into dex.prices.
 * Also backfill price_in_zig on the trades that were just inserted.
 */
export async function deriveAndUpsertPrices(
    client: PoolClient,
    tradeCount: number,
): Promise<void> {
    if (tradeCount === 0) return;

    // 1. Upsert dex.prices from latest swap trades per pool
    //    For each pool, take the most recent trade's price_in_quote.
    //    If the quote is uzig → price_in_zig = price_in_quote directly.
    const result = await client.query(`
    WITH latest_swaps AS (
      SELECT DISTINCT ON (t.pool_id)
        t.pool_id,
        p.base_token_id AS token_id,
        t.price_in_quote,
        p.is_uzig_quote
      FROM dex.trades t
      JOIN dex.pools p ON p.pool_id = t.pool_id
      WHERE t.action = 'swap' AND t.price_in_quote IS NOT NULL AND t.pool_id IS NOT NULL
      ORDER BY t.pool_id, t.created_at DESC, t.trade_id DESC
    )
    INSERT INTO dex.prices (token_id, pool_id, price_in_zig, is_pair_native, updated_at)
    SELECT
      ls.token_id,
      ls.pool_id,
      CASE WHEN ls.is_uzig_quote THEN ls.price_in_quote ELSE NULL END,
      COALESCE(ls.is_uzig_quote, FALSE),
      NOW()
    FROM latest_swaps ls
    WHERE ls.token_id IS NOT NULL
    ON CONFLICT (token_id, pool_id) DO UPDATE SET
      price_in_zig   = COALESCE(EXCLUDED.price_in_zig, dex.prices.price_in_zig),
      is_pair_native = COALESCE(EXCLUDED.is_pair_native, dex.prices.is_pair_native),
      updated_at     = NOW()
  `);

    // 2. Backfill price_in_zig on recent trades where it's NULL
    //    For pools with uzig quote, price_in_zig = price_in_quote
    await client.query(`
    UPDATE dex.trades t
    SET price_in_zig = t.price_in_quote
    FROM dex.pools p
    WHERE t.pool_id = p.pool_id
      AND p.is_uzig_quote = TRUE
      AND t.price_in_zig IS NULL
      AND t.price_in_quote IS NOT NULL
      AND t.action = 'swap'
      AND t.created_at > NOW() - INTERVAL '1 hour'
  `);

    // 3. For non-uzig pools, try to derive price_in_zig via intermediate ZIG pool
    //    (quote_token → find its price_in_zig from another pool → multiply)
    await client.query(`
    UPDATE dex.trades t
    SET price_in_zig = t.price_in_quote * ref.price_in_zig
    FROM dex.pools p
    JOIN dex.prices ref ON ref.token_id = p.quote_token_id AND ref.price_in_zig IS NOT NULL
    WHERE t.pool_id = p.pool_id
      AND p.is_uzig_quote = FALSE
      AND t.price_in_zig IS NULL
      AND t.price_in_quote IS NOT NULL
      AND t.action = 'swap'
      AND t.created_at > NOW() - INTERVAL '1 hour'
  `);

    log.debug(`[price-engine] prices derived, ${result.rowCount ?? 0} pools updated`);
}
