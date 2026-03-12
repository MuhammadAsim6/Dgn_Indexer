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
  //    (Point 2: Also calculate price_in_usd)
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
    ),
    latest_zig_usd AS (
      SELECT price AS zig_usd FROM dex.current_prices WHERE symbol = 'ZIG_USD'
    )
    INSERT INTO dex.prices (token_id, pool_id, price_in_zig, price_in_usd, is_pair_native, updated_at)
    SELECT
      ls.token_id,
      ls.pool_id,
      CASE WHEN ls.is_uzig_quote THEN ls.price_in_quote ELSE NULL END,
      CASE WHEN ls.is_uzig_quote THEN ls.price_in_quote * lzu.zig_usd ELSE NULL END,
      COALESCE(ls.is_uzig_quote, FALSE),
      NOW()
    FROM latest_swaps ls
    LEFT JOIN latest_zig_usd lzu ON TRUE
    WHERE ls.token_id IS NOT NULL
    ON CONFLICT (token_id, pool_id) DO UPDATE SET
      price_in_zig   = COALESCE(EXCLUDED.price_in_zig, dex.prices.price_in_zig),
      price_in_usd   = COALESCE(EXCLUDED.price_in_usd, dex.prices.price_in_usd),
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

  // Fetch the latest ZIG/USD rate for subsequent calculations
  const zigUsdResult = await client.query(`
    SELECT price AS zig_usd FROM dex.current_prices WHERE symbol = 'ZIG_USD'
  `);
  const zigUsd = Number(zigUsdResult.rows[0]?.zig_usd || 0);

  // 4. (Point 3) Backfill value_in_zig and value_in_usd on trades
  //    value_in_zig = amount_base * price_in_zig
    await client.query(`
    UPDATE dex.trades t
    SET 
      price_in_usd = t.price_in_zig * $1,
      value_in_zig = (CASE 
        WHEN t.direction = 'buy' THEN t.return_amount_base 
        ELSE t.offer_amount_base 
      END) * t.price_in_zig,
      value_in_usd = (CASE 
        WHEN t.direction = 'buy' THEN t.return_amount_base 
        ELSE t.offer_amount_base 
      END) * t.price_in_zig * $1
    WHERE t.price_in_zig IS NOT NULL 
      AND t.value_in_usd IS NULL
      AND t.created_at > NOW() - INTERVAL '1 hour'
  `, [zigUsd]);

  log.debug(`[price-engine] prices derived, ${result.rowCount ?? 0} pools updated`);
}
