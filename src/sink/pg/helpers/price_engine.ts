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
 *
 * @param batchMinTime - Oldest block time in the current batch.
 *   During historical sync, this allows the engine to look back far enough
 *   to find the trades just inserted (instead of using NOW() - '1 hour').
 */
export async function deriveAndUpsertPrices(
  client: PoolClient,
  tradeCount: number,
  batchMinTime?: Date,
): Promise<void> {
  if (tradeCount === 0) return;

  // ✅ FIX #4: Use batch start time for historical sync, 1 hour for live
  const timeFilter = batchMinTime
    ? `t.created_at >= $1`
    : `t.created_at > NOW() - INTERVAL '1 hour'`;
  const timeParams: any[] = batchMinTime ? [batchMinTime] : [];

  // 1. Upsert dex.prices from latest swap trades per pool
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
      SELECT price AS zig_usd, updated_at FROM dex.current_prices WHERE symbol = 'ZIG_USD'
    )
    INSERT INTO dex.prices (token_id, pool_id, price_in_zig, price_in_usd, is_pair_native, updated_at)
    SELECT
      ls.token_id,
      ls.pool_id,
      CASE WHEN ls.is_uzig_quote THEN ls.price_in_quote ELSE NULL END,
      CASE WHEN ls.is_uzig_quote AND lzu.updated_at > NOW() - INTERVAL '30 minutes'
           THEN ls.price_in_quote * lzu.zig_usd ELSE NULL END,
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
      AND ${timeFilter}
  `, timeParams);

  // 3. For non-uzig pools, try to derive price_in_zig via intermediate ZIG pool
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
      AND ${timeFilter}
  `, timeParams);

  // ✅ FIX #15: Only use ZIG_USD if it has been confirmed by the oracle (not stale seed)
  const zigUsdResult = await client.query(`
    SELECT price AS zig_usd, updated_at FROM dex.current_prices WHERE symbol = 'ZIG_USD'
  `);
  const zigUsdRow = zigUsdResult.rows[0];
  const isStale = !zigUsdRow || !zigUsdRow.updated_at ||
    (Date.now() - new Date(zigUsdRow.updated_at).getTime()) > 30 * 60 * 1000;
  const zigUsd = isStale ? null : Number(zigUsdRow.zig_usd || 0);

  // 4. Backfill value_in_zig and value_in_usd on trades
  if (zigUsd != null && zigUsd > 0) {
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
        AND ${timeFilter.replace('$1', '$2')}
    `, [zigUsd, ...(batchMinTime ? [batchMinTime] : [])]);
  } else {
    // Still backfill value_in_zig even without USD rate
    await client.query(`
      UPDATE dex.trades t
      SET
        value_in_zig = (CASE 
          WHEN t.direction = 'buy' THEN t.return_amount_base 
          ELSE t.offer_amount_base 
        END) * t.price_in_zig
      WHERE t.price_in_zig IS NOT NULL
        AND t.value_in_zig IS NULL
        AND ${timeFilter}
    `, timeParams);
  }

  log.debug(`[price-engine] prices derived, ${result.rowCount ?? 0} pools updated${isStale ? ' (ZIG_USD stale, skipping USD)' : ''}`);
}

