/**
 * Matrix Roller Job — Aggregates pool_matrix and token_matrix from dex.trades.
 * Also refreshes the ohlcv_1m materialized view.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/matrix-roller');

const WINDOWS = [
    { bucket: '24h', interval: '24 hours' },
    { bucket: '7d', interval: '7 days' },
    { bucket: '30d', interval: '30 days' },
    { bucket: 'all', interval: null },     // all-time
] as const;

export async function runMatrixRoller(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // 1. Pool Matrix — volume + tx counts per pool per window
        for (const w of WINDOWS) {
            const timeFilter = w.interval
                ? `AND t.created_at > NOW() - INTERVAL '${w.interval}'`
                : '';

            await client.query(`
        INSERT INTO dex.pool_matrix (pool_id, bucket, vol_buy_zig, vol_sell_zig, tx_buy, tx_sell, updated_at)
        SELECT
          t.pool_id,
          $1::dex.wallet_window,
          COALESCE(SUM(CASE WHEN t.direction = 'buy'  THEN t.value_in_zig ELSE 0 END), 0),
          COALESCE(SUM(CASE WHEN t.direction = 'sell' THEN t.value_in_zig ELSE 0 END), 0),
          COALESCE(SUM(CASE WHEN t.direction = 'buy'  THEN 1 ELSE 0 END), 0),
          COALESCE(SUM(CASE WHEN t.direction = 'sell' THEN 1 ELSE 0 END), 0),
          NOW()
        FROM dex.trades t
        WHERE t.action = 'swap' AND t.pool_id IS NOT NULL
          ${timeFilter}
        GROUP BY t.pool_id
        ON CONFLICT (pool_id, bucket) DO UPDATE SET
          vol_buy_zig  = EXCLUDED.vol_buy_zig,
          vol_sell_zig = EXCLUDED.vol_sell_zig,
          tx_buy       = EXCLUDED.tx_buy,
          tx_sell      = EXCLUDED.tx_sell,
          updated_at   = NOW()
      `, [w.bucket]);
        }

        // 2. Update token_holders_stats from balances (before token_matrix so data is fresh)
        await client.query(`
      INSERT INTO dex.token_holders_stats (token_id, holders_count, updated_at)
      SELECT token_id, COUNT(*), NOW()
      FROM dex.holders
      WHERE balance_base > 0
      GROUP BY token_id
      ON CONFLICT (token_id) DO UPDATE SET
        holders_count = EXCLUDED.holders_count,
        updated_at    = NOW()
    `);

        // 3. Token Matrix — tvl, mcap, fdv, buy/sell volume per token per window
        for (const w of WINDOWS) {
            const timeFilter = w.interval
                ? `AND t.created_at > NOW() - INTERVAL '${w.interval}'`
                : '';

            await client.query(`
        INSERT INTO dex.token_matrix (token_id, bucket, tvl_zig, mcap_zig, fdv_zig, vol_buy_zig, vol_sell_zig, updated_at)
        SELECT
          tr.token_id,
          $1::dex.wallet_window,
          -- tvl_zig: sum of holder balances × price
          CASE WHEN dp.price_in_zig IS NOT NULL
               THEN COALESCE(ths.holders_total_base, 0) * dp.price_in_zig
               ELSE NULL END,
          -- mcap_zig: circulating supply × price
          CASE WHEN tr.total_supply_base IS NOT NULL AND dp.price_in_zig IS NOT NULL
               THEN tr.total_supply_base * dp.price_in_zig
               ELSE NULL END,
          -- fdv_zig: max supply × price
          CASE WHEN tr.max_supply_base IS NOT NULL AND dp.price_in_zig IS NOT NULL
               THEN tr.max_supply_base * dp.price_in_zig
               ELSE NULL END,
          -- vol_buy_zig / vol_sell_zig from trades
          COALESCE(tv.vol_buy, 0),
          COALESCE(tv.vol_sell, 0),
          NOW()
        FROM tokens.registry tr
        LEFT JOIN (
          SELECT DISTINCT ON (token_id) token_id, price_in_zig
          FROM dex.prices
          WHERE price_in_zig IS NOT NULL
          ORDER BY token_id, is_pair_native DESC NULLS LAST, updated_at DESC
        ) dp ON dp.token_id = tr.token_id
        LEFT JOIN (
          SELECT token_id, SUM(balance_base) AS holders_total_base
          FROM dex.holders
          WHERE balance_base > 0
          GROUP BY token_id
        ) ths ON ths.token_id = tr.token_id
        LEFT JOIN (
          SELECT p.base_token_id AS token_id,
                 SUM(CASE WHEN t.direction = 'buy'  THEN t.value_in_zig ELSE 0 END) AS vol_buy,
                 SUM(CASE WHEN t.direction = 'sell' THEN t.value_in_zig ELSE 0 END) AS vol_sell
          FROM dex.trades t
          JOIN dex.pools p ON p.pool_id = t.pool_id
          WHERE t.action = 'swap' AND p.base_token_id IS NOT NULL
            ${timeFilter}
          GROUP BY p.base_token_id
        ) tv ON tv.token_id = tr.token_id
        WHERE tr.token_id IS NOT NULL
        ON CONFLICT (token_id, bucket) DO UPDATE SET
          tvl_zig      = EXCLUDED.tvl_zig,
          mcap_zig     = EXCLUDED.mcap_zig,
          fdv_zig      = EXCLUDED.fdv_zig,
          vol_buy_zig  = EXCLUDED.vol_buy_zig,
          vol_sell_zig = EXCLUDED.vol_sell_zig,
          updated_at   = NOW()
      `, [w.bucket]);
        }

        await client.query('COMMIT');
        
        log.info(`[matrix-roller] completed pool_matrix, token_matrix, holder_stats`);
    } catch (err: any) {
        if (client) {
            try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        }
        throw err;
    } finally {
        client.release();
    }
}
