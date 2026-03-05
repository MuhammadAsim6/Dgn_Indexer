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

        // 2. Token Matrix — price, mcap, holders per token per window
        for (const w of WINDOWS) {
            await client.query(`
        INSERT INTO dex.token_matrix (token_id, bucket, price_in_zig, mcap_zig, fdv_zig, holders, updated_at)
        SELECT
          tr.token_id,
          $1::dex.wallet_window,
          dp.price_in_zig,
          CASE WHEN tr.total_supply_base IS NOT NULL AND dp.price_in_zig IS NOT NULL
               THEN tr.total_supply_base * dp.price_in_zig
               ELSE NULL END,
          CASE WHEN tr.max_supply_base IS NOT NULL AND dp.price_in_zig IS NOT NULL
               THEN tr.max_supply_base * dp.price_in_zig
               ELSE NULL END,
          ths.holders_count,
          NOW()
        FROM tokens.registry tr
        LEFT JOIN dex.prices dp ON dp.token_id = tr.token_id
        LEFT JOIN dex.token_holders_stats ths ON ths.token_id = tr.token_id
        WHERE tr.token_id IS NOT NULL
        ON CONFLICT (token_id, bucket) DO UPDATE SET
          price_in_zig = EXCLUDED.price_in_zig,
          mcap_zig     = EXCLUDED.mcap_zig,
          fdv_zig      = EXCLUDED.fdv_zig,
          holders      = EXCLUDED.holders,
          updated_at   = NOW()
      `, [w.bucket]);
        }

        // 3. Update token_holders_stats from balances
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

        await client.query('COMMIT');
        
        // 4. Refresh OHLCV materialized view (OUTSIDE transaction)
        try {
            await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY dex.ohlcv_1m`);
        } catch (refreshErr: any) {
            log.warn(`[matrix-roller] concurrent refresh failed, retrying normally: ${refreshErr.message}`);
            await client.query(`REFRESH MATERIALIZED VIEW dex.ohlcv_1m`);
        }

        log.info(`[matrix-roller] completed pool_matrix, token_matrix, ohlcv_1m, holder_stats`);
    } catch (err: any) {
        if (client) {
            try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        }
        throw err;
    } finally {
        client.release();
    }
}
