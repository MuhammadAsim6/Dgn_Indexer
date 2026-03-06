/**
 * Leaderboard Job — Aggregates top traders by volume and PnL per time window.
 * Runs every 5 minutes.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/leaderboard');

const WINDOWS = [
    { bucket: '24h', interval: '24 hours' },
    { bucket: '7d', interval: '7 days' },
    { bucket: '30d', interval: '30 days' },
    { bucket: 'all', interval: null },
] as const;

export async function runLeaderboard(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        for (const w of WINDOWS) {
            const timeFilter = w.interval
                ? `AND t.created_at > NOW() - INTERVAL '${w.interval}'`
                : '';

            await client.query(`
                INSERT INTO dex.leaderboard_traders (
                    bucket, address, trades_count, volume_zig, gross_pnl_zig, updated_at
                )
                SELECT
                    $1::dex.wallet_window,
                    t.signer,
                    COUNT(*),
                    COALESCE(SUM(t.value_in_zig), 0),
                    COALESCE(
                        SUM(CASE WHEN t.direction = 'sell' THEN t.value_in_zig ELSE 0 END) -
                        SUM(CASE WHEN t.direction = 'buy'  THEN t.value_in_zig ELSE 0 END),
                        0
                    ),
                    NOW()
                FROM dex.trades t
                WHERE t.action = 'swap'
                  AND t.signer IS NOT NULL
                  ${timeFilter}
                GROUP BY t.signer
                HAVING COUNT(*) > 0
                ON CONFLICT (bucket, address) DO UPDATE SET
                    trades_count  = EXCLUDED.trades_count,
                    volume_zig    = EXCLUDED.volume_zig,
                    gross_pnl_zig = EXCLUDED.gross_pnl_zig,
                    updated_at    = NOW()
            `, [w.bucket]);
        }

        await client.query('COMMIT');
        log.info(`[leaderboard] updated all window buckets`);
    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
    } finally {
        client.release();
    }
}
