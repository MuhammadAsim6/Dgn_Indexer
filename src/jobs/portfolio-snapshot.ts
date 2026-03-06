/**
 * Portfolio Snapshot Job — Takes daily snapshots of wallet portfolio values.
 * Runs once every 24 hours.
 * Calculates total portfolio value in USD using current token prices.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/portfolio-snapshot');

export async function runPortfolioSnapshot(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        // Snapshot portfolio values for all wallets with non-zero positions.
        // Uses the latest dex.prices and dex.exchange_rates for USD conversion.
        const { rowCount } = await client.query(`
            INSERT INTO dex.wallet_portfolio_snapshots (wallet_id, ts, value_usd)
            SELECT
                wtp.wallet_id,
                date_trunc('day', NOW()),
                SUM(
                    CASE WHEN wtp.amount_base > 0 AND dp.price_in_zig IS NOT NULL AND er.zig_usd IS NOT NULL
                         THEN wtp.amount_base * dp.price_in_zig * er.zig_usd
                         ELSE 0
                    END
                )
            FROM dex.wallet_token_positions wtp
            LEFT JOIN dex.prices dp ON dp.token_id = wtp.token_id
            LEFT JOIN LATERAL (
                SELECT zig_usd FROM dex.exchange_rates ORDER BY ts DESC LIMIT 1
            ) er ON TRUE
            WHERE wtp.amount_base > 0
            GROUP BY wtp.wallet_id
            HAVING SUM(
                CASE WHEN wtp.amount_base > 0 AND dp.price_in_zig IS NOT NULL AND er.zig_usd IS NOT NULL
                     THEN wtp.amount_base * dp.price_in_zig * er.zig_usd
                     ELSE 0
                END
            ) > 0
            ON CONFLICT (wallet_id, ts) DO UPDATE SET
                value_usd = EXCLUDED.value_usd
        `);

        // Also update portfolio_value_usd in wallet_stats_window for all wallets
        await client.query(`
            UPDATE dex.wallet_stats_window ws
            SET portfolio_value_usd = snap.value_usd
            FROM (
                SELECT wallet_id, value_usd
                FROM dex.wallet_portfolio_snapshots
                WHERE ts = date_trunc('day', NOW())
            ) snap
            WHERE ws.wallet_id = snap.wallet_id
        `);

        log.info(`[portfolio-snapshot] snapshotted ${rowCount ?? 0} wallet portfolios`);
    } catch (err: any) {
        throw err;
    } finally {
        client.release();
    }
}
