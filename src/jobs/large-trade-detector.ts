/**
 * Large Trade Detector — Identifies whale/notable trades and records them.
 * Runs every 60 seconds, scans recent trades above a configurable ZIG threshold.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/large-trade-detector');

const THRESHOLD_ZIG = Number(process.env.LARGE_TRADE_THRESHOLD_ZIG) || 10_000;

export async function runLargeTradeDetector(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        // Find recent trades above the threshold that haven't been recorded yet
        const { rowCount } = await client.query(`
            INSERT INTO dex.large_trades (bucket, pool_id, tx_hash, signer, value_zig, direction, created_at)
            SELECT
                CASE
                    WHEN t.created_at > NOW() - INTERVAL '24 hours' THEN '24h'::dex.wallet_window
                    WHEN t.created_at > NOW() - INTERVAL '7 days'   THEN '7d'::dex.wallet_window
                    WHEN t.created_at > NOW() - INTERVAL '30 days'  THEN '30d'::dex.wallet_window
                    ELSE 'all'::dex.wallet_window
                END,
                t.pool_id,
                t.tx_hash,
                t.signer,
                t.value_in_zig,
                t.direction,
                t.created_at
            FROM dex.trades t
            WHERE t.action = 'swap'
              AND t.value_in_zig IS NOT NULL
              AND t.value_in_zig >= $1
              AND t.created_at > NOW() - INTERVAL '5 minutes'
              AND NOT EXISTS (
                  SELECT 1 FROM dex.large_trades lt
                  WHERE lt.tx_hash = t.tx_hash
              )
        `, [THRESHOLD_ZIG]);

        if (rowCount && rowCount > 0) {
            log.info(`[large-trade-detector] detected ${rowCount} large trades (threshold: ${THRESHOLD_ZIG} ZIG)`);
        }
    } catch (err: any) {
        throw err;
    } finally {
        client.release();
    }
}
