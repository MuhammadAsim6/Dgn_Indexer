/**
 * Price Ticker Job — Snapshots dex.prices → dex.price_ticks every 30s.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/price-ticker');

export async function runPriceTicker(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        const result = await client.query(`
      INSERT INTO dex.price_ticks (pool_id, token_id, price_in_zig, ts)
      SELECT pool_id, token_id, price_in_zig, NOW()
      FROM dex.prices
      WHERE price_in_zig IS NOT NULL
    `);
        const count = result.rowCount ?? 0;
        if (count > 0) {
            log.debug(`[price-ticker] snapshotted ${count} price ticks`);
        }
    } finally {
        client.release();
    }
}
