/**
 * Alert Evaluator Job — Evaluates user-defined alerts against market data.
 * Runs every 30 seconds.
 *
 * ✅ FIX #12: Reads happen outside the transaction; only writes are transactional.
 * ✅ FIX #13: Whale alerts track last_seen_tx to prevent re-firing on same trade.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/alert-evaluator');

export async function runAlertEvaluator(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        // ✅ FIX #12: Read active alerts OUTSIDE the transaction
        const { rows: alerts } = await client.query(`
            SELECT alert_id, wallet_id, alert_type, params, last_seen_tx
            FROM dex.alerts
            WHERE is_active = TRUE
              AND (last_triggered IS NULL OR NOW() > last_triggered + (throttle_sec * INTERVAL '1 second'))
        `);

        if (alerts.length === 0) return;

        // ✅ FIX #12: Batch-read all prices for price alerts outside transaction
        const priceAlertTokenIds = alerts
            .filter(a => a.alert_type === 'price_above' || a.alert_type === 'price_below')
            .map(a => Number(a.params?.token_id))
            .filter(id => id > 0);

        const priceMap = new Map<number, number>();
        if (priceAlertTokenIds.length > 0) {
            const { rows: priceRows } = await client.query(`
                SELECT DISTINCT ON (token_id) token_id, price_in_usd
                FROM dex.prices
                WHERE token_id = ANY($1)
                ORDER BY token_id, updated_at DESC
            `, [priceAlertTokenIds]);
            for (const r of priceRows) {
                if (r.price_in_usd != null) priceMap.set(Number(r.token_id), Number(r.price_in_usd));
            }
        }

        // Evaluate all alerts in memory
        const triggered: { alert: any; payload: any }[] = [];

        for (const alert of alerts) {
            let shouldTrigger = false;
            let payload: any = null;

            try {
                switch (alert.alert_type) {
                    case 'price_above': {
                        const tokenId = Number(alert.params?.token_id);
                        const threshold = Number(alert.params?.threshold);
                        if (!tokenId || !threshold) break;

                        const currentPrice = priceMap.get(tokenId);
                        if (currentPrice !== undefined && currentPrice > threshold) {
                            shouldTrigger = true;
                            payload = { current_price: currentPrice, threshold };
                        }
                        break;
                    }

                    case 'price_below': {
                        const tokenId = Number(alert.params?.token_id);
                        const threshold = Number(alert.params?.threshold);
                        if (!tokenId || !threshold) break;

                        const currentPrice = priceMap.get(tokenId);
                        if (currentPrice !== undefined && currentPrice < threshold) {
                            shouldTrigger = true;
                            payload = { current_price: currentPrice, threshold };
                        }
                        break;
                    }

                    case 'whale_trade': {
                        const poolId = alert.params?.pool_id ? Number(alert.params.pool_id) : null;
                        const minVal = Number(alert.params?.min_value_zig) || 10000;

                        let query = `
                            SELECT tx_hash, value_in_zig, direction, pool_id, signer
                            FROM dex.trades
                            WHERE value_in_zig >= $1
                              AND created_at > (NOW() - INTERVAL '1 minute')
                              AND action = 'swap'
                        `;
                        const queryParams: any[] = [minVal];

                        if (poolId) {
                            query += ` AND pool_id = $2`;
                            queryParams.push(poolId);
                        }

                        query += ` ORDER BY created_at DESC LIMIT 1`;

                        const { rows } = await client.query(query, queryParams);

                        if (rows.length > 0) {
                            // ✅ FIX #13: Skip if we already triggered on this exact trade
                            if (alert.last_seen_tx === rows[0].tx_hash) break;

                            shouldTrigger = true;
                            payload = {
                                tx_hash: rows[0].tx_hash,
                                value_zig: rows[0].value_in_zig,
                                direction: rows[0].direction,
                                pool_id: rows[0].pool_id,
                                signer: rows[0].signer
                            };
                        }
                        break;
                    }
                }

                if (shouldTrigger && payload) {
                    triggered.push({ alert, payload });
                }
            } catch (evalErr: any) {
                log.error(`[alert-evaluator] error evaluating alert ${alert.alert_id}: ${evalErr.message}`);
            }
        }

        // ✅ FIX #12: Only open a short transaction for writes
        if (triggered.length > 0) {
            await client.query('BEGIN');
            try {
                for (const { alert, payload } of triggered) {
                    // Update last_triggered + last_seen_tx
                    await client.query(`
                        UPDATE dex.alerts
                        SET last_triggered = NOW(),
                            last_seen_tx = $2
                        WHERE alert_id = $1
                    `, [alert.alert_id, payload.tx_hash ?? null]);

                    // Insert event
                    await client.query(`
                        INSERT INTO dex.alert_events (alert_id, wallet_id, kind, payload)
                        VALUES ($1, $2, $3, $4)
                    `, [alert.alert_id, alert.wallet_id, alert.alert_type, JSON.stringify(payload)]);
                }
                await client.query('COMMIT');
                log.info(`[alert-evaluator] triggered ${triggered.length} alerts`);
            } catch (writeErr: any) {
                try { await client.query('ROLLBACK'); } catch { /* ignore */ }
                throw writeErr;
            }
        }
    } catch (err: any) {
        log.error(`[alert-evaluator] error: ${err.message}`);
    } finally {
        client.release();
    }
}
