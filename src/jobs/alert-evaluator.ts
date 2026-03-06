/**
 * Alert Evaluator Job — Evaluates user-defined alerts against market data.
 * Runs every 30 seconds.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/alert-evaluator');

export async function runAlertEvaluator(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // 1. Fetch active alerts that haven't triggered within their throttle window
        const { rows: alerts } = await client.query(`
            SELECT alert_id, wallet_id, alert_type, params
            FROM dex.alerts
            WHERE is_active = TRUE
              AND (last_triggered IS NULL OR NOW() > last_triggered + (throttle_sec * INTERVAL '1 second'))
        `);

        if (alerts.length === 0) {
            await client.query('COMMIT');
            return;
        }

        let triggeredCount = 0;

        for (const alert of alerts) {
            let shouldTrigger = false;
            let payload: any = null;

            try {
                switch (alert.alert_type) {
                    case 'price_above': {
                        // params: { token_id: 123, threshold: 0.05 }
                        const tokenId = Number(alert.params?.token_id);
                        const threshold = Number(alert.params?.threshold);

                        if (!tokenId || !threshold) break;

                        const { rows } = await client.query(`
                            SELECT price_in_usd
                            FROM dex.prices
                            WHERE token_id = $1
                            ORDER BY updated_at DESC LIMIT 1
                        `, [tokenId]);

                        const currentPrice = rows[0]?.price_in_usd;

                        if (currentPrice !== undefined && currentPrice > threshold) {
                            shouldTrigger = true;
                            payload = { current_price: currentPrice, threshold };
                        }
                        break;
                    }

                    case 'price_below': {
                        // params: { token_id: 123, threshold: 0.05 }
                        const tokenId = Number(alert.params?.token_id);
                        const threshold = Number(alert.params?.threshold);

                        if (!tokenId || !threshold) break;

                        const { rows } = await client.query(`
                            SELECT price_in_usd
                            FROM dex.prices
                            WHERE token_id = $1
                            ORDER BY updated_at DESC LIMIT 1
                        `, [tokenId]);

                        const currentPrice = rows[0]?.price_in_usd;

                        if (currentPrice !== undefined && currentPrice < threshold) {
                            shouldTrigger = true;
                            payload = { current_price: currentPrice, threshold };
                        }
                        break;
                    }

                    case 'whale_trade': {
                        // params: { pool_id: null (all) or 123, min_value_zig: 50000 }
                        const poolId = alert.params?.pool_id ? Number(alert.params.pool_id) : null;
                        const minVal = Number(alert.params?.min_value_zig) || 10000;

                        let query = `
                            SELECT tx_hash, value_zig, direction, pool_id, signer
                            FROM dex.large_trades
                            WHERE value_zig >= $1
                              AND created_at > (NOW() - INTERVAL '1 minute')
                        `;
                        const queryParams: any[] = [minVal];

                        if (poolId) {
                            query += ` AND pool_id = $2`;
                            queryParams.push(poolId);
                        }

                        query += ` ORDER BY created_at DESC LIMIT 1`;

                        const { rows } = await client.query(query, queryParams);

                        if (rows.length > 0) {
                            shouldTrigger = true;
                            payload = {
                                tx_hash: rows[0].tx_hash,
                                value_zig: rows[0].value_zig,
                                direction: rows[0].direction,
                                pool_id: rows[0].pool_id,
                                signer: rows[0].signer
                            };
                        }
                        break;
                    }

                    // Add more alert types here (volume_spike, etc.)
                }

                if (shouldTrigger && payload) {
                    // Update last_triggered
                    await client.query(`
                        UPDATE dex.alerts 
                        SET last_triggered = NOW() 
                        WHERE alert_id = $1
                    `, [alert.alert_id]);

                    // Insert into events
                    await client.query(`
                        INSERT INTO dex.alert_events (alert_id, wallet_id, kind, payload)
                        VALUES ($1, $2, $3, $4)
                    `, [alert.alert_id, alert.wallet_id, alert.alert_type, JSON.stringify(payload)]);

                    triggeredCount++;
                }
            } catch (evalErr: any) {
                log.error(`[alert-evaluator] error evaluating alert ${alert.alert_id}: ${evalErr.message}`);
                // continue with next alert
            }
        }

        await client.query('COMMIT');

        if (triggeredCount > 0) {
            log.info(`[alert-evaluator] triggered ${triggeredCount} alerts`);
        }
    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
    } finally {
        client.release();
    }
}
