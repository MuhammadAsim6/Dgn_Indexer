/**
 * Wallet Roller Job — Processes dirty wallets and recalculates:
 *   1. wallet_activities (denormalized trade log)
 *   2. wallet_token_positions (cost basis + realized PnL)
 *   3. wallet_stats_window (aggregated stats per time window)
 *   4. wallet_token_stats_window (per-token stats per window)
 *
 * Runs every 60 seconds, processes up to WALLET_ROLLER_BATCH_SIZE dirty wallets per tick.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/wallet-roller');

const BATCH_SIZE = Number(process.env.WALLET_ROLLER_BATCH_SIZE) || 100;

const WINDOWS = [
    { win: '24h', interval: '24 hours' },
    { win: '7d', interval: '7 days' },
    { win: '30d', interval: '30 days' },
    { win: 'all', interval: null },
] as const;

export async function runWalletRoller(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        // 1. Fetch dirty wallets
        const { rows: dirtyRows } = await client.query(`
            SELECT wd.wallet_id, w.address
            FROM dex.wallet_dirty wd
            JOIN dex.wallets w ON w.wallet_id = wd.wallet_id
            ORDER BY wd.last_marked_at ASC
            LIMIT $1
        `, [BATCH_SIZE]);

        if (dirtyRows.length === 0) return;

        const walletIds = dirtyRows.map((r: any) => r.wallet_id);

        await client.query('BEGIN');

        // 2. Upsert wallet_activities from dex.trades for these wallets
        await client.query(`
            INSERT INTO dex.wallet_activities (
                wallet_id, trade_id, trade_created_at, pool_id, action, direction,
                token_in_id, token_out_id, amount_in_base, amount_out_base,
                price_in_zig, price_in_usd, value_zig, value_usd,
                tx_hash, msg_index, realized_pnl_zig, realized_pnl_usd
            )
            SELECT
                w.wallet_id,
                t.trade_id,
                t.created_at,
                t.pool_id,
                t.action,
                t.direction,
                -- token_in/out depends on direction:
                -- buy: paying quote (in) for base (out)
                -- sell: paying base (in) for quote (out)
                CASE WHEN t.direction = 'buy' THEN p.quote_token_id ELSE p.base_token_id END,
                CASE WHEN t.direction = 'buy' THEN p.base_token_id ELSE p.quote_token_id END,
                t.offer_amount_base,
                t.return_amount_base,
                t.price_in_zig,
                t.price_in_usd,
                t.value_in_zig,
                t.value_in_usd,
                t.tx_hash,
                t.msg_index,
                NULL, -- realized PnL calculated below
                NULL
            FROM dex.trades t
            JOIN dex.wallets w ON w.address = t.signer
            LEFT JOIN dex.pools p ON p.pool_id = t.pool_id
            WHERE w.wallet_id = ANY($1::bigint[])
              AND t.action = 'swap'
              AND NOT EXISTS (
                  SELECT 1 FROM dex.wallet_activities wa
                  WHERE wa.wallet_id = w.wallet_id
                    AND wa.trade_id = t.trade_id
                    AND wa.trade_created_at = t.created_at
              )
        `, [walletIds]);

        // 3. Recalculate wallet_token_positions (weighted average cost basis)
        //    For buys: increase position, update weighted avg cost
        //    For sells: decrease position, realize PnL
        await client.query(`
            WITH trade_summary AS (
                SELECT
                    w.wallet_id,
                    CASE WHEN t.direction = 'buy' THEN p.base_token_id
                         WHEN t.direction = 'sell' THEN p.base_token_id
                         ELSE NULL END AS token_id,
                    t.direction,
                    SUM(CASE
                        WHEN t.direction = 'buy' THEN t.return_amount_base
                        WHEN t.direction = 'sell' THEN -t.offer_amount_base
                        ELSE 0
                    END) AS net_amount,
                    SUM(CASE
                        WHEN t.direction = 'buy' THEN COALESCE(t.value_in_zig, 0)
                        ELSE 0
                    END) AS total_buy_value_zig,
                    SUM(CASE
                        WHEN t.direction = 'sell' THEN COALESCE(t.value_in_zig, 0)
                        ELSE 0
                    END) AS total_sell_value_zig,
                    MIN(CASE WHEN t.direction = 'buy' THEN t.created_at ELSE NULL END) AS first_buy,
                    MAX(t.created_at) AS last_trade
                FROM dex.trades t
                JOIN dex.wallets w ON w.address = t.signer
                LEFT JOIN dex.pools p ON p.pool_id = t.pool_id
                WHERE w.wallet_id = ANY($1::bigint[])
                  AND t.action = 'swap'
                  AND p.base_token_id IS NOT NULL
                GROUP BY w.wallet_id, p.base_token_id, t.direction
            ),
            aggregated AS (
                SELECT
                    wallet_id,
                    token_id,
                    SUM(net_amount) AS total_amount,
                    SUM(total_buy_value_zig) AS total_cost_zig,
                    SUM(total_sell_value_zig) AS total_revenue_zig,
                    MIN(first_buy) AS first_buy_at,
                    MAX(last_trade) AS last_trade_at
                FROM trade_summary
                WHERE token_id IS NOT NULL
                GROUP BY wallet_id, token_id
            )
            INSERT INTO dex.wallet_token_positions (
                wallet_id, token_id, amount_base, cost_basis_zig,
                realized_pnl_zig, first_buy_at, last_trade_at, updated_at
            )
            SELECT
                wallet_id, token_id, total_amount, total_cost_zig,
                total_revenue_zig - total_cost_zig,
                first_buy_at, last_trade_at, NOW()
            FROM aggregated
            ON CONFLICT (wallet_id, token_id) DO UPDATE SET
                amount_base      = EXCLUDED.amount_base,
                cost_basis_zig   = EXCLUDED.cost_basis_zig,
                realized_pnl_zig = EXCLUDED.realized_pnl_zig,
                first_buy_at     = COALESCE(dex.wallet_token_positions.first_buy_at, EXCLUDED.first_buy_at),
                last_trade_at    = GREATEST(dex.wallet_token_positions.last_trade_at, EXCLUDED.last_trade_at),
                updated_at       = NOW()
        `, [walletIds]);

        // 4. Recalculate wallet_stats_window for each time window
        for (const w of WINDOWS) {
            const timeFilter = w.interval
                ? `AND t.created_at > NOW() - INTERVAL '${w.interval}'`
                : '';

            await client.query(`
                INSERT INTO dex.wallet_stats_window (
                    wallet_id, win, as_of, volume_zig, volume_usd,
                    tx_count, realized_pnl_usd, win_rate, portfolio_value_usd
                )
                SELECT
                    wal.wallet_id,
                    $1::dex.wallet_window,
                    NOW(),
                    COALESCE(SUM(t.value_in_zig), 0),
                    COALESCE(SUM(t.value_in_usd), 0),
                    COUNT(*),
                    NULL, -- realized_pnl_usd deferred to portfolio-snapshot
                    CASE WHEN COUNT(CASE WHEN t.direction = 'sell' THEN 1 END) > 0
                         THEN (
                             -- Win rate = profitable token positions / total token positions traded
                             SELECT COALESCE(
                                 COUNT(*) FILTER (WHERE wtp.realized_pnl_zig > 0)::numeric
                                 / NULLIF(COUNT(*), 0),
                                 0
                             )
                             FROM dex.wallet_token_positions wtp
                             WHERE wtp.wallet_id = wal.wallet_id
                               AND wtp.realized_pnl_zig IS NOT NULL
                         )
                         ELSE NULL END,
                    NULL -- portfolio_value_usd deferred to portfolio-snapshot
                FROM dex.wallets wal
                JOIN dex.trades t ON t.signer = wal.address AND t.action = 'swap'
                WHERE wal.wallet_id = ANY($2::bigint[])
                  ${timeFilter}
                GROUP BY wal.wallet_id
                ON CONFLICT (wallet_id, win) DO UPDATE SET
                    as_of      = NOW(),
                    volume_zig = EXCLUDED.volume_zig,
                    volume_usd = EXCLUDED.volume_usd,
                    tx_count   = EXCLUDED.tx_count,
                    win_rate   = EXCLUDED.win_rate,
                    -- Keep existing realized_pnl_usd and portfolio_value_usd
                    realized_pnl_usd    = COALESCE(EXCLUDED.realized_pnl_usd, dex.wallet_stats_window.realized_pnl_usd),
                    portfolio_value_usd = COALESCE(EXCLUDED.portfolio_value_usd, dex.wallet_stats_window.portfolio_value_usd)
            `, [w.win, walletIds]);
        }

        // 5. Recalculate wallet_token_stats_window
        for (const w of WINDOWS) {
            const timeFilter = w.interval
                ? `AND t.created_at > NOW() - INTERVAL '${w.interval}'`
                : '';

            await client.query(`
                INSERT INTO dex.wallet_token_stats_window (
                    wallet_id, token_id, win, as_of,
                    tx_count, volume_usd, bought_usd, sold_usd,
                    realized_pnl_usd, avg_cost_usd
                )
                SELECT
                    wal.wallet_id,
                    p.base_token_id,
                    $1::dex.wallet_window,
                    NOW(),
                    COUNT(*),
                    COALESCE(SUM(t.value_in_usd), 0),
                    COALESCE(SUM(CASE WHEN t.direction = 'buy'  THEN t.value_in_usd ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN t.direction = 'sell' THEN t.value_in_usd ELSE 0 END), 0),
                    NULL, -- realized_pnl_usd
                    CASE WHEN SUM(CASE WHEN t.direction = 'buy' THEN t.return_amount_base ELSE 0 END) > 0
                         THEN SUM(CASE WHEN t.direction = 'buy' THEN t.value_in_usd ELSE 0 END)
                              / SUM(CASE WHEN t.direction = 'buy' THEN t.return_amount_base ELSE 0 END)
                         ELSE NULL END
                FROM dex.wallets wal
                JOIN dex.trades t ON t.signer = wal.address AND t.action = 'swap'
                LEFT JOIN dex.pools p ON p.pool_id = t.pool_id
                WHERE wal.wallet_id = ANY($2::bigint[])
                  AND p.base_token_id IS NOT NULL
                  ${timeFilter}
                GROUP BY wal.wallet_id, p.base_token_id
                ON CONFLICT (wallet_id, token_id, win) DO UPDATE SET
                    as_of            = NOW(),
                    tx_count         = EXCLUDED.tx_count,
                    volume_usd       = EXCLUDED.volume_usd,
                    bought_usd       = EXCLUDED.bought_usd,
                    sold_usd         = EXCLUDED.sold_usd,
                    avg_cost_usd     = EXCLUDED.avg_cost_usd
            `, [w.win, walletIds]);
        }

        // 6. Remove processed dirty flags
        await client.query(`
            DELETE FROM dex.wallet_dirty WHERE wallet_id = ANY($1::bigint[])
        `, [walletIds]);

        await client.query('COMMIT');

        log.info(`[wallet-roller] processed ${dirtyRows.length} dirty wallets`);
    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
    } finally {
        client.release();
    }
}
