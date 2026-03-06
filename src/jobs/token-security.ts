/**
 * Token Security Job — Scans tokens for risk factors.
 * Runs every 1 hour. No external API calls.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/token-security');

export async function runTokenSecurity(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Factory tokens have predictable creator addresses and minting behavior on Cosmos
        const { rows: tokens } = await client.query(`
            SELECT 
                t.token_id, 
                t.denom,
                t.decimals
            FROM tokens.registry t
            WHERE t.denom LIKE 'factory/%'
        `);

        if (tokens.length === 0) {
            await client.query('COMMIT');
            return;
        }

        let scannedCount = 0;

        for (const token of tokens) {
            try {
                // Parse creator address from factory denom: factory/{creator_address}/{subdenom}
                const parts = token.denom.split('/');
                const creatorAddress = parts.length >= 2 ? parts[1] : null;

                // For Cosmos runtime, mintable often depends on specific contract state or bank module params.
                // Here we use heuristics: if max_supply is null or very large, it might be mintable.
                // Actual minting capability check requires RPC query to the contract/module.

                // 1. Get total supply (we'd ideally query the node, but for now we aggregate known balances)
                // In a real production app, total_supply should be fetched directly via RPC /cosmos/bank/v1beta1/supply
                const { rows: supplyRows } = await client.query(`
                    SELECT SUM(balance_base) as total
                    FROM bank.balances_current bc,
                         jsonb_each_text(bc.balances) kv
                    WHERE kv.key = $1
                `, [token.denom]);

                const totalSupplyBase = supplyRows[0]?.total ? BigInt(supplyRows[0].total) : 0n;

                // 2. Get creator balance
                let creatorBalanceBase = 0n;
                if (creatorAddress) {
                    const { rows: creatorRows } = await client.query(`
                        SELECT balance_base 
                        FROM dex.holders 
                        WHERE token_id = $1 AND address = $2
                    `, [token.token_id, creatorAddress]);

                    if (creatorRows.length > 0) {
                        creatorBalanceBase = BigInt(creatorRows[0].balance_base);
                    }
                }

                // 3. Get top 10 holders percentage
                const { rows: top10Rows } = await client.query(`
                    SELECT SUM(balance_base::numeric) as top10_sum
                    FROM (
                        SELECT balance_base 
                        FROM dex.holders 
                        WHERE token_id = $1
                        ORDER BY balance_base::numeric DESC 
                        LIMIT 10
                    ) sub
                `, [token.token_id]);

                const top10Sum = top10Rows[0]?.top10_sum ? BigInt(Math.floor(Number(top10Rows[0].top10_sum))) : 0n;

                // 4. Get total holders count
                const { rows: holdersCountRows } = await client.query(`
                    SELECT COUNT(*) as cnt
                    FROM dex.holders 
                    WHERE token_id = $1 AND balance_base > 0
                `, [token.token_id]);

                const holdersCount = Number(holdersCountRows[0]?.cnt || 0);

                // Calculate percentages
                let creatorPct = 0;
                let top10Pct = 0;

                if (totalSupplyBase > 0n) {
                    creatorPct = Number((creatorBalanceBase * 10000n) / totalSupplyBase) / 100;
                    top10Pct = Number((top10Sum * 10000n) / totalSupplyBase) / 100;
                }

                // Determine risk flags
                const riskFlags: string[] = [];
                if (creatorPct > 50) riskFlags.push('creator_owns_>50%');
                else if (creatorPct > 20) riskFlags.push('creator_owns_>20%');

                if (top10Pct > 90) riskFlags.push('top10_owns_>90%');
                else if (top10Pct > 70) riskFlags.push('top10_owns_>70%');

                if (holdersCount < 100) riskFlags.push('low_holders');

                // Upsert into dex.token_security
                await client.query(`
                    INSERT INTO dex.token_security (
                        token_id, denom, is_mintable, can_change_minting_cap,
                        max_supply_base, total_supply_base, creator_address,
                        creator_balance_base, creator_pct_of_max, top10_pct_of_max,
                        holders_count, first_seen_at, risk_flags, checked_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, (SELECT MIN(created_at) FROM dex.trades WHERE pool_id IN (SELECT pool_id FROM dex.pools WHERE base_token_id=$1 OR quote_token_id=$1)), $12, NOW())
                    ON CONFLICT (token_id) DO UPDATE SET
                        is_mintable            = EXCLUDED.is_mintable,
                        total_supply_base      = EXCLUDED.total_supply_base,
                        creator_balance_base   = EXCLUDED.creator_balance_base,
                        creator_pct_of_max     = EXCLUDED.creator_pct_of_max,
                        top10_pct_of_max       = EXCLUDED.top10_pct_of_max,
                        holders_count          = EXCLUDED.holders_count,
                        risk_flags             = EXCLUDED.risk_flags,
                        checked_at             = NOW()
                `, [
                    token.token_id,
                    token.denom,
                    null, // is_mintable (requires RPC)
                    null, // can_change_minting_cap
                    null, // max_supply (from contract info)
                    totalSupplyBase.toString(),
                    creatorAddress,
                    creatorBalanceBase.toString(),
                    creatorPct,
                    top10Pct,
                    holdersCount,
                    JSON.stringify(riskFlags)
                ]);

                scannedCount++;
            } catch (tokenErr: any) {
                log.error(`[token-security] error processing token ${token.denom}: ${tokenErr.message}`);
                // Continue with next token
            }
        }

        await client.query('COMMIT');

        if (scannedCount > 0) {
            log.info(`[token-security] scanned ${scannedCount} tokens for risk factors`);
        }
    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
    } finally {
        client.release();
    }
}
