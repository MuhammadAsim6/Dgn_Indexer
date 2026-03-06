/**
 * IBC Market Refresher Job — Fetches market data for IBC tokens from CoinMarketCap.
 * Runs every 1 hour (budget constraint).
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/ibc-market-refresher');

const CMC_QUOTES_URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest';

export async function runIbcMarketRefresher(pool: Pool): Promise<void> {
    const apiKey = process.env.CMC_API_KEY;
    if (!apiKey) {
        log.debug('[ibc-market-refresher] CMC_API_KEY not set, skipping');
        return;
    }

    const client = await pool.connect();
    try {
        // Get all IBC tokens with a defined cmc_id
        const { rows } = await client.query(`
            SELECT token_id, cmc_id 
            FROM dex.ibc_tokens 
            WHERE cmc_id IS NOT NULL
        `);

        if (rows.length === 0) {
            log.debug('[ibc-market-refresher] no IBC tokens with cmc_id, skipping');
            return;
        }

        const cmcIds = rows.map((r: any) => r.cmc_id).join(',');
        const tokenByCmc = new Map<number, number>();
        for (const r of rows) {
            tokenByCmc.set(Number(r.cmc_id), Number(r.token_id));
        }

        // Fetch from CMC (batching up to limits)
        const url = `${CMC_QUOTES_URL}?id=${cmcIds}&convert=USD`;
        const response = await fetch(url, {
            headers: {
                'X-CMC_PRO_API_KEY': apiKey,
                'Accept': 'application/json',
            },
        });

        if (!response.ok) {
            log.warn(`[ibc-market-refresher] CMC API returned ${response.status}`);
            return;
        }

        const body = await response.json() as any;
        if (!body?.data) return;

        let updated = 0;
        await client.query('BEGIN');

        for (const [cmcId, info] of Object.entries(body.data) as any[]) {
            const item = Array.isArray(info) ? info[0] : info;
            const quote = item?.quote?.USD;
            const tokenId = tokenByCmc.get(Number(cmcId));

            if (tokenId != null && quote != null) {
                const priceUsd = quote.price;
                const marketCapUsd = quote.market_cap;
                const circSupply = item.circulating_supply;
                const totalSupply = item.total_supply;

                if (priceUsd != null && Number.isFinite(priceUsd)) {
                    await client.query(`
                        INSERT INTO dex.ibc_token_stats (
                            token_id, price_usd, market_cap_usd, 
                            circulating_supply, total_supply, last_updated
                        )
                        VALUES ($1, $2, $3, $4, $5, NOW())
                        ON CONFLICT (token_id) DO UPDATE SET
                            price_usd          = EXCLUDED.price_usd,
                            market_cap_usd     = EXCLUDED.market_cap_usd,
                            circulating_supply = EXCLUDED.circulating_supply,
                            total_supply       = EXCLUDED.total_supply,
                            last_updated       = NOW()
                    `, [
                        tokenId,
                        priceUsd,
                        marketCapUsd || null,
                        circSupply || null,
                        totalSupply || null
                    ]);
                    updated++;
                }
            }
        }

        await client.query('COMMIT');
        log.info(`[ibc-market-refresher] updated stats for ${updated} IBC tokens`);
    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        log.error(`[ibc-market-refresher] error: ${err.message}`);
    } finally {
        client.release();
    }
}
