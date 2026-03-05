/**
 * External Prices Job — Fetches token prices from CoinMarketCap.
 * Only fetches for tokens that have a cmc_id in dex.ibc_tokens.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/external-prices');

const CMC_QUOTES_URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest';

export async function runExternalPrices(pool: Pool): Promise<void> {
    const apiKey = process.env.CMC_API_KEY;
    if (!apiKey) {
        log.debug('[external-prices] CMC_API_KEY not set, skipping');
        return;
    }

    const client = await pool.connect();
    try {
        // Get all tokens with cmc_id
        const { rows } = await client.query(`
      SELECT token_id, cmc_id FROM dex.ibc_tokens WHERE cmc_id IS NOT NULL
    `);

        if (rows.length === 0) {
            log.debug('[external-prices] no tokens with cmc_id, skipping');
            return;
        }

        const cmcIds = rows.map((r: any) => r.cmc_id).join(',');
        const tokenByCmc = new Map<number, number>();
        for (const r of rows) {
            tokenByCmc.set(Number(r.cmc_id), Number(r.token_id));
        }

        // Fetch from CMC
        const url = `${CMC_QUOTES_URL}?id=${cmcIds}&convert=USD`;
        const response = await fetch(url, {
            headers: {
                'X-CMC_PRO_API_KEY': apiKey,
                'Accept': 'application/json',
            },
        });

        if (!response.ok) {
            log.warn(`[external-prices] CMC API returned ${response.status}`);
            return;
        }

        const body = await response.json() as any;
        if (!body?.data) return;

        let updated = 0;
        for (const [cmcId, info] of Object.entries(body.data) as any[]) {
            const item = Array.isArray(info) ? info[0] : info;
            const priceUsd = item?.quote?.USD?.price;
            const tokenId = tokenByCmc.get(Number(cmcId));

            if (tokenId != null && priceUsd != null && Number.isFinite(priceUsd)) {
                await client.query(`
          INSERT INTO dex.external_prices (token_id, source, price_usd, updated_at)
          VALUES ($1, 'cmc', $2, NOW())
          ON CONFLICT (token_id, source) DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            updated_at = NOW()
        `, [tokenId, priceUsd]);
                updated++;
            }
        }

        log.info(`[external-prices] updated ${updated} token prices from CMC`);
    } catch (err: any) {
        log.error(`[external-prices] error: ${err.message}`);
    } finally {
        client.release();
    }
}
