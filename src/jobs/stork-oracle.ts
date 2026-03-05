/**
 * Stork Oracle Job — Fetches ZIG/USD price from CoinMarketCap.
 * Uses CMC_API_KEY env var. Insertions go to dex.exchange_rates.
 * Can be swapped out for Stork or any oracle later.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/stork-oracle');

const CMC_ZIG_URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest';

// Zigchain's CMC ID — update if different
const ZIG_CMC_ID = process.env.ZIG_CMC_ID || '';
const ZIG_CMC_SYMBOL = process.env.ZIG_CMC_SYMBOL || 'ZIG';

export async function runStorkOracle(pool: Pool): Promise<void> {
    const apiKey = process.env.CMC_API_KEY;
    if (!apiKey) {
        log.debug('[stork-oracle] CMC_API_KEY not set, skipping');
        return;
    }

    try {
        // Build URL — prefer ID lookup, fallback to symbol
        const params = ZIG_CMC_ID
            ? `id=${ZIG_CMC_ID}`
            : `symbol=${ZIG_CMC_SYMBOL}`;

        const url = `${CMC_ZIG_URL}?${params}&convert=USD`;

        const response = await fetch(url, {
            headers: {
                'X-CMC_PRO_API_KEY': apiKey,
                'Accept': 'application/json',
            },
        });

        if (!response.ok) {
            log.warn(`[stork-oracle] CMC API returned ${response.status}`);
            return;
        }

        const body = await response.json() as any;

        // Extract price from CMC response
        let priceUsd: number | null = null;

        if (body?.data) {
            // CMC v2 returns data keyed by ID or symbol
            const entries = Object.values(body.data) as any[];
            for (const entry of entries) {
                // Handle array (symbol lookup) or object (id lookup)
                const item = Array.isArray(entry) ? entry[0] : entry;
                if (item?.quote?.USD?.price) {
                    priceUsd = Number(item.quote.USD.price);
                    break;
                }
            }
        }

        if (priceUsd == null || !Number.isFinite(priceUsd) || priceUsd <= 0) {
            log.warn(`[stork-oracle] could not extract ZIG/USD from CMC response`);
            return;
        }

        // Insert into dex.exchange_rates
        const client = await pool.connect();
        try {
            await client.query(
                `INSERT INTO dex.exchange_rates (ts, zig_usd) VALUES (NOW(), $1)
         ON CONFLICT (ts) DO UPDATE SET zig_usd = EXCLUDED.zig_usd`,
                [priceUsd],
            );
            log.info(`[stork-oracle] ZIG/USD = ${priceUsd}`);
        } finally {
            client.release();
        }
    } catch (err: any) {
        log.error(`[stork-oracle] fetch error: ${err.message}`);
    }
}
