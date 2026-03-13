
import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// Re-use logic from indexer to connect
const createPgPool = () => {
    return new Pool({
        host: process.env.PG_HOST,
        port: Number(process.env.PG_PORT || 5432),
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        database: process.env.PG_DB,
        max: 5,
    });
};

async function main() {
    console.log('[Analytics] Connecting to database...');
    const pool = createPgPool();

    try {
        console.log('[Analytics] Triggering refresh_all_analytics(30 days)...');
        const start = Date.now();
        await pool.query('SELECT util.refresh_all_analytics(30)');
        const elapsed = ((Date.now() - start) / 1000).toFixed(2);
        console.log(`[Analytics] ✅ Refreshed successfully in ${elapsed}s`);
    } catch (err) {
        console.error('[Analytics] ❌ Failed to refresh:', err);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

main();
