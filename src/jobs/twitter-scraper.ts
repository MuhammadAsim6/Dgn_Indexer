/**
 * Twitter Scraper Job — Fetches token social metrics via Twitter API v2.
 * Runs every 6 hours. Skips gracefully if TWITTER_BEARER_TOKEN is not set.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/twitter-scraper');

export async function runTwitterScraper(pool: Pool): Promise<void> {
    const bearerToken = process.env.TWITTER_BEARER_TOKEN;

    if (!bearerToken) {
        log.debug('[twitter-scraper] TWITTER_BEARER_TOKEN not set, skipping');
        return;
    }

    const client = await pool.connect();
    try {
        // 1. Find tokens that have a twitter handle configured somewhere
        // For Phase 5, we expect the user/admin to populate some seed data
        // into token_twitter with just the 'handle' to trigger scaffolding.
        const { rows } = await client.query(`
            SELECT token_id, handle 
            FROM dex.token_twitter 
            WHERE handle IS NOT NULL
              AND (last_refreshed IS NULL OR NOW() > last_refreshed + INTERVAL '6 hours')
            LIMIT 100
        `);

        if (rows.length === 0) {
            log.debug('[twitter-scraper] no handles need refreshing');
            return;
        }

        // Twitter API v2 allows fetching up to 100 users by username per request
        const handles = rows.map((r: any) => r.handle.replace(/^@/, '')); // strip @ if present
        const handleCsv = handles.join(',');

        const url = `https://api.twitter.com/2/users/by?usernames=${handleCsv}&user.fields=description,profile_image_url,public_metrics,verified,verified_type,url,name`;

        log.info(`[twitter-scraper] fetching stats for ${handles.length} handles`);

        const response = await fetch(url, {
            headers: {
                'Authorization': `Bearer ${bearerToken}`,
            },
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Twitter API returned ${response.status}: ${errorText}`);
        }

        const body = await response.json();
        let updatedCount = 0;

        await client.query('BEGIN');

        if (body.data && Array.isArray(body.data)) {
            for (const user of body.data) {
                // Find matching token_id
                const tokens = rows.filter((r: any) =>
                    r.handle.replace(/^@/, '').toLowerCase() === user.username.toLowerCase()
                );

                for (const token of tokens) {
                    await client.query(`
                        UPDATE dex.token_twitter SET
                            user_id = $1,
                            profile_url = $2,
                            name = $3,
                            is_blue_verified = $4,
                            verified_type = $5,
                            followers_count = $6,
                            following_count = $7,
                            statuses_count = $8,
                            description = $9,
                            raw = $10,
                            last_refreshed = NOW(),
                            last_error = NULL,
                            updated_at = NOW()
                        WHERE token_id = $11
                    `, [
                        user.id,
                        user.profile_image_url,
                        user.name,
                        user.verified || false,
                        user.verified_type || null,
                        user.public_metrics?.followers_count || 0,
                        user.public_metrics?.following_count || 0,
                        user.public_metrics?.tweet_count || 0,
                        user.description,
                        JSON.stringify(user),
                        token.token_id
                    ]);
                    updatedCount++;
                }
            }
        }

        // Mark errors for handles that weren't found
        if (body.errors && Array.isArray(body.errors)) {
            for (const err of body.errors) {
                if (err.value) { // The requested username
                    const tokens = rows.filter((r: any) =>
                        r.handle.replace(/^@/, '').toLowerCase() === err.value.toLowerCase()
                    );
                    for (const token of tokens) {
                        await client.query(`
                            UPDATE dex.token_twitter 
                            SET last_refreshed = NOW(), last_error = $1
                            WHERE token_id = $2
                        `, [err.detail || 'Not found', token.token_id]);
                    }
                }
            }
        }

        await client.query('COMMIT');
        log.info(`[twitter-scraper] refreshed ${updatedCount} profiles`);

    } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        log.error(`[twitter-scraper] error: ${err.message}`);
    } finally {
        client.release();
    }
}
