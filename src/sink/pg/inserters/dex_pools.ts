import type { PoolClient } from 'pg';

export type DexPoolRow = {
    pair_contract: string;     // zigchain pool_id string (unique identifier)
    base_denom: string | null;
    quote_denom: string | null;
    lp_token_denom?: string | null;
    pair_id?: string | null;
    signer?: string | null;
    created_height?: number | null;
    created_tx_hash?: string | null;
};

/**
 * Upserts pool rows into dex.pools.
 * Resolves base_token_id / quote_token_id from tokens.registry in a single round-trip.
 * IMPORTANT: flushTokenRegistry must run before this in each flushAll() cycle
 *            so the FK sub-SELECT can find the token_id.
 */
export async function insertDexPools(client: PoolClient, rows: DexPoolRow[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by pair_contract — riches row (with metadata) wins
    const byContract = new Map<string, DexPoolRow>();
    for (const row of rows) {
        if (!row.pair_contract?.trim()) continue;
        const existing = byContract.get(row.pair_contract);
        // Favor row with metadata (created_height/signer) over metadata-less row
        const isRicher = !existing || (existing.created_height == null && row.created_height != null);
        if (isRicher) {
            byContract.set(row.pair_contract, row);
        }
    }
    const unique = Array.from(byContract.values());
    if (!unique.length) return;

    // Build a single multi-row INSERT using sub-SELECTs to resolve FK IDs.
    // This avoids an extra round-trip to fetch token_ids before inserting.
    const values: any[] = [];
    const placeholders = unique.map((r, i) => {
        const b = i * 8;
        values.push(
            r.pair_contract,
            r.base_denom ?? null,
            r.quote_denom ?? null,
            r.lp_token_denom ?? null,
            r.pair_id ?? null,
            r.signer ?? null,
            r.created_height ?? null,
            r.created_tx_hash ?? null,
        );
        return (
            `($${b + 1},` +
            `(SELECT token_id FROM tokens.registry WHERE denom = $${b + 2}),` +
            `(SELECT token_id FROM tokens.registry WHERE denom = $${b + 3}),` +
            `$${b + 4},$${b + 5},$${b + 6},$${b + 7},$${b + 8})`
        );
    });

    await client.query(
        `INSERT INTO dex.pools
       (pair_contract, base_token_id, quote_token_id,
        lp_token_denom, pair_id, signer, created_height, created_tx_hash)
     VALUES ${placeholders.join(', ')}
     ON CONFLICT (pair_contract) DO UPDATE SET
       base_token_id   = COALESCE(EXCLUDED.base_token_id,  dex.pools.base_token_id),
       quote_token_id  = COALESCE(EXCLUDED.quote_token_id, dex.pools.quote_token_id),
       lp_token_denom  = COALESCE(EXCLUDED.lp_token_denom, dex.pools.lp_token_denom),
       pair_id         = COALESCE(dex.pools.pair_id,       EXCLUDED.pair_id),
       signer          = COALESCE(dex.pools.signer,        EXCLUDED.signer),
       created_height  = COALESCE(dex.pools.created_height,EXCLUDED.created_height),
       created_tx_hash = COALESCE(dex.pools.created_tx_hash,dex.pools.created_tx_hash),
       updated_at      = NOW()`,
        values,
    );
}
