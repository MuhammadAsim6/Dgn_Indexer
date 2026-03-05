import type { PoolClient } from 'pg';

export type DexIbcTokenRow = {
    ibc_denom: string;        // ibc/HASH
    base_denom?: string | null;
    source_chain?: string | null;
    channel?: string | null;
    port?: string | null;
};

/**
 * Upserts rows into dex.ibc_tokens.
 * Resolves token_id via sub-SELECT on tokens.registry(denom).
 * IMPORTANT: flushTokenRegistry must run before this so the FK lookup succeeds.
 * Rows where no matching tokens.registry entry exists are silently skipped via the sub-SELECT returning NULL.
 */
export async function insertDexIbcTokens(client: PoolClient, rows: DexIbcTokenRow[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by ibc_denom
    const byDenom = new Map<string, DexIbcTokenRow>();
    for (const row of rows) {
        if (row.ibc_denom?.trim() && !byDenom.has(row.ibc_denom)) {
            byDenom.set(row.ibc_denom, row);
        }
    }
    const unique = Array.from(byDenom.values());
    if (!unique.length) return;

    const values: any[] = [];
    const placeholders = unique.map((r, i) => {
        const b = i * 5;
        values.push(
            r.ibc_denom,
            r.base_denom ?? null,
            r.source_chain ?? null,
            r.channel ?? null,
            r.port ?? null,
        );
        // Sub-SELECT resolves token_id from tokens.registry; returns NULL if not found (row is skipped).
        return (
            `((SELECT token_id FROM tokens.registry WHERE denom=$${b + 1}),` +
            `$${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5})`
        );
    });

    await client.query(
        `INSERT INTO dex.ibc_tokens (token_id, ibc_denom, base_denom, source_chain, channel, port)
     SELECT 
       v.token_id, 
       v.ibc_denom, 
       v.base_denom, 
       COALESCE(v.source_chain, (
         SELECT cl.chain_id 
         FROM ibc.channels ch
         JOIN ibc.connections con ON con.connection_id = ch.connection_hops[1]
         JOIN ibc.clients cl ON cl.client_id = con.client_id
         WHERE ch.port_id = v.port AND ch.channel_id = v.channel
         LIMIT 1
       )), 
       v.channel, 
       v.port
     FROM (VALUES ${placeholders.join(', ')}) AS v(token_id, ibc_denom, base_denom, source_chain, channel, port)
     WHERE v.token_id IS NOT NULL
     ON CONFLICT (token_id) DO UPDATE SET
       base_denom   = COALESCE(EXCLUDED.base_denom,   dex.ibc_tokens.base_denom),
       source_chain = COALESCE(EXCLUDED.source_chain, dex.ibc_tokens.source_chain),
       channel      = COALESCE(EXCLUDED.channel,      dex.ibc_tokens.channel),
       port         = COALESCE(EXCLUDED.port,         dex.ibc_tokens.port),
       updated_at   = NOW()`,
        values,
    );
}
