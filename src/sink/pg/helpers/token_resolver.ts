/**
 * Token ID Resolver — Resolves denom strings to numeric token_id values.
 * Uses batch upsert + select for efficiency.
 */
import type { PoolClient } from 'pg';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('sink/pg/helpers/token_resolver');

/**
 * Resolves an array of denom strings to their token_id values in tokens.registry.
 * Any denoms not yet in the registry are inserted with minimal info.
 * Returns a Map<denom, token_id>.
 */
export async function resolveTokenIds(
    client: PoolClient,
    denoms: string[],
): Promise<Map<string, number>> {
    const result = new Map<string, number>();
    if (!denoms.length) return result;

    // Deduplicate
    const unique = [...new Set(denoms.filter(d => d && d.trim().length > 0))];
    if (!unique.length) return result;

    // Batch lookup existing token_ids
    const { rows: existing } = await client.query(
        `SELECT token_id, denom FROM tokens.registry WHERE denom = ANY($1)`,
        [unique],
    );

    for (const row of existing) {
        result.set(row.denom, Number(row.token_id));
    }

    // Find denoms that don't have token_ids yet
    const missing = unique.filter(d => !result.has(d));
    if (missing.length > 0) {
        // Batch insert missing denoms with minimal info
        // We infer type from the denom string
        const values: string[] = [];
        const params: any[] = [];
        let idx = 1;

        for (const denom of missing) {
            let type = 'native';
            const lower = denom.toLowerCase();
            if (lower.startsWith('factory/') || lower.startsWith('coin.')) type = 'factory';
            else if (lower.startsWith('ibc/') || lower.startsWith('transfer/')) type = 'ibc';
            else if (lower.startsWith('zig1') && !denom.includes('/')) type = 'cw20';

            values.push(`($${idx}, $${idx + 1})`);
            params.push(denom, type);
            idx += 2;
        }

        const sql = `
      INSERT INTO tokens.registry (denom, type)
      VALUES ${values.join(', ')}
      ON CONFLICT (denom) DO NOTHING
      RETURNING token_id, denom
    `;

        const { rows: inserted } = await client.query(sql, params);
        for (const row of inserted) {
            result.set(row.denom, Number(row.token_id));
        }

        // For rows that hit ON CONFLICT (already existed but weren't in our first SELECT),
        // do a final lookup
        const stillMissing = missing.filter(d => !result.has(d));
        if (stillMissing.length > 0) {
            const { rows: remaining } = await client.query(
                `SELECT token_id, denom FROM tokens.registry WHERE denom = ANY($1)`,
                [stillMissing],
            );
            for (const row of remaining) {
                result.set(row.denom, Number(row.token_id));
            }
        }
    }

    log.debug(`[token-resolver] resolved ${result.size}/${unique.length} denoms to token_ids`);
    return result;
}

/**
 * Stamps token_id on an array of rows that have a `denom` field.
 * Mutates rows in place.
 */
export function stampTokenIds(rows: any[], tokenMap: Map<string, number>): void {
    for (const row of rows) {
        const denom = row?.denom;
        if (denom && tokenMap.has(denom)) {
            row.token_id = tokenMap.get(denom);
        }
    }
}
