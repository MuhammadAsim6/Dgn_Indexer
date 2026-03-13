import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export type DexWalletRow = {
    address: string;
    first_seen_height?: number | null;
    first_seen_at?: Date | null;
};

/**
 * Upserts wallet addresses into dex.wallets.
 * Keeps the earliest first_seen_height across all upserts.
 * Called every flushAll() cycle from bufDexWallets.
 */
export async function insertDexWallets(client: PoolClient, rows: DexWalletRow[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate: keep earliest first_seen_height per address
    const byAddress = new Map<string, DexWalletRow>();
    for (const row of rows) {
        if (!row.address?.trim()) continue;
        const existing = byAddress.get(row.address);
        const isEarlier =
            row.first_seen_height != null &&
            (existing?.first_seen_height == null || row.first_seen_height < existing.first_seen_height);

        if (!existing || isEarlier) {
            byAddress.set(row.address, row);
        }
    }

    const unique = Array.from(byAddress.values());
    if (!unique.length) return;

    await execBatchedInsert(
        client,
        'dex.wallets',
        ['address', 'first_seen_height', 'first_seen_at'],
        unique,
        'ON CONFLICT (address) DO UPDATE SET ' +
        'first_seen_height = CASE ' +
        '  WHEN dex.wallets.first_seen_height IS NULL THEN EXCLUDED.first_seen_height ' +
        '  WHEN EXCLUDED.first_seen_height IS NULL    THEN dex.wallets.first_seen_height ' +
        '  ELSE LEAST(dex.wallets.first_seen_height, EXCLUDED.first_seen_height) END, ' +
        'first_seen_at = CASE ' +
        '  WHEN dex.wallets.first_seen_height IS NULL OR EXCLUDED.first_seen_height < dex.wallets.first_seen_height THEN EXCLUDED.first_seen_at ' +
        '  ELSE dex.wallets.first_seen_at END, ' +
        'updated_at = NOW()'
    );
}
