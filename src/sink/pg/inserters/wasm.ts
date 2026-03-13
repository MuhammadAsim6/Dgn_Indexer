import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmCodes(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['code_id', 'checksum', 'creator', 'instantiate_permission', 'store_tx_hash', 'store_height'];
    const { text, values } = makeMultiInsert(
        'wasm.codes',
        cols,
        rows,
        'ON CONFLICT (code_id) DO NOTHING',
        { instantiate_permission: 'jsonb' }
    );
    await client.query(text, values);
}

export async function insertWasmContracts(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // Deduplicate by address - keep the row with most complete data (prefer one with admin/label)
    const dedupedMap = new Map<string, any>();
    for (const row of rows) {
        const existing = dedupedMap.get(row.address);
        if (!existing) {
            dedupedMap.set(row.address, row);
        } else {
            // Merge: prefer non-null values, earlier height
            dedupedMap.set(row.address, {
                address: row.address,
                code_id: row.code_id || existing.code_id,
                creator: row.creator || existing.creator,
                admin: row.admin || existing.admin,
                label: row.label || existing.label,
                created_height: Math.min(existing.created_height || Infinity, row.created_height || Infinity),
                created_tx_hash: existing.created_tx_hash || row.created_tx_hash
            });
        }
    }
    const dedupedRows = Array.from(dedupedMap.values());

    const cols = ['address', 'code_id', 'creator', 'admin', 'label', 'created_height', 'created_tx_hash'];
    const { text, values } = makeMultiInsert(
        'wasm.contracts',
        cols,
        dedupedRows,
        `ON CONFLICT (address) DO UPDATE SET 
            code_id = EXCLUDED.code_id,
            admin = COALESCE(wasm.contracts.admin, EXCLUDED.admin),
            label = COALESCE(wasm.contracts.label, EXCLUDED.label),
            created_height = LEAST(wasm.contracts.created_height, EXCLUDED.created_height),
            created_tx_hash = COALESCE(wasm.contracts.created_tx_hash, EXCLUDED.created_tx_hash)`
    );
    await client.query(text, values);
}

export async function insertWasmMigrations(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['contract', 'from_code_id', 'to_code_id', 'height', 'tx_hash'];
    const { text, values } = makeMultiInsert(
        'wasm.contract_migrations',
        cols,
        rows,
        'ON CONFLICT (contract, height, tx_hash) DO NOTHING'
    );
    await client.query(text, values);
}

export async function updateWasmInstantiateConfig(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    for (const row of rows) {
        // Individual updates for permissions as it's a rare/low-volume operation
        await client.query(
            `UPDATE wasm.codes SET instantiate_permission = $1 WHERE code_id = $2`,
            [row.instantiate_permission, row.code_id]
        );
    }
}
