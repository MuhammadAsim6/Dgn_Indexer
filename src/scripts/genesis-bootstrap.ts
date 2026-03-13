import fs from 'node:fs';
import path from 'node:path';
import { getPgPool, closePgPool, createPgPool } from '../db/pg.js';
import { getLogger } from '../utils/logger.js';
import { getConfig } from '../config.js';
import { insertWrapperSettings } from '../sink/pg/inserters/zigchain.js';
import { insertTokenRegistry } from '../sink/pg/inserters/tokens.js';
import { buildTokenRegistryRow } from '../utils/token-registry.js';

const log = getLogger('scripts/genesis-bootstrap');

function normalizeNonEmptyString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function normalizeNonNegativeInt(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    const s = String(value).trim();
    if (!/^\d+$/.test(s)) return null;
    const n = Number(s);
    if (!Number.isSafeInteger(n) || n < 0 || n > 2147483647) return null;
    return n;
}

/**
 * Enhanced Genesis Bootstrap Script
 * Captures:
 * - Bank balances (standard)
 * - Staking delegations (bonded tokens - informational)
 * - Vesting accounts (locked + vested - informational)
 * - Supply verification
 */
export async function bootstrapGenesis(genesisPath: string) {
    if (!genesisPath) {
        log.error('Usage: bootstrapGenesis(<path-to-genesis.json>)');
        return;
    }

    log.info(`Reading genesis file from: ${genesisPath}`);

    let genesis: any;
    try {
        if (!fs.existsSync(genesisPath)) {
            log.warn(`Genesis file not found at ${genesisPath}. Skipping bootstrap.`);
            return;
        }
        const raw = fs.readFileSync(path.resolve(genesisPath), 'utf8');
        genesis = JSON.parse(raw);
    } catch (err: any) {
        log.error(`Failed to read genesis file: ${err.message}`);
        return;
    }

    const bankBalances = genesis.app_state?.bank?.balances ?? [];
    const stakingDelegations = genesis.app_state?.staking?.delegations ?? [];
    const vestingAccounts = genesis.app_state?.auth?.accounts?.filter(
        (acc: any) => acc['@type']?.includes('Vesting') || acc.base_vesting_account
    ) ?? [];
    const totalSupply = genesis.app_state?.bank?.supply ?? [];
    const tokenWrapperState = genesis.app_state?.tokenwrapper ?? null;

    if (!Array.isArray(bankBalances) || bankBalances.length === 0) {
        log.error('Invalid genesis format: app_state.bank.balances is missing or empty');
        return;
    }

    log.info(`Found: ${bankBalances.length} bank accounts, ${stakingDelegations.length} delegations, ${vestingAccounts.length} vesting accounts`);

    const config = getConfig();
    const pool = createPgPool({ ...config.pg, applicationName: 'genesis-bootstrap' });
    const client = await pool.connect();

    try {
        // Check if we already have genesis data
        const checkRes = await client.query('SELECT 1 FROM bank.balance_deltas WHERE height = 0 LIMIT 1');
        if (checkRes.rowCount && checkRes.rowCount > 0) {
            log.info('Genesis balances already exist (Height 0 found). Skipping.');
            return;
        }

        await client.query('BEGIN');

        // 0. Tokenwrapper settings baseline from genesis
        let wrapperDenomForRegistry: string | null = null;
        if (tokenWrapperState && typeof tokenWrapperState === 'object') {
            const wrapperRow = {
                denom: normalizeNonEmptyString(tokenWrapperState.denom),
                native_client_id: normalizeNonEmptyString(tokenWrapperState.native_client_id ?? tokenWrapperState.client_id),
                counterparty_client_id: normalizeNonEmptyString(tokenWrapperState.counterparty_client_id),
                native_port: normalizeNonEmptyString(tokenWrapperState.native_port ?? tokenWrapperState.source_port),
                counterparty_port: normalizeNonEmptyString(tokenWrapperState.counterparty_port),
                native_channel: normalizeNonEmptyString(tokenWrapperState.native_channel ?? tokenWrapperState.source_channel),
                counterparty_channel: normalizeNonEmptyString(tokenWrapperState.counterparty_channel),
                decimal_difference: normalizeNonNegativeInt(tokenWrapperState.decimal_difference),
                updated_at_height: 0,
            };

            if (wrapperRow.denom) {
                wrapperDenomForRegistry = wrapperRow.denom;
                await insertWrapperSettings(client, [wrapperRow]);
                log.info(`[genesis] seeded tokenwrapper settings for denom=${wrapperRow.denom}`);
            } else {
                log.warn('[genesis] tokenwrapper state present but denom missing; skipping wrapper_settings seed');
            }
        }

        // 0b. Seed tokens.registry baseline from genesis denom sources
        const registryDenoms = new Set<string>();
        for (const supply of totalSupply) {
            const denom = normalizeNonEmptyString(supply?.denom);
            if (denom) registryDenoms.add(denom);
        }
        for (const bal of bankBalances) {
            for (const coin of bal?.coins ?? []) {
                const denom = normalizeNonEmptyString(coin?.denom);
                if (denom) registryDenoms.add(denom);
            }
        }
        const registryRowsByDenom = new Map<string, NonNullable<ReturnType<typeof buildTokenRegistryRow>>>();
        for (const denom of registryDenoms) {
            const row = buildTokenRegistryRow({
                denom,
                height: 0,
                txHash: 'genesis_bootstrap',
            });
            if (row) {
                registryRowsByDenom.set(row.denom, row);
            }
        }

        if (wrapperDenomForRegistry) {
            const wrapperRegistryRow = buildTokenRegistryRow({
                denom: wrapperDenomForRegistry,
                height: 0,
                txHash: 'genesis_bootstrap',
                source: 'wrapper_settings',
            });
            if (wrapperRegistryRow) {
                // Ensure wrapper-specific classification wins without creating duplicates.
                registryRowsByDenom.set(wrapperRegistryRow.denom, wrapperRegistryRow);
            }
        }

        const registryRows = Array.from(registryRowsByDenom.values());

        if (registryRows.length > 0) {
            await insertTokenRegistry(client, registryRows);
            log.info(`[genesis] seeded ${registryRows.length} token registry rows`);
        }

        // 1. Bank Balances (liquid balances)
        const rows: any[] = [];
        const balanceTracker = new Map<string, bigint>(); // Track total per denom for verification

        for (const bal of bankBalances) {
            const address = bal.address;
            for (const coin of bal.coins ?? []) {
                rows.push({
                    height: 0,
                    account: address,
                    denom: coin.denom,
                    delta: coin.amount,
                    tx_hash: 'genesis_bank',
                    msg_index: 0,
                    event_index: 0
                });
                // Track for verification
                const current = balanceTracker.get(coin.denom) ?? 0n;
                balanceTracker.set(coin.denom, current + BigInt(coin.amount));
            }
        }
        log.info(`Prepared ${rows.length} bank balance entries.`);

        // 2. Staking Delegations (bonded tokens - informational only)
        // Note: Delegated tokens are NOT additional liquid balances
        // They represent tokens bonded to validators from the delegator's account
        let totalDelegated = 0n;
        for (const del of stakingDelegations) {
            if (del.shares) {
                // shares are typically equal to tokens at genesis
                totalDelegated += BigInt(Math.floor(parseFloat(del.shares)));
            }
        }
        log.info(`Total delegated tokens at genesis: ${totalDelegated.toString()} (informational - not added to balances)`);

        // 3. Vesting Accounts (track original vesting for reference)
        // Vesting tokens ARE included in bank balances, this is informational
        let totalVesting = 0n;
        for (const acc of vestingAccounts) {
            const vestingAcc = acc.base_vesting_account ?? acc;
            const originalVesting = vestingAcc.original_vesting ?? [];
            for (const coin of originalVesting) {
                totalVesting += BigInt(coin.amount);
            }
        }
        log.info(`Total vesting tokens at genesis: ${totalVesting.toString()} (included in bank balances)`);

        // 4. Supply Verification
        if (totalSupply.length > 0) {
            log.info('Verifying total supply...');
            for (const supply of totalSupply) {
                const tracked = balanceTracker.get(supply.denom) ?? 0n;
                const expected = BigInt(supply.amount);
                if (tracked !== expected) {
                    log.warn(`[supply mismatch] ${supply.denom}: tracked=${tracked}, expected=${expected}, diff=${expected - tracked}`);
                } else {
                    log.info(`[supply verified] ${supply.denom}: ${tracked.toString()}`);
                }
            }
        }

        // 4b. Seed supply baseline for tokens.factory_supply_current via events table trigger
        if (Array.isArray(totalSupply) && totalSupply.length > 0) {
            const validSupplyRows = totalSupply
                .map((s: any) => ({
                    denom: typeof s?.denom === 'string' ? s.denom.trim() : '',
                    amount: typeof s?.amount === 'string' ? s.amount.trim() : String(s?.amount ?? '').trim(),
                }))
                .filter((s: any) => s.denom.length > 0 && /^\d+$/.test(s.amount));

            if (validSupplyRows.length > 0) {
                const supplyValues: any[] = [];
                const supplyPlaceholders: string[] = [];
                let p = 1;
                for (const s of validSupplyRows) {
                    supplyPlaceholders.push(`($${p}, 'genesis_supply', 0, 0, $${p + 1}, 'mint', $${p + 2}::numeric, 'genesis', NULL, NULL)`);
                    supplyValues.push(0, s.denom, s.amount);
                    p += 3;
                }

                await client.query(`
        INSERT INTO tokens.factory_supply_events
          (height, tx_hash, msg_index, event_index, denom, action, amount, sender, recipient, metadata)
        VALUES ${supplyPlaceholders.join(', ')}
        ON CONFLICT (height, tx_hash, msg_index, event_index, denom, action) DO NOTHING
      `, supplyValues);

                log.info(`Seeded ${validSupplyRows.length} genesis supply rows into tokens.factory_supply_events.`);
            }
        }

        if (rows.length === 0) {
            log.warn('No balances found to insert.');
            await client.query('ROLLBACK');
            return;
        }

        // 5. Batch Insert into bank.balance_deltas (Chunked)
        const BATCH_SIZE = 5000;
        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const values: any[] = [];
            const placeholders: string[] = [];
            let idx = 1;

            for (const row of batch) {
                placeholders.push(`($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}::numeric)`);
                values.push(row.height, row.account, row.denom, row.delta);
                idx += 4;
            }

            await client.query(`
        INSERT INTO bank.balance_deltas (height, account, denom, delta)
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (height, account, denom) DO UPDATE SET delta = bank.balance_deltas.delta + EXCLUDED.delta
      `, values);

            log.info(`Inserted batch ${i / BATCH_SIZE + 1}/${Math.ceil(rows.length / BATCH_SIZE)}`);
        }

        log.info('All deltas inserted. Refreshing current balances view...');

        // 6. Trigger Refresh
        await client.query('SELECT bank.populate_balances_current()');

        await client.query('COMMIT');
        log.info('✅ Genesis bootstrap complete!');

    } catch (err: any) {
        await client.query('ROLLBACK');
        log.error(`Bootstrap failed: ${err.message}`);
    } finally {
        client.release();
    }
}

// Support standalone execution if called directly
import { fileURLToPath } from 'node:url';
const isEntry = () => {
    try {
        return process.argv[1] && fs.realpathSync(process.argv[1]) === fs.realpathSync(fileURLToPath(import.meta.url));
    } catch {
        return false;
    }
};

if (isEntry()) {
    const args = process.argv.slice(2);
    const genesisPath = args[0] || process.env.GENESIS_PATH;
    if (genesisPath) {
        bootstrapGenesis(genesisPath).then(() => {
            closePgPool();
        });
    } else {
        console.log('Usage: npx tsx src/scripts/genesis-bootstrap.ts <path-to-genesis.json>');
    }
}
