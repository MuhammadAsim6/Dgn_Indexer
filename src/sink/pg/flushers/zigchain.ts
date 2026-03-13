import type { PoolClient } from 'pg';
import {
  insertFactoryDenoms,
  insertDexPools,
  insertDexSwaps,
  insertDexLiquidity, // 👈 New Liquidity Import
  insertWrapperSettings,
  insertWrapperEvents
} from '../inserters/zigchain.js';

/**
 * Zigchain data flusher.
 * Delegates the actual SQL construction to the Inserters.
 */
export async function flushZigchainData(
  client: PoolClient,
  data: {
    factoryDenoms?: any[],
    dexPools?: any[],
    dexSwaps?: any[],
    dexLiquidity?: any[], // 👈 Added New Table Support
    wrapperSettings?: any[],
    wrapperEvents?: any[]
  }
): Promise<void> {
  // Safety Timeouts
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  // 1. Factory Denoms
  if (data.factoryDenoms?.length) {
    await insertFactoryDenoms(client, data.factoryDenoms);
  }

  // 2. DEX Pools (Automatically includes new columns like base_reserve)
  if (data.dexPools?.length) {
    await insertDexPools(client, data.dexPools);
  }

  // 3. DEX Swaps
  if (data.dexSwaps?.length) {
    await insertDexSwaps(client, data.dexSwaps);
  }

  // 4. DEX Liquidity (New Logic)
  if (data.dexLiquidity?.length) {
    await insertDexLiquidity(client, data.dexLiquidity);
  }

  // 5. Wrapper Settings
  if (data.wrapperSettings?.length) {
    await insertWrapperSettings(client, data.wrapperSettings);
  }

  // 6. Wrapper Events
  if (data.wrapperEvents?.length) {
    await insertWrapperEvents(client, data.wrapperEvents);
  }
}
