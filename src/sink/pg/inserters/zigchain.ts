import type { PoolClient } from 'pg';
import { execBatchedInsert, makeMultiInsert } from '../batch.js';

// 1. Factory Denoms
export async function insertFactoryDenoms(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'creator_address', 'sub_denom', 'minting_cap',
    'uri', 'uri_hash', 'description', 'creation_tx_hash', 'block_height'
  ];
  await execBatchedInsert(
    client,
    'zigchain.factory_denoms',
    cols,
    rows,
    'ON CONFLICT (denom) DO UPDATE SET ' +
    'creator_address = COALESCE(EXCLUDED.creator_address, zigchain.factory_denoms.creator_address), ' +
    'sub_denom = COALESCE(EXCLUDED.sub_denom, zigchain.factory_denoms.sub_denom), ' +
    'minting_cap = COALESCE(EXCLUDED.minting_cap, zigchain.factory_denoms.minting_cap), ' +
    'uri = COALESCE(EXCLUDED.uri, zigchain.factory_denoms.uri), ' +
    'uri_hash = COALESCE(EXCLUDED.uri_hash, zigchain.factory_denoms.uri_hash), ' +
    'description = COALESCE(EXCLUDED.description, zigchain.factory_denoms.description), ' +
    'creation_tx_hash = zigchain.factory_denoms.creation_tx_hash, ' +
    'block_height = zigchain.factory_denoms.block_height'
  );
}

// 2. DEX Pools
export async function insertDexPools(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'pool_id', 'creator_address', 'pair_id', 'base_denom', 'quote_denom',
    'lp_token_denom', 'base_reserve', 'quote_reserve', 'block_height', 'tx_hash'
  ];
  await execBatchedInsert(
    client,
    'zigchain.dex_pools',
    cols,
    rows,
    'ON CONFLICT (pool_id) DO UPDATE SET ' +
    'creator_address = COALESCE(zigchain.dex_pools.creator_address, EXCLUDED.creator_address), ' +
    'pair_id = COALESCE(zigchain.dex_pools.pair_id, EXCLUDED.pair_id), ' +
    'base_denom = COALESCE(zigchain.dex_pools.base_denom, EXCLUDED.base_denom), ' +
    'quote_denom = COALESCE(zigchain.dex_pools.quote_denom, EXCLUDED.quote_denom), ' +
    'lp_token_denom = COALESCE(zigchain.dex_pools.lp_token_denom, EXCLUDED.lp_token_denom), ' +
    // Preserve reserves once pool exists; they are maintained by swaps/liquidity inserters.
    'base_reserve = COALESCE(zigchain.dex_pools.base_reserve, EXCLUDED.base_reserve), ' +
    'quote_reserve = COALESCE(zigchain.dex_pools.quote_reserve, EXCLUDED.quote_reserve), ' +
    'block_height = LEAST(zigchain.dex_pools.block_height, EXCLUDED.block_height), ' +
    'tx_hash = COALESCE(zigchain.dex_pools.tx_hash, EXCLUDED.tx_hash)'
  );
}

// 3. DEX Swaps
export async function insertDexSwaps(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'event_index', 'pool_id', 'sender_address', 'token_in_denom',
    'token_in_amount', 'token_out_denom', 'token_out_amount',
    'pair_id', 'effective_price', 'total_fee', 'fee_denom', 'price_impact',
    'block_height', 'timestamp'
  ];
  const maxRows = 500;
  for (let i = 0; i < rows.length; i += maxRows) {
    const slice = rows.slice(i, i + maxRows);
    const { text, values } = makeMultiInsert(
      'zigchain.dex_swaps',
      cols,
      slice,
      'ON CONFLICT (tx_hash, msg_index, event_index, block_height) DO NOTHING RETURNING pool_id, token_in_denom, token_in_amount, token_out_denom, token_out_amount, block_height',
    );

    // Maintain current pool reserves from newly inserted swaps only.
    await client.query(
      `
      WITH inserted AS (
        ${text}
      ),
      deltas AS (
        SELECT
          p.pool_id,
          SUM(
            CASE
              WHEN i.token_in_denom = p.base_denom AND i.token_in_amount::text ~ '^[0-9]+$' THEN i.token_in_amount::numeric
              WHEN i.token_out_denom = p.base_denom AND i.token_out_amount::text ~ '^[0-9]+$' THEN -i.token_out_amount::numeric
              ELSE 0
            END
          ) AS delta_base,
          SUM(
            CASE
              WHEN i.token_in_denom = p.quote_denom AND i.token_in_amount::text ~ '^[0-9]+$' THEN i.token_in_amount::numeric
              WHEN i.token_out_denom = p.quote_denom AND i.token_out_amount::text ~ '^[0-9]+$' THEN -i.token_out_amount::numeric
              ELSE 0
            END
          ) AS delta_quote,
          MAX(i.block_height) AS max_height
        FROM inserted i
        JOIN zigchain.dex_pools p ON p.pool_id = i.pool_id
        GROUP BY p.pool_id
      )
      UPDATE zigchain.dex_pools p
      SET
        base_reserve = COALESCE(p.base_reserve::numeric, 0) + d.delta_base,
        quote_reserve = COALESCE(p.quote_reserve::numeric, 0) + d.delta_quote,
        block_height = GREATEST(COALESCE(p.block_height, 0), COALESCE(d.max_height, 0))
      FROM deltas d
      WHERE p.pool_id = d.pool_id
      `,
      values,
    );
  }
}

// 4. DEX Liquidity
export async function insertDexLiquidity(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'pool_id', 'sender_address', 'action_type',
    'amount_0', 'amount_1', 'shares_minted_burned', 'block_height'
  ];
  const maxRows = 500;
  for (let i = 0; i < rows.length; i += maxRows) {
    const slice = rows.slice(i, i + maxRows);
    const { text, values } = makeMultiInsert(
      'zigchain.dex_liquidity',
      cols,
      slice,
      'ON CONFLICT (tx_hash, msg_index, block_height) DO NOTHING RETURNING pool_id, action_type, amount_0, amount_1, block_height',
    );

    // Maintain current pool reserves from newly inserted liquidity changes only.
    await client.query(
      `
      WITH inserted AS (
        ${text}
      ),
      deltas AS (
        SELECT
          p.pool_id,
          SUM(
            CASE
              WHEN upper(COALESCE(i.action_type, '')) = 'ADD' AND i.amount_0::text ~ '^[0-9]+$' THEN i.amount_0::numeric
              WHEN upper(COALESCE(i.action_type, '')) = 'REMOVE' AND i.amount_0::text ~ '^[0-9]+$' THEN -i.amount_0::numeric
              ELSE 0
            END
          ) AS delta_base,
          SUM(
            CASE
              WHEN upper(COALESCE(i.action_type, '')) = 'ADD' AND i.amount_1::text ~ '^[0-9]+$' THEN i.amount_1::numeric
              WHEN upper(COALESCE(i.action_type, '')) = 'REMOVE' AND i.amount_1::text ~ '^[0-9]+$' THEN -i.amount_1::numeric
              ELSE 0
            END
          ) AS delta_quote,
          MAX(i.block_height) AS max_height
        FROM inserted i
        JOIN zigchain.dex_pools p ON p.pool_id = i.pool_id
        GROUP BY p.pool_id
      )
      UPDATE zigchain.dex_pools p
      SET
        base_reserve = COALESCE(p.base_reserve::numeric, 0) + d.delta_base,
        quote_reserve = COALESCE(p.quote_reserve::numeric, 0) + d.delta_quote,
        block_height = GREATEST(COALESCE(p.block_height, 0), COALESCE(d.max_height, 0))
      FROM deltas d
      WHERE p.pool_id = d.pool_id
      `,
      values,
    );
  }
}

// 5. Wrapper Settings
export async function insertWrapperSettings(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'native_client_id', 'counterparty_client_id', 'native_port',
    'counterparty_port', 'native_channel', 'counterparty_channel',
    'decimal_difference', 'updated_at_height'
  ];
  await execBatchedInsert(
    client,
    'zigchain.wrapper_settings',
    cols,
    rows,
    'ON CONFLICT (denom) DO UPDATE SET ' +
    'native_client_id = COALESCE(NULLIF(EXCLUDED.native_client_id, \'\'), zigchain.wrapper_settings.native_client_id), ' +
    'counterparty_client_id = COALESCE(NULLIF(EXCLUDED.counterparty_client_id, \'\'), zigchain.wrapper_settings.counterparty_client_id), ' +
    'native_port = COALESCE(NULLIF(EXCLUDED.native_port, \'\'), zigchain.wrapper_settings.native_port), ' +
    'counterparty_port = COALESCE(NULLIF(EXCLUDED.counterparty_port, \'\'), zigchain.wrapper_settings.counterparty_port), ' +
    'native_channel = COALESCE(NULLIF(EXCLUDED.native_channel, \'\'), zigchain.wrapper_settings.native_channel), ' +
    'counterparty_channel = COALESCE(NULLIF(EXCLUDED.counterparty_channel, \'\'), zigchain.wrapper_settings.counterparty_channel), ' +
    'decimal_difference = COALESCE(EXCLUDED.decimal_difference, zigchain.wrapper_settings.decimal_difference), ' +
    'updated_at_height = CASE ' +
    'WHEN EXCLUDED.updated_at_height IS NULL THEN zigchain.wrapper_settings.updated_at_height ' +
    'WHEN zigchain.wrapper_settings.updated_at_height IS NULL THEN EXCLUDED.updated_at_height ' +
    'ELSE GREATEST(zigchain.wrapper_settings.updated_at_height, EXCLUDED.updated_at_height) ' +
    'END ' +
    'WHERE COALESCE(EXCLUDED.updated_at_height, -1) >= COALESCE(zigchain.wrapper_settings.updated_at_height, -1)'
  );
}

// 6. Wrapper Events (JSONB metadata storage for bridge events)
export async function insertWrapperEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'height', 'tx_hash', 'msg_index', 'event_index', 'sender', 'action',
    'amount', 'denom', 'metadata'
  ];
  await execBatchedInsert(
    client,
    'zigchain.wrapper_events',
    cols,
    rows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index, action) DO NOTHING'
  );
}
