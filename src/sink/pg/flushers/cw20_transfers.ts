import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export async function flushCw20Transfers(client: PoolClient, rowsAll: any[]): Promise<void> {
  if (!rowsAll.length) return;
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  const rows = rowsAll.filter((r) => r && r.contract && r.from_addr && r.to_addr && r.amount);
  if (!rows.length) return;

  const columns = ['contract', 'from_addr', 'to_addr', 'amount', 'height', 'tx_hash'] as const;
  const shaped = rows.map((r) => ({
    contract: r.contract,
    from_addr: r.from_addr,
    to_addr: r.to_addr,
    amount: r.amount,
    height: r.height,
    tx_hash: r.tx_hash,
  }));

  await execBatchedInsert(
    client,
    'tokens.cw20_transfers',
    columns as unknown as string[],
    shaped,
    'ON CONFLICT DO NOTHING',
  );

  // ✅ Update CW20 balances after inserting transfers
  await updateCw20Balances(client, rows);
}

/**
 * Update CW20 balances based on transfers.
 * Decreases sender balance and increases receiver balance.
 */
async function updateCw20Balances(client: PoolClient, transfers: any[]): Promise<void> {
  if (!transfers.length) return;

  // Aggregate balance changes by (contract, account)
  const deltas = new Map<string, bigint>();

  for (const t of transfers) {
    const senderKey = `${t.contract}|${t.from_addr}`;
    const receiverKey = `${t.contract}|${t.to_addr}`;
    const amount = BigInt(t.amount);

    // Decrease sender
    deltas.set(senderKey, (deltas.get(senderKey) || 0n) - amount);
    // Increase receiver
    deltas.set(receiverKey, (deltas.get(receiverKey) || 0n) + amount);
  }

  // Batch upsert balance changes
  if (deltas.size === 0) return;

  // Build single batched UPSERT
  const values: any[] = [];
  const valueParts: string[] = [];
  let paramIdx = 1;

  for (const [key, delta] of deltas) {
    if (delta === 0n) continue;
    const [contract, account] = key.split('|');

    valueParts.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
    values.push(contract, account, delta.toString());
    paramIdx += 3;
  }

  if (valueParts.length === 0) return;

  const query = `
    INSERT INTO tokens.cw20_balances_current AS t (contract, account, balance)
    SELECT * FROM (VALUES ${valueParts.join(', ')}) AS v(contract, account, delta)
    ON CONFLICT (contract, account)
    DO UPDATE SET balance = t.balance + EXCLUDED.balance::numeric
  `;

  await client.query(query, values);
}
