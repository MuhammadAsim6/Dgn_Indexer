import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertTxs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  const cols = [
    'tx_hash', 'height', 'tx_index', 'code', 'gas_wanted', 'gas_used',
    'fee', 'memo', 'signers', 'raw_tx', 'log_summary', 'time'
  ];

  // ✅ SOLID FIX
  const safeRows = rows.map(r => ({
    ...r,
    fee: JSON.stringify(r.fee),
    raw_tx: JSON.stringify(r.raw_tx)
  }));

  const { text, values } = makeMultiInsert(
    'core.transactions',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash) DO UPDATE SET gas_used = EXCLUDED.gas_used, log_summary = EXCLUDED.log_summary',
    { fee: 'jsonb', raw_tx: 'jsonb' }
  );
  await client.query(text, values);
}