import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';
import { safeSerializeAttributes } from '../parsing.js';

export async function insertWasmEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes'];

  // ✅ Apply safe serialization and truncation
  const safeRows = rows.map(r => ({
    ...r,
    attributes: safeSerializeAttributes(r.attributes)
  }));

  // ✅ Use batched insert with strict limits
  await execBatchedInsert(
    client,
    'wasm.events',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index) DO NOTHING',
    { attributes: 'jsonb' },
    { maxRows: 100, maxParams: 700 }
  );
}
