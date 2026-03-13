import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

// ✅ Maximum size for contract messages (1MB)
const MAX_WASM_MSG_SIZE = 1_000_000;
const PREVIEW_SIZE = 4096;

function toJsonbSafeString(input: unknown, maxSize: number): string {
  let payload: unknown = input;
  if (typeof input === 'string') {
    try {
      payload = JSON.parse(input);
    } catch {
      payload = {
        _non_json: true,
        _raw: input.length > PREVIEW_SIZE ? input.slice(0, PREVIEW_SIZE) : input,
        _raw_length: input.length,
      };
    }
  }

  let json: string;
  try {
    json = JSON.stringify(payload ?? {});
  } catch {
    return JSON.stringify({ _error: 'failed_to_serialize' });
  }

  if (json.length <= maxSize) return json;

  return JSON.stringify({
    _truncated: true,
    _reason: 'max_size',
    _original_length: json.length,
    _preview: json.slice(0, PREVIEW_SIZE),
  });
}

export async function insertWasmExec(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  const cols = ['tx_hash', 'msg_index', 'contract', 'caller', 'funds', 'msg', 'success', 'error', 'gas_used', 'height'];

  const safeRows = rows.map(r => {
    return {
      ...r,
      funds: JSON.stringify(r.funds ?? []),
      msg: toJsonbSafeString(r.msg, MAX_WASM_MSG_SIZE),
    };
  });

  await execBatchedInsert(
    client,
    'wasm.executions',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING',
    { funds: 'jsonb', msg: 'jsonb' },
    { maxRows: 100, maxParams: 1000 }
  );
}
