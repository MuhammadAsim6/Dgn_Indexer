// src/sink/pg/flushers/transfers.ts
import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('flusher/transfers');

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function asInteger(value: unknown): number | null {
  const n = Number(value);
  return Number.isInteger(n) ? n : null;
}

function normalizeTransferRow(row: any): any | null {
  const tx_hash = asNonEmptyString(row?.tx_hash);
  const from_addr = asNonEmptyString(row?.from_addr);
  const to_addr = asNonEmptyString(row?.to_addr);
  const denom = asNonEmptyString(row?.denom);
  const amount = String(row?.amount ?? '').trim();
  const height = asInteger(row?.height);
  const msg_index = asInteger(row?.msg_index);
  const event_index = asInteger(row?.event_index) ?? -1;
  const token_id = row?.token_id != null ? Number(row.token_id) : null;

  if (!tx_hash || !from_addr || !to_addr || !denom) return null;
  if (!/^\d+$/.test(amount)) return null;
  if (height == null || msg_index == null) return null;

  return { tx_hash, msg_index, event_index, from_addr, to_addr, denom, token_id, amount, height };
}

/**
 * Flushes batched transfer rows into the `bank.transfers` table in Postgres.
 *
 * @param {PoolClient} client - The Postgres PoolClient used to execute queries.
 * @param {any[]} rows - An array of transfer records to insert.
 * @returns {Promise<void>} A Promise that resolves when the insert is complete.
 */
export async function flushTransfers(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows.length) return;
  const validRows: any[] = [];
  let dropped = 0;
  for (const row of rows) {
    const normalized = normalizeTransferRow(row);
    if (!normalized) {
      dropped++;
      continue;
    }
    validRows.push(normalized);
  }
  if (dropped > 0) {
    log.warn(`[flush] dropping ${dropped} invalid transfer row(s) before insert (received=${rows.length})`);
  }
  if (!validRows.length) return;

  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);
  const cols = ['tx_hash', 'msg_index', 'event_index', 'from_addr', 'to_addr', 'denom', 'token_id', 'amount', 'height'];
  await execBatchedInsert(
    client,
    'bank.transfers',
    cols,
    validRows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index, from_addr, to_addr, denom) DO NOTHING',
    undefined,
    { maxRows: 5000, maxParams: 30000 },
  );
}
