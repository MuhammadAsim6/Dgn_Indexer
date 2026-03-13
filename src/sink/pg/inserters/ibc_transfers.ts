import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts/Updates IBC Transfers with lifecycle merging.
 * PK is (port_id_src, channel_id_src, sequence) to allow one row per transfer.
 */
export async function insertIbcTransfers(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // Pre-merge in-memory
  const mergedMap = new Map<string, any>();
  for (const row of rows) {
    const key = `${row.port_id_src}:${row.channel_id_src}:${row.sequence}`;
    const existing = mergedMap.get(key);
    if (!existing) {
      mergedMap.set(key, { ...row });
    } else {
      for (const [k, v] of Object.entries(row)) {
        if (v !== null && v !== undefined) existing[k] = v;
      }
    }
  }
  const finalRows = Array.from(mergedMap.values());

  const cols = [
    'port_id_src', 'channel_id_src', 'sequence',
    'port_id_dst', 'channel_id_dst',
    'sender', 'receiver', 'denom', 'amount', 'memo',
    'timeout_height', 'timeout_ts', 'status',
    'tx_hash_send', 'height_send', 'time_send',
    'tx_hash_recv', 'height_recv', 'time_recv',
    'tx_hash_ack', 'height_ack', 'time_ack', 'ack_success', 'ack_error',
    'tx_hash_timeout', 'height_timeout', 'time_timeout',
    'relayer_send', 'relayer_recv', 'relayer_ack'
  ];

  const { text, values } = makeMultiInsert(
    'ibc.transfers',
    cols,
    finalRows,
    `ON CONFLICT (port_id_src, channel_id_src, sequence) DO UPDATE SET
       port_id_dst = COALESCE(EXCLUDED.port_id_dst, ibc.transfers.port_id_dst),
       channel_id_dst = COALESCE(EXCLUDED.channel_id_dst, ibc.transfers.channel_id_dst),
       sender = COALESCE(EXCLUDED.sender, ibc.transfers.sender),
       receiver = COALESCE(EXCLUDED.receiver, ibc.transfers.receiver),
       denom = COALESCE(EXCLUDED.denom, ibc.transfers.denom),
       amount = COALESCE(EXCLUDED.amount, ibc.transfers.amount),
       memo = COALESCE(EXCLUDED.memo, ibc.transfers.memo),
       timeout_height = COALESCE(EXCLUDED.timeout_height, ibc.transfers.timeout_height),
       timeout_ts = COALESCE(EXCLUDED.timeout_ts, ibc.transfers.timeout_ts),
       status = CASE
         WHEN ibc.transfers.status IN ('acknowledged', 'timeout', 'failed') THEN ibc.transfers.status
         WHEN ibc.transfers.status = 'received' AND EXCLUDED.status = 'sent' THEN ibc.transfers.status
         ELSE EXCLUDED.status
       END,
       tx_hash_send = COALESCE(EXCLUDED.tx_hash_send, ibc.transfers.tx_hash_send),
       height_send = COALESCE(EXCLUDED.height_send, ibc.transfers.height_send),
       time_send = COALESCE(EXCLUDED.time_send, ibc.transfers.time_send),
       tx_hash_recv = COALESCE(EXCLUDED.tx_hash_recv, ibc.transfers.tx_hash_recv),
       height_recv = COALESCE(EXCLUDED.height_recv, ibc.transfers.height_recv),
       time_recv = COALESCE(EXCLUDED.time_recv, ibc.transfers.time_recv),
       tx_hash_ack = COALESCE(EXCLUDED.tx_hash_ack, ibc.transfers.tx_hash_ack),
       height_ack = COALESCE(EXCLUDED.height_ack, ibc.transfers.height_ack),
       time_ack = COALESCE(EXCLUDED.time_ack, ibc.transfers.time_ack),
       ack_success = COALESCE(EXCLUDED.ack_success, ibc.transfers.ack_success),
       ack_error = COALESCE(EXCLUDED.ack_error, ibc.transfers.ack_error),
       tx_hash_timeout = COALESCE(EXCLUDED.tx_hash_timeout, ibc.transfers.tx_hash_timeout),
       height_timeout = COALESCE(EXCLUDED.height_timeout, ibc.transfers.height_timeout),
       time_timeout = COALESCE(EXCLUDED.time_timeout, ibc.transfers.time_timeout),
       relayer_send = COALESCE(EXCLUDED.relayer_send, ibc.transfers.relayer_send),
       relayer_recv = COALESCE(EXCLUDED.relayer_recv, ibc.transfers.relayer_recv),
       relayer_ack = COALESCE(EXCLUDED.relayer_ack, ibc.transfers.relayer_ack)
    `
  );
  await client.query(text, values);
}
