import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts/Updates IBC Packets with lifecycle merging.
 * PK is (port_id_src, channel_id_src, sequence) to allow one row per packet.
 */
export async function insertIbcPackets(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // Pre-merge in-memory to avoid "cannot affect row twice" errors
  const mergedMap = new Map<string, any>();
  for (const row of rows) {
    const key = `${row.port_id_src}:${row.channel_id_src}:${row.sequence}`;
    const existing = mergedMap.get(key);
    if (!existing) {
      mergedMap.set(key, { ...row });
    } else {
      // Merge all non-null fields
      for (const [k, v] of Object.entries(row)) {
        if (v !== null && v !== undefined) existing[k] = v;
      }
    }
  }
  const finalRows = Array.from(mergedMap.values());

  const cols = [
    'port_id_src', 'channel_id_src', 'sequence',
    'port_id_dst', 'channel_id_dst', 'timeout_height', 'timeout_ts', 'status',
    'tx_hash_send', 'height_send', 'time_send',
    'tx_hash_recv', 'height_recv', 'time_recv',
    'tx_hash_ack', 'height_ack', 'time_ack', 'ack_success', 'ack_error',
    'tx_hash_timeout', 'height_timeout', 'time_timeout',
    'relayer_send', 'relayer_recv', 'relayer_ack',
    'denom', 'amount', 'sender', 'receiver', 'memo'
  ];

  const { text, values } = makeMultiInsert(
    'ibc.packets',
    cols,
    finalRows,
    `ON CONFLICT (port_id_src, channel_id_src, sequence) DO UPDATE SET
       port_id_dst = COALESCE(EXCLUDED.port_id_dst, ibc.packets.port_id_dst),
       channel_id_dst = COALESCE(EXCLUDED.channel_id_dst, ibc.packets.channel_id_dst),
       timeout_height = COALESCE(EXCLUDED.timeout_height, ibc.packets.timeout_height),
       timeout_ts = COALESCE(EXCLUDED.timeout_ts, ibc.packets.timeout_ts),
       status = CASE
         WHEN ibc.packets.status IN ('acknowledged', 'timeout', 'failed') THEN ibc.packets.status
         WHEN ibc.packets.status = 'received' AND EXCLUDED.status = 'sent' THEN ibc.packets.status
         ELSE EXCLUDED.status
       END,
       tx_hash_send = COALESCE(EXCLUDED.tx_hash_send, ibc.packets.tx_hash_send),
       height_send = COALESCE(EXCLUDED.height_send, ibc.packets.height_send),
       time_send = COALESCE(EXCLUDED.time_send, ibc.packets.time_send),
       tx_hash_recv = COALESCE(EXCLUDED.tx_hash_recv, ibc.packets.tx_hash_recv),
       height_recv = COALESCE(EXCLUDED.height_recv, ibc.packets.height_recv),
       time_recv = COALESCE(EXCLUDED.time_recv, ibc.packets.time_recv),
       tx_hash_ack = COALESCE(EXCLUDED.tx_hash_ack, ibc.packets.tx_hash_ack),
       height_ack = COALESCE(EXCLUDED.height_ack, ibc.packets.height_ack),
       time_ack = COALESCE(EXCLUDED.time_ack, ibc.packets.time_ack),
       ack_success = COALESCE(EXCLUDED.ack_success, ibc.packets.ack_success),
       ack_error = COALESCE(EXCLUDED.ack_error, ibc.packets.ack_error),
       tx_hash_timeout = COALESCE(EXCLUDED.tx_hash_timeout, ibc.packets.tx_hash_timeout),
       height_timeout = COALESCE(EXCLUDED.height_timeout, ibc.packets.height_timeout),
       time_timeout = COALESCE(EXCLUDED.time_timeout, ibc.packets.time_timeout),
       relayer_send = COALESCE(EXCLUDED.relayer_send, ibc.packets.relayer_send),
       relayer_recv = COALESCE(EXCLUDED.relayer_recv, ibc.packets.relayer_recv),
       relayer_ack = COALESCE(EXCLUDED.relayer_ack, ibc.packets.relayer_ack),
       denom = COALESCE(EXCLUDED.denom, ibc.packets.denom),
       amount = COALESCE(EXCLUDED.amount, ibc.packets.amount),
       sender = COALESCE(EXCLUDED.sender, ibc.packets.sender),
       receiver = COALESCE(EXCLUDED.receiver, ibc.packets.receiver),
       memo = COALESCE(EXCLUDED.memo, ibc.packets.memo)
    `
  );
  await client.query(text, values);
}
