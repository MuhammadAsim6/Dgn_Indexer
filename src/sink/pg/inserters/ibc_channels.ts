import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertIbcChannels(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // Deduplicate by (port_id, channel_id) to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
  const mergedMap = new Map<string, any>();
  for (const row of rows) {
    const key = `${row.port_id}:${row.channel_id}`;
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
    'port_id', 'channel_id', 'state', 'ordering', 'connection_hops',
    'counterparty_port', 'counterparty_channel', 'version'
  ];

  const { text, values } = makeMultiInsert(
    'ibc.channels',
    cols,
    finalRows,
    `ON CONFLICT (port_id, channel_id) DO UPDATE SET
       state = EXCLUDED.state,
       ordering = COALESCE(ibc.channels.ordering, EXCLUDED.ordering),
       connection_hops = COALESCE(ibc.channels.connection_hops, EXCLUDED.connection_hops),
       counterparty_port = COALESCE(ibc.channels.counterparty_port, EXCLUDED.counterparty_port),
       counterparty_channel = COALESCE(ibc.channels.counterparty_channel, EXCLUDED.counterparty_channel),
       version = COALESCE(ibc.channels.version, EXCLUDED.version)
    `
  );
  await client.query(text, values);
}
