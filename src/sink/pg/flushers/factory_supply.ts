import type { PoolClient } from 'pg';
import { insertFactorySupplyEvents } from '../inserters/factory_supply.js';

export async function flushFactorySupplyEvents(client: PoolClient, buf: any[]): Promise<void> {
    if (!buf.length) return;
    const rows = [...buf];
    buf.length = 0;
    await insertFactorySupplyEvents(client, rows);
}
