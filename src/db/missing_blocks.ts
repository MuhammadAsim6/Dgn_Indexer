/**
 * Helpers for tracking and resolving missing blocks in the database.
 */
import type { Pool, PoolClient } from 'pg';

export async function recordMissingBlock(
  poolOrClient: Pool | PoolClient,
  height: number,
  lastError: string | null,
): Promise<void> {
  const sql = `
    INSERT INTO core.missing_blocks (height, last_error)
    VALUES ($1, $2)
    ON CONFLICT (height)
    DO UPDATE SET
      last_seen = now(),
      attempts = core.missing_blocks.attempts + 1,
      last_error = EXCLUDED.last_error,
      status = 'missing',
      resolved_at = NULL
  `;
  await (poolOrClient as any).query(sql, [height, lastError]);
}

export async function resolveMissingBlock(poolOrClient: Pool | PoolClient, height: number): Promise<void> {
  const sql = `
    UPDATE core.missing_blocks
    SET status = 'resolved', resolved_at = now()
    WHERE height = $1
  `;
  await (poolOrClient as any).query(sql, [height]);
}

export async function listMissingBlocks(
  poolOrClient: Pool | PoolClient,
  limit = 1000,
): Promise<number[]> {
  const sql = `
    SELECT height
    FROM core.missing_blocks
    WHERE status = 'missing'
    ORDER BY height ASC
    LIMIT $1
  `;
  const res = await (poolOrClient as any).query(sql, [limit]);
  return res.rows.map((r: any) => Number(r.height)).filter((h: number) => Number.isFinite(h));
}
